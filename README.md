# stream-join-watermark

基于**事件时间（event time）**的流式**双流窗口 Join** 最小实现，仅用 Go 标准库，
用于演示和验证三件事：

1. **Watermark 的单调推进**；
2. **双流在对齐滚动窗口内按 Key 的 Join**（含窗口边界归属）；
3. **迟到数据的侧输出（side output）处理**（不丢弃、不重算、不重开窗口）。

并附带一个**数据与逻辑全部内嵌的单一静态 HTML**，逐帧回放整个过程
（原生 HTML/CSS/JavaScript，无任何外部依赖、无网络请求）。

## 目录结构

```
watermark.go              WatermarkGenerator：Watermark 推进
window.go                 WindowAssigner：左闭右开滚动窗口分配（floor 对齐）
joiner.go                 Joiner：双流 Join + 窗口关闭 + 迟到侧输出 + 逐帧快照（Replay JSON）
joiner_test.go            自动化测试
cmd/demo/main.go          运行固定场景，go:embed 模板并注入 Replay 数据
cmd/demo/template.html    回放页面模板（原生 HTML/CSS/JavaScript）
watermark-demo.html       生成产物：单文件回放页面，可直接双击打开
```

包名为 `streamjoin`，模块路径为 `github.com/TommyMarsss/stream-join-watermark`。

## 快速开始

```bash
# 运行全部自动化测试
go test ./... -v

# 生成逐帧回放页面（写入 watermark-demo.html，可加参数指定输出路径）
go run ./cmd/demo
```

打开 `watermark-demo.html` 后可播放 / 暂停 / 单步 / 拖动进度条逐帧查看；
支持 `空格`、`←`、`→` 键盘控制与多档变速。URL 参数 `?frame=8` 直接定位第 8 帧，
`?theme=light|dark` 指定明暗主题（默认跟随系统）。

## 核心类型与 API

| 类型 / 函数 | 说明 |
|---|---|
| `NewWatermarkGenerator(tolerance)` | 以最大乱序容忍度创建 Watermark 生成器 |
| `(*WatermarkGenerator).Observe(t)` | 观察一个事件时间，返回 `(watermark, advanced)` |
| `NewWindowAssigner(size)` / `NewWindowAssignerOffset(size, offset)` | 创建滚动窗口分配器 |
| `(*WindowAssigner).Assign(t)` | 返回 `t` 所属的左闭右开窗口 `Window{Start,End}` |
| `NewJoiner(windowSize, tolerance)` | 创建双流 Join 算子 |
| `(*Joiner).Process(side, key, time)` | 按到达顺序处理一条事件，返回该帧 `FrameSnapshot` |
| `(*Joiner).Results()` / `LateEvents()` / `Frames()` | 正式输出 / 侧输出 / 全部帧快照 |
| `(*Joiner).MarshalReplay()` | 导出回放页面所需的全部 JSON |

事件用 `Side`（`Left` / `Right` 两个常量）+ `Key` + `Time` 表示；
算子为每条事件按到达顺序分配从 1 开始的 `Seq`，因此**两条内容完全相同的重复事件
也能彼此区分、各自参与 Join**。

## Watermark 推进算法

```
WM(n) = max( WM(n−1),  maxObservedEventTime(n) − maxOutOfOrderness )
```

- 每观察到一个事件时间戳 `t`：先更新 `maxObserved = max(maxObserved, t)`，
  再计算候选值 `maxObserved − tolerance`；仅当候选值大于当前 WM 时才更新。
  因此 **Watermark 单调不减**，乱序（旧）事件不会让它回退；时间戳不变也不产生推进。
- `tolerance`（maxOutOfOrderness）是允许的最大乱序时间。演示场景取
  **窗口长度 10、容忍度 3**：一个 `t=18` 的事件把 Watermark 推到 15，
  含义是“我们相信 ≤ 15 的事件都已经到齐了”。
- 首个事件之前 Watermark 语义为 −∞（实现为 `math.MinInt64`），
  帧快照中 `watermarkBefore` 序列化为 `null`。

### 窗口何时关闭

窗口为左闭右开区间 `[Start, End)`。处理每个事件时顺序固定为：
**先推进 Watermark → 再判定迟到 → 最后关闭所有到期窗口**。凡满足

```
WM ≥ window.End
```

的窗口按开始时间升序依次关闭，此时才正式输出该窗口的最终 Join 结果。
注意是 `≥` 而非 `>`：`WM == End` 的边界帧即触发关闭（有专门测试覆盖）。

## 双流窗口 Join 语义

- 两条流的事件由同一个 `WindowAssigner` 分配到起止完全相同的窗口——天然对齐。
- 窗口存活期间事件只做**缓冲与配对暂存**：新事件与对侧流中**同 Key** 的已缓冲
  事件两两组合（inner join 笛卡尔积），但暂不输出。
- 窗口关闭时一次性正式输出窗口内全部同 Key `(left, right)` 组合。
- 不同 Key、不同窗口的事件绝不关联（有跨 Key / 跨窗口测试覆盖）。

### 窗口边界事件归属

分配规则对整数时间轴做 **floor 对齐**（对负时间戳同样正确）：

```
Start = floor((ts − offset) / size) * size + offset
End   = Start + size
```

因此 `t=10` 属于 `[10,20)` 而不是 `[0,10)`；`t=20` 属于 `[20,30)`。
边界归属完全确定，无重复计入或遗漏。

## 迟到数据与侧输出策略

判定一个事件迟到的条件：

```
事件所属窗口已关闭（closed），或当前 Watermark 已 ≥ 该窗口 End
```

第二条覆盖“从未出现过事件、但早已过期的窗口”这类迟到。事件自身不可能把
Watermark 推过自己窗口的 End（`WM = t − tolerance ≤ t < End`），因此不会误杀当前事件。

迟到事件遵循三条铁律，全部有测试断言：

1. **不丢弃**：完整记录到独立的**侧输出** `LateEvent`（含事件、所属窗口、
   判定时 Watermark、该窗口关闭前是否缓冲过事件 `WindowHadEvents`）；
2. **不重开窗口**：已关闭窗口在后续每一帧快照中都保持 `closed=true`；
3. **不重算结果**：已正式输出的 Join 结果不受任何影响，结果总数不增不减。

下游可消费侧输出做补偿（告警、对账、修正聚合等）。

注意区分：**事件时间落后于 Watermark ≠ 迟到**。只要事件所属窗口尚未关闭
（仍落在乱序容忍覆盖内），事件照常缓冲配对。回放场景第 13 帧 `t=22` 晚于
`t=28` 到达即“乱序但未迟到”的正例；第 8、10、15 帧是进入侧输出的迟到反例。

## 回放场景说明（18 帧）

窗口长度 10、容忍度 3，最终 **7 条 Join 结果、3 条迟到侧输出**。

| 帧 | 流 | Key | t | 处理后 WM | 关键行为 |
|---|---|---|---|---|---|
| 1 | L | a | 2 | −1 | 进入 `[0,10)` |
| 2 | R | a | 4 | 1 | 配对 (2,4) 暂存 |
| 3 | R | a | 1 | 1 | 容忍内乱序，再配对 (2,1) |
| 4 | L | x | 13 | 10 | **WM=10=End 关闭 `[0,10)`，输出 2 条** |
| 5 | R | b | 11 | 10 | 进入 `[10,20)` |
| 6 | L | b | 15 | 12 | 配对 (15,11) 暂存 |
| 7 | R | x | 18 | 15 | 配对 (13,18) 暂存 |
| **8** | R | a | 3 | 15 | **迟到：`[0,10)` 已关 → 侧输出** |
| 9 | L | c | 25 | 22 | **关闭 `[10,20)`，输出 2 条**；进入 `[20,30)` |
| **10** | L | a | 19 | 22 | **迟到 → 侧输出** |
| 11 | R | c | 28 | 25 | 配对 (25,28) 暂存 |
| 12 | R | b | 20 | 25 | **边界事件** `t=20` 入 `[20,30)`；无同 Key 左事件 |
| 13 | R | c | 22 | 25 | 乱序但未迟到：再配对 (25,22) |
| 14 | L | c | 35 | 32 | **关闭 `[20,30)`，输出 2 条**；进入 `[30,40)` |
| **15** | R | b | 21 | 32 | **迟到 → 侧输出** |
| 16 | R | c | 36 | 33 | 配对 (35,36) 暂存 |
| 17 | R | c | 40 | 37 | **边界事件** `t=40` 入 `[40,50)` |
| 18 | L | z | 43 | 40 | **WM=40=End 关闭 `[30,40)`，输出 1 条**；z/c 不同 Key 不关联 |

## 测试覆盖

```bash
go test ./...
```

- `TestWatermarkMonotonicAndTolerance`：乱序序列下 Watermark 严格按
  `maxObserved − tolerance` 计算且单调不减，推进标志正确；首帧前为 −∞；
- `TestWatermarkZeroTolerance` / `TestWatermarkNegativeTolerancePanics`：容忍度边界与非法参数；
- `TestWindowBoundaryAssignment`：`t=0/10/20` 及负时间戳的边界窗口归属；
- `TestWindowAssignerOffsetAndInvalidArgs`：对齐偏移与非法参数 panic；
- `TestWindowClosesWhenWatermarkEqualsEnd`：`WM == End` 边界帧触发关闭；
- `TestJoinWindowCloseAndLateSideOutput`：端到端场景，断言结果集合精确匹配、
  迟到事件不改变结果数且不产生配对、三类迟到（已关窗口 / 边界起点 /
  从未出现过的过期窗口）全部进入侧输出、已关窗口不被重开、迟到后算子状态不被污染；
- `TestJoinDoesNotCrossKeysOrWindows`：不跨 Key、不跨窗口关联；
- `TestDuplicateEventsEachParticipate`：重复事件 `Seq` 不同、各自参与笛卡尔积；
- `TestFirstFrameWatermarkBeforeNil`：首帧 `watermarkBefore` 为 `null`；
- `TestReplayMarshalShape`：Replay JSON 形状正确，无元素的切片序列化为 `[]` 而非 `null`。
