package streamjoin

import (
	"encoding/json"
	"math"
	"sort"
)

// minWatermark 是首个事件之前 Watermark 的语义值 -∞。
const minWatermark int64 = math.MinInt64

// Side 标识事件来自左流还是右流。
type Side string

const (
	Left  Side = "L"
	Right Side = "R"
)

// Event 是一条带事件时间的输入事件。Seq 由 Joiner 按到达顺序分配，
// 从 1 开始；因此两条内容完全相同的重复事件也能彼此区分。
type Event struct {
	Side Side   `json:"side"`
	Key  string `json:"key"`
	Time int64  `json:"time"`
	Seq  int    `json:"seq"`
}

// JoinResult 是同窗口、同 Key 的一条 (Left, Right) 配对。
type JoinResult struct {
	Left   Event  `json:"left"`
	Right  Event  `json:"right"`
	Window Window `json:"window"`
}

// LateEvent 是一条进入侧输出的迟到事件：不丢弃、不重开窗口、不重算结果。
type LateEvent struct {
	Event Event `json:"event"`
	// Window 是事件按事件时间本应归属的窗口。
	Window Window `json:"window"`
	// Watermark 是判定该事件迟到时的 Watermark；
	// 若窗口曾被关闭，它也就是窗口关闭时的 Watermark。
	Watermark int64 `json:"watermark"`
	// WindowHadEvents 表示该窗口关闭前是否缓冲过事件
	// （false 对应“从未出现过事件、但早已过期的窗口”这类迟到）。
	WindowHadEvents bool `json:"windowHadEvents"`
}

// WindowClose 记录一次窗口关闭及其正式输出的最终 Join 结果。
type WindowClose struct {
	Window    Window       `json:"window"`
	Watermark int64        `json:"watermark"`
	Results   []JoinResult `json:"results"`
}

// WindowState 是某一帧结束时单个窗口的完整状态（供逐帧回放渲染）。
type WindowState struct {
	Window Window       `json:"window"`
	Left   []Event      `json:"left"`
	Right  []Event      `json:"right"`
	Pairs  []JoinResult `json:"pairs"`
	Closed bool         `json:"closed"`
}

// FrameSnapshot 是处理完一个输入事件后的逐帧快照。
type FrameSnapshot struct {
	Index           int           `json:"index"`
	Event           Event         `json:"event"`
	WatermarkBefore *int64        `json:"watermarkBefore"` // 首帧之前为 null（语义 -∞）
	WatermarkAfter  int64         `json:"watermarkAfter"`
	Advanced        bool          `json:"advanced"`
	Window          Window        `json:"window"`
	Late            bool          `json:"late"`
	NewPairs        []JoinResult  `json:"newPairs"`
	Closed          []WindowClose `json:"closed"`
	Windows         []WindowState `json:"windows"`
	Results         []JoinResult  `json:"results"`
	LateEvents      []LateEvent   `json:"lateEvents"`
}

// Replay 是回放页面所需的全部数据（配置 + 逐帧快照）。
type Replay struct {
	WindowSize int64           `json:"windowSize"`
	Tolerance  int64           `json:"tolerance"`
	Frames     []FrameSnapshot `json:"frames"`
}

type windowBuf struct {
	start  int64
	left   []Event
	right  []Event
	pairs  []JoinResult
	closed bool
}

// Joiner 是双流事件时间窗口 Join 算子：
// 同一个 WindowAssigner 为两条流分配天然对齐的窗口，
// 同一个 WatermarkGenerator 驱动窗口关闭，迟到事件进入独立侧输出。
type Joiner struct {
	assigner *WindowAssigner
	wmGen    *WatermarkGenerator

	windows map[int64]*windowBuf // 以窗口 Start 为 key，关闭后仍保留用于判定迟到
	seq     int

	frames     []FrameSnapshot
	results    []JoinResult
	lateEvents []LateEvent
}

// NewJoiner 创建算子：windowSize 为滚动窗口长度，tolerance 为最大乱序容忍度。
func NewJoiner(windowSize, tolerance int64) *Joiner {
	return &Joiner{
		assigner: NewWindowAssigner(windowSize),
		wmGen:    NewWatermarkGenerator(tolerance),
		windows:  make(map[int64]*windowBuf),
	}
}

// Process 按到达顺序处理一条事件并返回该帧快照。每帧处理顺序固定：
// 先推进 Watermark → 再判定是否迟到 → 最后关闭所有 End <= WM 的窗口。
func (j *Joiner) Process(side Side, key string, time int64) FrameSnapshot {
	j.seq++
	e := Event{Side: side, Key: key, Time: time, Seq: j.seq}

	// 1. 推进 Watermark。
	var before *int64
	if wm := j.wmGen.Watermark(); wm != minWatermark {
		v := wm
		before = &v
	}
	wmAfter, advanced := j.wmGen.Observe(time)

	win := j.assigner.Assign(time)

	// 2. 判定迟到：窗口已关闭，或 Watermark 已越过窗口 End。
	//    事件自身不可能把 WM 推过自己窗口的 End：
	//    WM = t - tolerance <= t < End，因此不会误杀当前事件。
	buf, existed := j.windows[win.Start]
	late := (existed && buf.closed) || wmAfter >= win.End

	var newPairs []JoinResult = make([]JoinResult, 0)
	if late {
		// 3a. 迟到：完整记入侧输出，不缓冲、不配对、不重开窗口。
		j.lateEvents = append(j.lateEvents, LateEvent{
			Event:           e,
			Window:          win,
			Watermark:       wmAfter,
			WindowHadEvents: existed && len(buf.left)+len(buf.right) > 0,
		})
	} else {
		// 3b. 非迟到：缓冲事件，并与对侧流同 Key 的已缓冲事件两两配对
		//     （inner join 笛卡尔积，暂存于窗口，关闭时才正式输出）。
		if buf == nil {
			buf = &windowBuf{start: win.Start}
			j.windows[win.Start] = buf
		}
		if side == Left {
			buf.left = append(buf.left, e)
			for _, r := range buf.right {
				if r.Key == key {
					p := JoinResult{Left: e, Right: r, Window: win}
					buf.pairs = append(buf.pairs, p)
					newPairs = append(newPairs, p)
				}
			}
		} else {
			buf.right = append(buf.right, e)
			for _, l := range buf.left {
				if l.Key == key {
					p := JoinResult{Left: l, Right: e, Window: win}
					buf.pairs = append(buf.pairs, p)
					newPairs = append(newPairs, p)
				}
			}
		}
	}

	// 4. 关闭所有 End <= Watermark 的窗口（按 Start 升序依次关闭）。
	//    注意是 <=：WM == End 的边界帧即触发关闭。
	var closed []WindowClose = make([]WindowClose, 0)
	for _, b := range j.sortedWindows() {
		if b.closed {
			continue
		}
		end := b.start + j.assigner.Size()
		if wmAfter < end {
			break // 已按 Start 排序，后面的窗口 End 更大
		}
		b.closed = true
		out := make([]JoinResult, 0, len(b.pairs))
		out = append(out, b.pairs...)
		j.results = append(j.results, out...)
		closed = append(closed, WindowClose{
			Window:    Window{Start: b.start, End: end},
			Watermark: wmAfter,
			Results:   out,
		})
	}

	snap := j.snapshot(e, before, wmAfter, advanced, win, late, newPairs, closed)
	j.frames = append(j.frames, snap)
	return snap
}

func (j *Joiner) sortedWindows() []*windowBuf {
	bufs := make([]*windowBuf, 0, len(j.windows))
	for _, b := range j.windows {
		bufs = append(bufs, b)
	}
	sort.Slice(bufs, func(i, k int) bool { return bufs[i].start < bufs[k].start })
	return bufs
}

func (j *Joiner) snapshot(e Event, before *int64, wmAfter int64, advanced bool,
	win Window, late bool, newPairs []JoinResult, closed []WindowClose) FrameSnapshot {

	states := make([]WindowState, 0, len(j.windows))
	for _, b := range j.sortedWindows() {
		w := Window{Start: b.start, End: b.start + j.assigner.Size()}
		states = append(states, WindowState{
			Window: w,
			Left:   append(make([]Event, 0, len(b.left)), b.left...),
			Right:  append(make([]Event, 0, len(b.right)), b.right...),
			Pairs:  append(make([]JoinResult, 0, len(b.pairs)), b.pairs...),
			Closed: b.closed,
		})
	}
	return FrameSnapshot{
		Index:           len(j.frames) + 1,
		Event:           e,
		WatermarkBefore: before,
		WatermarkAfter:  wmAfter,
		Advanced:        advanced,
		Window:          win,
		Late:            late,
		// 所有切片字段即使为空也序列化为 [] 而非 null（前端依赖可迭代）。
		NewPairs:   append(make([]JoinResult, 0, len(newPairs)), newPairs...),
		Closed:     append(make([]WindowClose, 0, len(closed)), closed...),
		Windows:    states,
		Results:    append(make([]JoinResult, 0, len(j.results)), j.results...),
		LateEvents: append(make([]LateEvent, 0, len(j.lateEvents)), j.lateEvents...),
	}
}

// Frames 返回目前为止的全部逐帧快照。
func (j *Joiner) Frames() []FrameSnapshot { return j.frames }

// Results 返回窗口关闭时正式输出的全部 Join 结果（按窗口 Start、配对形成顺序）。
func (j *Joiner) Results() []JoinResult { return j.results }

// LateEvents 返回侧输出中的全部迟到事件（按到达顺序）。
func (j *Joiner) LateEvents() []LateEvent { return j.lateEvents }

// Watermark 返回当前 Watermark。
func (j *Joiner) Watermark() int64 { return j.wmGen.Watermark() }

// Replay 导出回放数据。
func (j *Joiner) Replay() Replay {
	return Replay{
		WindowSize: j.assigner.Size(),
		Tolerance:  j.wmGen.Tolerance(),
		Frames:     j.frames,
	}
}

// MarshalReplay 把回放数据序列化为 JSON。
func (j *Joiner) MarshalReplay() ([]byte, error) {
	return json.Marshal(j.Replay())
}
