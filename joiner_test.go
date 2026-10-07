package streamjoin

import (
	"encoding/json"
	"math"
	"strconv"
	"testing"
)

func TestWatermarkMonotonicAndTolerance(t *testing.T) {
	g := NewWatermarkGenerator(3)
	if g.Watermark() != math.MinInt64 {
		t.Fatalf("watermark before any event should be -inf, got %d", g.Watermark())
	}

	cases := []struct {
		ts       int64
		wantWM   int64
		advanced bool
	}{
		{5, 2, true},    // 首个事件：5 - 3 = 2
		{4, 2, false},   // 乱序旧事件：Watermark 不回退、不推进
		{10, 7, true},   // 10 - 3 = 7
		{6, 7, false},   // 再次乱序：保持 7
		{100, 97, true}, // 大步推进
		{50, 97, false}, // 绝不回退
		{100, 97, false},
	}
	for i, c := range cases {
		wm, advanced := g.Observe(c.ts)
		if wm != c.wantWM {
			t.Fatalf("case %d: observe %d, watermark = %d, want %d", i, c.ts, wm, c.wantWM)
		}
		if advanced != c.advanced {
			t.Fatalf("case %d: observe %d, advanced = %v, want %v", i, c.ts, advanced, c.advanced)
		}
		if g.MaxObserved() < c.ts {
			t.Fatalf("maxObserved not updated: %d < %d", g.MaxObserved(), c.ts)
		}
	}
	if g.Tolerance() != 3 {
		t.Fatalf("tolerance = %d, want 3", g.Tolerance())
	}
}

func TestWatermarkZeroTolerance(t *testing.T) {
	g := NewWatermarkGenerator(0)
	if wm, _ := g.Observe(42); wm != 42 {
		t.Fatalf("zero tolerance: watermark = %d, want 42", wm)
	}
	if wm, advanced := g.Observe(41); wm != 42 || advanced {
		t.Fatalf("zero tolerance: watermark regressed to %d advanced=%v, want 42/false", wm, advanced)
	}
}

func TestWatermarkNegativeTolerancePanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on negative tolerance")
		}
	}()
	NewWatermarkGenerator(-1)
}

func TestWindowBoundaryAssignment(t *testing.T) {
	a := NewWindowAssigner(10)
	cases := []struct {
		ts        int64
		wantStart int64
		wantEnd   int64
	}{
		{0, 0, 10}, // 窗口起点：属于本窗口
		{9, 0, 10},
		{10, 10, 20}, // 边界 t=10 必须进入新窗口，而不是 [0,10)
		{19, 10, 20},
		{20, 20, 30}, // 边界 t=20 属于 [20,30)，不属于 [10,20)
		{-1, -10, 0}, // 负时间戳：floor 对齐
		{-10, -10, 0},
	}
	for _, c := range cases {
		got := a.Assign(c.ts)
		if got.Start != c.wantStart || got.End != c.wantEnd {
			t.Errorf("assign(%d) = [%d,%d), want [%d,%d)", c.ts, got.Start, got.End, c.wantStart, c.wantEnd)
		}
	}
}

func TestWindowAssignerOffsetAndInvalidArgs(t *testing.T) {
	a := NewWindowAssignerOffset(10, 5)
	if w := a.Assign(5); w.Start != 5 || w.End != 15 {
		t.Fatalf("offset assign(5) = [%d,%d), want [5,15)", w.Start, w.End)
	}
	if w := a.Assign(14); w.Start != 5 || w.End != 15 {
		t.Fatalf("offset assign(14) = [%d,%d), want [5,15)", w.Start, w.End)
	}
	if w := a.Assign(15); w.Start != 15 || w.End != 25 {
		t.Fatalf("offset assign(15) = [%d,%d), want [15,25)", w.Start, w.End)
	}
	if a.Size() != 10 {
		t.Fatalf("size = %d, want 10", a.Size())
	}

	for name, fn := range map[string]func(){
		"size<=0":      func() { NewWindowAssigner(0) },
		"offset<0":     func() { NewWindowAssignerOffset(10, -1) },
		"offset>=size": func() { NewWindowAssignerOffset(10, 10) },
	} {
		func(name string, fn func()) {
			defer func() {
				if recover() == nil {
					t.Fatalf("%s: expected panic", name)
				}
			}()
			fn()
		}(name, fn)
	}
}

// 端到端：乱序到达、wm >= End 时关闭、边界事件归属、迟到侧输出。
func TestJoinWindowCloseAndLateSideOutput(t *testing.T) {
	j := NewJoiner(10, 3) // 窗口长度 10，乱序容忍 3

	// 序  流    key  t    观察后 wm   说明
	//  1  L     a   12     9       进入 [10,20)
	//  2  R     a   15    12       配对 (12,15) 暂存
	//  3  L     a   22    19       进入 [20,30)
	//  4  R     a   13    19       乱序但在容忍内：[10,20) 未关，配对 (12,13)
	//  5  R     a   25    22       关闭 [10,20) 输出 2 对；(22,25) 暂存 [20,30)
	//  6  L     a   29    26       配对 (29,25) 暂存
	//  7  R     a   28    26       wm 不动；配对 (22,28)、(29,28) 暂存
	//  8  L     a   33    30       关闭 [20,30) 输出 4 对；L33 进入 [30,40)
	type in struct {
		side Side
		key  string
		t    int64
	}
	events := []in{
		{Left, "a", 12}, {Right, "a", 15}, {Left, "a", 22}, {Right, "a", 13},
		{Right, "a", 25}, {Left, "a", 29}, {Right, "a", 28}, {Left, "a", 33},
	}
	for _, e := range events {
		j.Process(e.side, e.key, e.t)
	}

	results := j.Results()
	wantPairs := map[string]bool{
		pairKeyStr(10, "a", 12, 13): false,
		pairKeyStr(10, "a", 12, 15): false,
		pairKeyStr(20, "a", 22, 25): false,
		pairKeyStr(20, "a", 29, 25): false,
		pairKeyStr(20, "a", 22, 28): false,
		pairKeyStr(20, "a", 29, 28): false,
	}
	if len(results) != len(wantPairs) {
		t.Fatalf("results = %d, want %d: %+v", len(results), len(wantPairs), results)
	}
	for _, r := range results {
		key := pairKey(r)
		if _, ok := wantPairs[key]; !ok {
			t.Errorf("unexpected join result: %s", key)
		}
		wantPairs[key] = true
	}
	for k, found := range wantPairs {
		if !found {
			t.Errorf("missing expected join result: %s", k)
		}
	}

	// 9: 迟到。[10,20) 已在 wm=22 时关闭输出，R14 必须进入侧输出：
	// 不产生新结果、不重开窗口、不重算。
	before := len(j.Results())
	f := j.Process(Right, "a", 14)
	if !f.Late {
		t.Fatal("event at t=14 should be marked late")
	}
	if len(j.LateEvents()) != 1 {
		t.Fatalf("late events = %d, want 1", len(j.LateEvents()))
	}
	le := j.LateEvents()[0]
	if !le.WindowHadEvents {
		t.Fatal("window [10,20) had buffered events before close; WindowHadEvents should be true")
	}
	if le.Watermark != f.WatermarkAfter {
		t.Fatalf("late record watermark = %d, want %d", le.Watermark, f.WatermarkAfter)
	}
	if len(j.Results()) != before {
		t.Fatalf("late event changed result count: before=%d after=%d (must not recompute)", before, len(j.Results()))
	}
	if len(f.NewPairs) != 0 {
		t.Fatalf("late event must not create pairs, got %d", len(f.NewPairs))
	}

	// 10: 迟到边界事件 t=10（恰为已关窗口起点，左闭）同样进入侧输出。
	f2 := j.Process(Left, "a", 10)
	if !f2.Late || f2.Window.Start != 10 {
		t.Fatalf("boundary late event t=10: late=%v window=%+v", f2.Late, f2.Window)
	}

	// 11: 从未出现过事件、但 Watermark 早已越过其结束时间的窗口：也算迟到，不新建窗口。
	f3 := j.Process(Right, "b", 5)
	if !f3.Late || f3.Window.Start != 0 {
		t.Fatalf("event in never-seen expired window should be late: %+v", f3)
	}
	le3 := j.LateEvents()[2]
	if le3.WindowHadEvents {
		t.Fatal("window [0,10) never had events; WindowHadEvents should be false")
	}

	if len(j.LateEvents()) != 3 {
		t.Fatalf("side outputs = %d, want 3 (late data must not be dropped)", len(j.LateEvents()))
	}
	for _, s := range j.LateEvents() {
		if s.Watermark < s.Window.End {
			t.Errorf("side output claims watermark=%d < window end=%d", s.Watermark, s.Window.End)
		}
	}

	// 已关闭窗口在状态快照中必须保持 closed，不能被迟到事件重开。
	for _, ws := range f3.Windows {
		if ws.Window.Start == 10 && !ws.Closed {
			t.Fatal("closed window [10,20) was reopened by a late event")
		}
	}

	// 12: 迟到事件之后再来一个活跃窗口事件，算子仍正常工作，证明迟到路径没有污染状态。
	// R34 与 L33 同窗口 [30,40)、同键，形成配对但暂存（wm=31 < 40），结果数不变。
	fr := j.Process(Right, "a", 34)
	if fr.Late {
		t.Fatal("event t=34 in live window must not be late")
	}
	if len(fr.NewPairs) != 1 || fr.NewPairs[0].Left.Time != 33 || fr.NewPairs[0].Right.Time != 34 {
		t.Fatalf("expected new pair (33,34), got %+v", fr.NewPairs)
	}
	if len(j.Results()) != 6 {
		t.Fatalf("live-window pair must wait for window close, results=%d", len(j.Results()))
	}
}

func TestWindowClosesWhenWatermarkEqualsEnd(t *testing.T) {
	j := NewJoiner(10, 3)
	j.Process(Left, "k", 5)
	j.Process(Right, "k", 6)
	if len(j.Results()) != 0 {
		t.Fatal("window must not output before closing")
	}
	// t=13 → wm=10 == 窗口 [0,10) 的 End，边界条件（wm >= End）下应当关闭输出。
	f := j.Process(Left, "other", 13)
	if len(f.Closed) != 1 {
		t.Fatalf("expected exactly 1 window close, got %d", len(f.Closed))
	}
	if len(f.Results) != 1 || f.Results[0].Left.Time != 5 || f.Results[0].Right.Time != 6 {
		t.Fatalf("window should fire when wm == end: %+v", f.Results)
	}
}

func TestJoinDoesNotCrossKeysOrWindows(t *testing.T) {
	j := NewJoiner(10, 0)
	j.Process(Left, "a", 1)
	j.Process(Right, "b", 2)  // 不同 key：不配对
	j.Process(Right, "a", 11) // 不同窗口 [10,20)：不与 t=1 配对
	f := j.Process(Left, "a", 11)
	if f.Late {
		t.Fatal("t=11 event must not be late")
	}
	// wm=11 先关闭 [0,10)（无同 Key 配对），结果仍为 0。
	if len(j.Results()) != 0 {
		t.Fatalf("closed [0,10) should emit 0 pairs, got %+v", j.Results())
	}
	j.Process(Left, "z", 20) // wm=20 关闭 [10,20)
	res := j.Results()
	if len(res) != 1 || res[0].Left.Time != 11 || res[0].Right.Time != 11 {
		t.Fatalf("cross key/window join leaked, results=%+v", res)
	}
}

func TestDuplicateEventsEachParticipate(t *testing.T) {
	// 两条内容完全相同的左流事件是独立记录（Seq 不同），必须各自参与笛卡尔积。
	j := NewJoiner(10, 0)
	f1 := j.Process(Left, "a", 5)
	f2 := j.Process(Left, "a", 5)
	if f1.Event.Seq == f2.Event.Seq {
		t.Fatal("arrival Seq must distinguish identical events")
	}
	j.Process(Right, "a", 6)
	j.Process(Right, "x", 20) // 推进 wm=20 关闭 [0,10)
	if got := len(j.Results()); got != 2 {
		t.Fatalf("2 identical left events × 1 right = 2 pairs, got %d", got)
	}
}

func TestFirstFrameWatermarkBeforeNil(t *testing.T) {
	j := NewJoiner(10, 3)
	f := j.Process(Left, "a", 2) // wm = -1
	if f.WatermarkBefore != nil {
		t.Fatalf("watermarkBefore on first frame should be null, got %d", *f.WatermarkBefore)
	}
	if f.WatermarkAfter != -1 || !f.Advanced {
		t.Fatalf("first frame: wm=%d advanced=%v, want -1/true", f.WatermarkAfter, f.Advanced)
	}
	f2 := j.Process(Right, "a", 4) // wm = 1
	if f2.WatermarkBefore == nil || *f2.WatermarkBefore != -1 {
		t.Fatalf("second frame watermarkBefore should be -1, got %v", f2.WatermarkBefore)
	}
}

func TestReplayMarshalShape(t *testing.T) {
	j := NewJoiner(10, 3)
	j.Process(Left, "a", 2)
	j.Process(Right, "a", 4)
	j.Process(Left, "x", 13) // 关闭 [0,10)，输出 1 对
	j.Process(Right, "a", 3) // 迟到 → 侧输出

	raw, err := j.MarshalReplay()
	if err != nil {
		t.Fatalf("marshal replay: %v", err)
	}
	var rep map[string]any
	if err := json.Unmarshal(raw, &rep); err != nil {
		t.Fatalf("replay JSON invalid: %v", err)
	}
	if rep["windowSize"].(float64) != 10 || rep["tolerance"].(float64) != 3 {
		t.Fatalf("replay config wrong: %v", rep)
	}
	frames := rep["frames"].([]any)
	if len(frames) != 4 {
		t.Fatalf("frames = %d, want 4", len(frames))
	}
	// 切片字段在无元素时也必须是 []，不能是 null（前端依赖可迭代）。
	for i, frAny := range frames {
		fr := frAny.(map[string]any)
		for _, k := range []string{"newPairs", "closed", "windows", "results", "lateEvents"} {
			v, ok := fr[k]
			if !ok {
				t.Fatalf("frame %d missing field %s", i, k)
			}
			if v == nil {
				t.Fatalf("frame %d field %s serialized as null", i, k)
			}
		}
	}
}

func pairKey(r JoinResult) string {
	return pairKeyStr(r.Window.Start, r.Left.Key, r.Left.Time, r.Right.Time)
}

func pairKeyStr(start int64, key string, l, r int64) string {
	return strconv.FormatInt(start, 10) + ":" + key + ":" +
		strconv.FormatInt(l, 10) + ":" + strconv.FormatInt(r, 10)
}
