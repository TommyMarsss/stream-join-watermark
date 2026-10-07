package streamjoin

// Window 是左闭右开的时间窗口 [Start, End)。
type Window struct {
	Start int64 `json:"start"`
	End   int64 `json:"end"`
}

// WindowAssigner 把事件时间分配到对齐的左闭右开滚动窗口。
type WindowAssigner struct {
	size   int64
	offset int64
}

// NewWindowAssigner 创建一个从 0 对齐的滚动窗口分配器，size 为窗口长度（> 0）。
func NewWindowAssigner(size int64) *WindowAssigner {
	return NewWindowAssignerOffset(size, 0)
}

// NewWindowAssignerOffset 创建带对齐偏移的分配器（0 <= offset < size）。
func NewWindowAssignerOffset(size, offset int64) *WindowAssigner {
	if size <= 0 {
		panic("streamjoin: window size must be > 0")
	}
	if offset < 0 || offset >= size {
		panic("streamjoin: offset must be in [0, size)")
	}
	return &WindowAssigner{size: size, offset: offset}
}

// Size 返回窗口长度。
func (a *WindowAssigner) Size() int64 { return a.size }

// Assign 返回时间戳 ts 所属的窗口。
//
//	Start = floor((ts - offset) / size) * size + offset
//	End   = Start + size
//
// floor 整除对负时间戳同样正确，因此边界归属完全确定：
// t=10 属于 [10,20) 而不是 [0,10)；t=20 属于 [20,30)。
func (a *WindowAssigner) Assign(ts int64) Window {
	q := floorDiv(ts-a.offset, a.size)
	start := q*a.size + a.offset
	return Window{Start: start, End: start + a.size}
}

// floorDiv 返回数学意义上的 floor(a/b)，b 必须为正数。
// Go 的整数除法向零截断，负被除数时需要向下修正。
func floorDiv(a, b int64) int64 {
	q := a / b
	if (a%b) != 0 && ((a < 0) != (b < 0)) {
		q--
	}
	return q
}
