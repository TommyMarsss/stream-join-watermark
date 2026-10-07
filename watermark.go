package streamjoin

import "math"

// WatermarkGenerator 基于“已观察到的最大事件时间 - 最大乱序容忍度”
// 推进 Watermark，并保证其单调不减。
type WatermarkGenerator struct {
	maxObserved int64
	wm          int64
	tolerance   int64
}

// NewWatermarkGenerator 创建一个 Watermark 生成器。
// tolerance 即 maxOutOfOrderness，允许的最大事件时间乱序，必须 >= 0。
func NewWatermarkGenerator(tolerance int64) *WatermarkGenerator {
	if tolerance < 0 {
		panic("streamjoin: maxOutOfOrderness must be >= 0")
	}
	return &WatermarkGenerator{
		maxObserved: math.MinInt64,
		wm:          math.MinInt64,
		tolerance:   tolerance,
	}
}

// Observe 观察一个事件时间戳 t，返回更新后的 Watermark 以及它是否发生了推进。
//
//	候选值 = maxObserved - tolerance
//	WM(n)  = max(WM(n-1), 候选值)
//
// 旧（乱序）事件不会让 Watermark 回退；候选值不大于当前值时也不产生推进。
func (g *WatermarkGenerator) Observe(t int64) (watermark int64, advanced bool) {
	if t > g.maxObserved {
		g.maxObserved = t
	}
	candidate := g.maxObserved - g.tolerance
	if candidate > g.wm {
		g.wm = candidate
		return g.wm, true
	}
	return g.wm, false
}

// Watermark 返回当前 Watermark；首个事件之前语义为 -∞（math.MinInt64）。
func (g *WatermarkGenerator) Watermark() int64 { return g.wm }

// MaxObserved 返回目前观察到的最大事件时间（未有事件时为 math.MinInt64）。
func (g *WatermarkGenerator) MaxObserved() int64 { return g.maxObserved }

// Tolerance 返回允许的最大乱序时间。
func (g *WatermarkGenerator) Tolerance() int64 { return g.tolerance }
