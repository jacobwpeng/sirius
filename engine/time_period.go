package engine

import (
	"time"
)

// 表示一段周期性增长的时间段
// [Start + n * Interval, Start + n * Interval + Duration)
// 当Duration == 0 时退化为一个周期性的时间点
type TimePeriod struct {
	Start    time.Time
	Interval time.Duration
	Duration time.Duration
}

func (tp TimePeriod) Empty() bool {
	return tp.Start.IsZero()
}

func (tp TimePeriod) NextTimeFromNow() time.Time {
	return tp.NextTime(time.Now())
}

func (tp TimePeriod) NextTime(from time.Time) time.Time {
	if tp.Start.After(from) {
		return tp.Start
	}

	next := tp.Start
	for !next.After(from) {
		next = next.Add(tp.Interval)
	}

	return next
}
