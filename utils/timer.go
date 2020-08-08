package utils

import (
	"time"
)

type Timer struct {
	begin time.Time
	end   time.Time
}

func NewTimer() *Timer {
	t := &Timer{}
	t.Start()
	return t
}
func (t *Timer) Start() {
	t.begin = time.Now()
}

func (t *Timer) Stop() float64 {
	elapsed := time.Since(t.begin)
	return elapsed.Seconds() * 1000
}
