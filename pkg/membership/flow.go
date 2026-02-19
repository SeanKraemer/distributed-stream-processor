package membership

import (
	"container/list"
	"time"
)

const COUNTER_DURATION = 1 * time.Second

type Msg struct {
	Size      int64
	Timestamp time.Time
}

type FlowCounter struct {
	currentCounter int64
	que            list.List
}

func (f *FlowCounter) maintain() {
	currentTime := time.Now()
	for f.que.Len() > 0 && currentTime.Sub(f.que.Front().Value.(Msg).Timestamp) > COUNTER_DURATION {
		f.currentCounter -= f.que.Front().Value.(Msg).Size
		f.que.Remove(f.que.Front())
	}
}

func (f *FlowCounter) Add(size int64) {
	f.currentCounter += size
	f.que.PushBack(Msg{
		Size:      size,
		Timestamp: time.Now(),
	})
	f.maintain()
}

func (f *FlowCounter) Get() float64 {
	f.maintain()
	return float64(f.currentCounter) / COUNTER_DURATION.Seconds()
}
