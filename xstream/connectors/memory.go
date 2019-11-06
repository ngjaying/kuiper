package connectors

import (
	"engine/common"
	"engine/xsql"
	"engine/xstream/context"
	"time"
)

type MemoryInputConnector struct {
	data []*xsql.Tuple
	done chan<- struct{}
	isEventTime bool
	pause int
	offset int
}

// New creates a new MemoryInputConnector
func NewMemoryInputConnector(data []*xsql.Tuple, done chan<- struct{}, isEventTime bool, pause int) *MemoryInputConnector {
	mock := &MemoryInputConnector{
		data: data,
		done: done,
		isEventTime: isEventTime,
		pause: pause,
	}
	return mock
}

func (m *MemoryInputConnector) Open(sctx context.StreamContext, handler func(data interface{})) error {
	log := sctx.GetLogger()
	log.Trace("memory input connector starts")
	go func(){
		for i := m.offset; i < len(m.data); i++{
			d := m.data[i]
			log.Infof("memory input connector is sending data %v", d)
			if !m.isEventTime{
				common.SetMockNow(d.Timestamp)
				ticker := common.GetMockTicker()
				timer := common.GetMockTimer()
				if ticker != nil {
					ticker.DoTick(d.Timestamp)
				}
				if timer != nil {
					timer.DoTick(d.Timestamp)
				}
			}
			m.offset = i
			handler(d)
			time.Sleep(time.Duration(m.pause) * time.Millisecond)
		}
		if !m.isEventTime {
			//reset now for the next test
			common.SetMockNow(0)
		}
		m.done <- struct{}{}
	}()
	return nil
}

func (m *MemoryInputConnector) Rewind(offset interface{}) bool{
	o, ok := offset.(int)
	if !ok {
		return false
	}
	m.offset = o
	return true
}

//the offset must be calculated before running the callback handler
func (m *MemoryInputConnector) GetOffset() interface{} {
	return m.offset
}