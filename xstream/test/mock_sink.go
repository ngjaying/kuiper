package test

import (
	"engine/xsql"
	"engine/xstream/checkpoint"
	context2 "engine/xstream/context"
)

type MockSink struct {
	ruleId   string
	name 	 string
	results  [][]byte
	input chan *xsql.BufferOrEvent
	barrierHandler checkpoint.BarrierHandler
	inputCount int
	sctx     context2.StreamContext
}

func NewMockSink(name, ruleId string) *MockSink{
	m := &MockSink{
		ruleId:  ruleId,
		name:    name,
		input: make(chan *xsql.BufferOrEvent),
	}
	return m
}

func (m *MockSink) Open(sctx context2.StreamContext, result chan<- error) {
	m.sctx = sctx
	log := sctx.GetLogger()
	log.Trace("Opening mock sink")
	m.results = make([][]byte, 0)
	go func() {
		for {
			select {
			case item := <-m.input:
				if m.barrierHandler != nil && !item.Processed{
					//may be blocking
					isProcessed := m.barrierHandler.Process(item, sctx)
					if isProcessed{
						break
					}
				}
				if v, ok := item.Data.([]byte); ok {
					log.Infof("mock sink receive %s", v)
					m.results = append(m.results, v)
				}else{
					log.Info("mock sink receive non byte data")
				}

			case <-sctx.GetContext().Done():
				log.Infof("mock sink %s done", m.name)
				return
			}
		}
	}()
}

func (m *MockSink) GetInput() (chan<- *xsql.BufferOrEvent, string)  {
	return m.input, m.name
}

func (m *MockSink) GetResults() [][]byte {
	return m.results
}

func (m *MockSink) Broadcast(interface {}) error{
	return nil
}

func (m *MockSink) AddInputCount(){
	m.inputCount++
}

func (m *MockSink) GetInputCount() int{
	return m.inputCount
}

func (m *MockSink) GetName() string{
	return m.name
}

func (m *MockSink) SetBarrierHandler(handler checkpoint.BarrierHandler) {
	m.barrierHandler = handler
}

func (m *MockSink) GetStreamContext() context2.StreamContext{
	return m.sctx
}