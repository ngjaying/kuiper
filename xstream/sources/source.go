package sources

import (
	"engine/common"
	"engine/xsql"
	"engine/xstream/checkpoint"
	"engine/xstream/connectors"
	"engine/xstream/context"
	"fmt"
)

type SourceWrapper struct {
	connector connectors.Input
	outs map[string]chan<- *xsql.BufferOrEvent
	name string
	sctx context.StreamContext //only assigned after opened
}

func NewSourceWrapper(name string, connector connectors.Input) *SourceWrapper{
	return &SourceWrapper{
		connector: connector,
		outs: make(map[string]chan<- *xsql.BufferOrEvent),
		name: name,
		sctx: nil,
	}
}

func (m *SourceWrapper) Open(sctx context.StreamContext) (err error) {
	m.sctx = sctx
	offset, ok := sctx.GetState("offset")
	if ok{
		m.connector.Rewind(offset)
	}
	return m.connector.Open(sctx, func(data interface{}){
		m.Broadcast(data)
	})
}

func (m *SourceWrapper) AddOutput(output chan<- *xsql.BufferOrEvent, name string) {
	if _, ok := m.outs[name]; !ok{
		m.outs[name] = output
	}else{
		common.Log.Warnf("fail to add output %s, operator %s already has an output of the same name", name, m.name)
	}
}

func (m *SourceWrapper) Broadcast(data interface{}) (err error){
	logger := m.sctx.GetLogger()
	logger.Debugf("source %s broadcasts data &v", m.name, data)
	boe := &xsql.BufferOrEvent{
		Data:      data,
		Channel:   m.name,
		Processed: false,
	}
	for n, out := range m.outs{
		select{
		case out <- boe:
			//All ok
		default://TODO channel full strategy?
			err = fmt.Errorf("%v;channel full for %s", err, n)
		}
	}
	if err != nil{
		logger.Errorf("source %s broadcast error: %v", m.name, err)
	}
	m.sctx.PutState("offset", m.connector.GetOffset())
	return err
}

func (m *SourceWrapper) AddInputCount(){
	//Do nothing
}

func (m *SourceWrapper) GetInputCount() int{
	return 0
}

func (m *SourceWrapper) GetName() string{
	return m.name
}

func (m *SourceWrapper) SetBarrierHandler(checkpoint.BarrierHandler) {
	//DO nothing for sources as it only emits barrier
}

func (m *SourceWrapper) GetStreamContext() context.StreamContext{
	return m.sctx
}
