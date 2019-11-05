package collectors

import (
	"engine/xsql"
	"engine/xstream/checkpoint"
	context2 "engine/xstream/context"
	"errors"
)

// CollectorFunc is a function used to colllect
// incoming stream data. It can be used as a
// stream sink.
type CollectorFunc func(context2.StreamContext, interface{}) error

// FuncCollector is a colletor that uses a function
// to collect data.  The specified function must be
// of type:
//   CollectorFunc
type FuncCollector struct {
	input chan *xsql.BufferOrEvent
	//logf  api.LogFunc
	//errf  api.ErrorFunc
	f     CollectorFunc
	name  string
	barrierHandler checkpoint.BarrierHandler
	inputCount int
	sctx  context2.StreamContext
}

// Func creates a new value *FuncCollector that
// will use the specified function parameter to
// collect streaming data.
func Func(name string, f CollectorFunc) *FuncCollector {
	return &FuncCollector{f: f, name:name, input: make(chan *xsql.BufferOrEvent, 1024)}
}

func (c *FuncCollector) GetName() string  {
	return c.name
}

func (c *FuncCollector) GetInput() (chan<- *xsql.BufferOrEvent, string)  {
	return c.input, c.name
}

// Open is the starting point that starts the collector
func (c *FuncCollector) Open(sctx context2.StreamContext, result chan<- error) {
	c.sctx = sctx
	log := sctx.GetLogger()
	log.Println("Opening func collector")

	if c.f == nil {
		err := errors.New("Func collector missing function")
		log.Println(err)
		go func() { result <- err }()
	}

	go func() {
		for {
			select {
			case item := <-c.input:
				if c.barrierHandler != nil{
					//may be blocking
					isProcessed := c.barrierHandler.Process(item, sctx)
					if isProcessed{
						break
					}
				}
				if err := c.f(sctx, item.Data); err != nil {
					log.Println(err)
				}
			case <-sctx.GetContext().Done():
				log.Infof("Func collector %s done", c.name)
				return
			}
		}
	}()
}

func (c *FuncCollector) Broadcast(data interface{}) error{
	//do nothing
	return nil
}

func (c *FuncCollector) SetBarrierHandler(handler checkpoint.BarrierHandler) {
	c.barrierHandler = handler
}

func (c *FuncCollector) AddInputCount(){
	c.inputCount++
}

func (c *FuncCollector) GetInputCount() int{
	return c.inputCount
}

func (c *FuncCollector) GetStreamContext() context2.StreamContext{
	return c.sctx
}
