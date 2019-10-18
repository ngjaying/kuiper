package collectors

import (
	"context"
	"engine/common"
	"engine/xsql"
	"engine/xstream/checkpoint"
	"errors"
)

// CollectorFunc is a function used to colllect
// incoming stream data. It can be used as a
// stream sink.
type CollectorFunc func(context.Context, interface{}) error

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
func (c *FuncCollector) Open(ctx context.Context, result chan<- error) {
	//c.logf = autoctx.GetLogFunc(ctx)
	//c.errf = autoctx.GetErrFunc(ctx)
	log := common.GetLogger(ctx)
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
					isBarrier := c.barrierHandler.Process(item, ctx)
					if isBarrier{
						return
					}
				}
				if err := c.f(ctx, item.Data); err != nil {
					log.Println(err)
				}
			case <-ctx.Done():
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
