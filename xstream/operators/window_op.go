package operators

import (
	"context"
	"engine/common"
	"engine/xsql"
	"engine/xstream/checkpoint"
	context2 "engine/xstream/context"
	"fmt"
	"github.com/sirupsen/logrus"
	"math"
	"time"
)

type WindowConfig struct {
	Type xsql.WindowType
	Length int
	Interval int   //If interval is not set, it is equals to Length
}

type WindowOperator struct {
	input       chan *xsql.BufferOrEvent
	outputs     map[string]chan<- *xsql.BufferOrEvent
	name 		string
	ticker 		common.Ticker  //For processing time only
	window      *WindowConfig
	interval	int
	triggerTime int64
	isEventTime bool
	watermarkGenerator *WatermarkGenerator //For event time only
	barrierHandler checkpoint.BarrierHandler
	inputCount  int
	sctx        context2.StreamContext
}

func NewWindowOp(name string, w *xsql.Window, isEventTime bool, lateTolerance int64, streams []string) (*WindowOperator, error) {
	o := new(WindowOperator)

	o.input = make(chan *xsql.BufferOrEvent, 1024)
	o.outputs = make(map[string]chan<- *xsql.BufferOrEvent)
	o.name = name
	o.isEventTime = isEventTime
	if w != nil{
		o.window = &WindowConfig{
			Type: w.WindowType,
			Length: w.Length.Val,
			Interval: w.Interval.Val,
		}
	}else{
		o.window = &WindowConfig{
			Type: xsql.NOT_WINDOW,
		}
	}

	if isEventTime{
		//Create watermark generator
		if w, err := NewWatermarkGenerator(o.window, lateTolerance, streams, o.input); err != nil{
			return nil, err
		}else{
			o.watermarkGenerator = w
		}
	}else{
		switch o.window.Type{
		case xsql.NOT_WINDOW:
		case xsql.TUMBLING_WINDOW:
			o.ticker = common.GetTicker(o.window.Length)
			o.interval = o.window.Length
		case xsql.HOPPING_WINDOW:
			o.ticker = common.GetTicker(o.window.Interval)
			o.interval = o.window.Interval
		case xsql.SLIDING_WINDOW:
			o.interval = o.window.Length
		case xsql.SESSION_WINDOW:
			o.ticker = common.GetTicker(o.window.Length)
			o.interval = o.window.Interval
		default:
			return nil, fmt.Errorf("unsupported window type %d", o.window.Type)
		}
	}
	return o, nil
}

func (o *WindowOperator) GetName() string {
	return o.name
}

func (o *WindowOperator) AddOutput(output chan<- *xsql.BufferOrEvent, name string) {
	if _, ok := o.outputs[name]; !ok{
		o.outputs[name] = output
	}else{
		common.Log.Warnf("fail to add output %s, operator %s already has an output of the same name", name, o.name)
	}
}

func (o *WindowOperator) GetInput() (chan<- *xsql.BufferOrEvent, string) {
	return o.input, o.name
}

// Exec is the entry point for the executor
// input: *xsql.Tuple from preprocessor
// output: xsql.WindowTuplesSet
func (o *WindowOperator) Exec(sctx context2.StreamContext) (err error) {
	o.sctx = sctx
	log := sctx.GetLogger()
	log.Printf("Window operator %s is started", o.name)

	if len(o.outputs) <= 0 {
		err = fmt.Errorf("no output channel found")
		return
	}
	if o.isEventTime{
		go o.execEventWindow(sctx)
	}else{
		go o.execProcessingWindow(sctx)
	}

	return nil
}

func (o *WindowOperator) execProcessingWindow(sctx context2.StreamContext) {
	exeCtx, cancel := context.WithCancel(sctx.GetContext())
	log := sctx.GetLogger()
	var (
		inputs []*xsql.Tuple
		c <-chan time.Time
		timeoutTicker common.Timer
		timeout <-chan time.Time
	)

	if o.ticker != nil {
		c = o.ticker.GetC()
	}

	for {
		select {
		// process incoming item
		case item, opened := <-o.input:
			if !opened {
				break
			}
			if o.barrierHandler != nil && !item.Processed{
				//if it is barrier return true and ignore the further processing
				//if it is blocked(align handler), return true and then write back to the channel later
				isProcessed := o.barrierHandler.Process(item, sctx)
				if isProcessed{
					break
				}
			}
			if d, ok := item.Data.(*xsql.Tuple); !ok {
				log.Errorf("Expect xsql.Tuple type")
				break
			}else{
				log.Debugf("Event window receive tuple %s", d.Message)
				inputs = append(inputs, d)
				switch o.window.Type{
				case xsql.NOT_WINDOW:
					inputs, _ = o.scan(inputs, d.Timestamp, sctx)
				case xsql.SLIDING_WINDOW:
					inputs, _ = o.scan(inputs, d.Timestamp, sctx)
				case xsql.SESSION_WINDOW:
					if timeoutTicker != nil {
						timeoutTicker.Stop()
						timeoutTicker.Reset(time.Duration(o.window.Interval) * time.Millisecond)
					} else {
						timeoutTicker = common.GetTimer(o.window.Interval)
						timeout = timeoutTicker.GetC()
					}
				}
			}
		case now := <-c:
			if len(inputs) > 0 {
				n := common.TimeToUnixMilli(now)
				//For session window, check if the last scan time is newer than the inputs
				if o.window.Type == xsql.SESSION_WINDOW{
					//scan time for session window will record all triggers of the ticker but not the timeout
					lastTriggerTime := o.triggerTime
					o.triggerTime = n
					//Check if the current window has exceeded the max duration, if not continue expand
					if lastTriggerTime < inputs[0].Timestamp{
						break
					}
				}
				log.Infof("triggered by ticker")
				inputs, _ = o.scan(inputs, n, sctx)
			}
		case now := <-timeout:
			if len(inputs) > 0 {
				log.Infof("triggered by timeout")
				inputs, _ = o.scan(inputs, common.TimeToUnixMilli(now), sctx)
				//expire all inputs, so that when timer scan there is no item
				inputs = make([]*xsql.Tuple, 0)
			}
		// is cancelling
		case <-exeCtx.Done():
			log.Println("Cancelling window....")
			if o.ticker != nil{
				o.ticker.Stop()
			}
			cancel()
			return
		}
	}
}

func (o *WindowOperator) scan(inputs []*xsql.Tuple, triggerTime int64, sctx context2.StreamContext) ([]*xsql.Tuple, bool){
	log := sctx.GetLogger()
	log.Printf("window %s triggered at %s", o.name, time.Unix(triggerTime/1000, triggerTime%1000))
	var delta int64
	if o.window.Type == xsql.HOPPING_WINDOW || o.window.Type == xsql.SLIDING_WINDOW {
		delta = o.calDelta(triggerTime, delta, log)
	}
	var results xsql.WindowTuplesSet = make([]xsql.WindowTuples, 0)
	i := 0
	//Sync table
	for _, tuple := range inputs {
		if o.window.Type == xsql.HOPPING_WINDOW || o.window.Type == xsql.SLIDING_WINDOW {
			diff := o.triggerTime - tuple.Timestamp
			if diff > int64(o.window.Length)+delta {
				log.Infof("diff: %d, length: %d, delta: %d", diff, o.window.Length, delta)
				log.Infof("tuple %s emitted at %d expired", tuple, tuple.Timestamp)
				//Expired tuple, remove it by not adding back to inputs
				continue
			}
			//Added back all inputs for non expired events
			inputs[i] = tuple
			i++
		} else if tuple.Timestamp > triggerTime {
			//Only added back early arrived events
			inputs[i] = tuple
			i++
		}
		if tuple.Timestamp <= triggerTime{
			results = results.AddTuple(tuple)
		}
	}
	triggered := false
	if len(results) > 0 {
		log.Printf("window %s triggered for %d tuples", o.name, len(inputs))
		if o.isEventTime{
			results.Sort()
		}
		o.Broadcast(results)
	}

	return inputs[:i], triggered
}

func (o *WindowOperator) calDelta(triggerTime int64, delta int64, log *logrus.Entry) int64 {
	lastTriggerTime := o.triggerTime
	o.triggerTime = triggerTime
	if lastTriggerTime <= 0 {
		delta = math.MaxInt16 //max int, all events for the initial window
	} else {
		if !o.isEventTime && o.window.Interval > 0 {
			delta = o.triggerTime - lastTriggerTime - int64(o.window.Interval)
			if delta > 100 {
				log.Warnf("Possible long computation in window; Previous eviction time: %d, current eviction time: %d", lastTriggerTime, o.triggerTime)
			}
		} else {
			delta = 0
		}
	}
	return delta
}

func (o *WindowOperator) Broadcast(data interface{}) error{
	boe := &xsql.BufferOrEvent{
		Data: data,
		Channel: o.name,
	}
	for _, out := range o.outputs{
		out <- boe
	}
	return nil
}

func (o *WindowOperator) SetBarrierHandler(handler checkpoint.BarrierHandler) {
	o.barrierHandler = handler
}

func (o *WindowOperator) AddInputCount(){
	o.inputCount++
}

func (o *WindowOperator) GetInputCount() int{
	return o.inputCount
}

func (o *WindowOperator) GetStreamContext() context2.StreamContext{
	return o.sctx
}