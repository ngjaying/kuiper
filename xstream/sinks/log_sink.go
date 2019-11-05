package sinks

import (
	"engine/xstream/collectors"
	context2 "engine/xstream/context"
	"fmt"
	"sync"
	"time"
)

// log action, no properties now
// example: {"log":{}}
func NewLogSink(name string, ruleId string) *collectors.FuncCollector {
	return collectors.Func(name, func(sctx context2.StreamContext, data interface{}) error {
		log := sctx.GetLogger()
		log.Printf("sink result for rule %s: %s", ruleId, data)
		return nil
	})
}

type QueryResult struct {
	Results []string
	LastFetch time.Time
	Mux sync.Mutex
}

var QR = &QueryResult{LastFetch:time.Now()}

func NewLogSinkToMemory(name string, ruleId string) *collectors.FuncCollector {
	QR.Results = make([]string, 10)
	return collectors.Func(name, func(sctx context2.StreamContext, data interface{}) error {
		QR.Mux.Lock()
		QR.Results = append(QR.Results, fmt.Sprintf("%s", data))
		QR.Mux.Unlock()
		return nil
	})
}