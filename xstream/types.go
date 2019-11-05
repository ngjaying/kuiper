package xstream

import (
	"engine/xsql"
	"engine/xstream/checkpoint"
	context2 "engine/xstream/context"
)

type Emitter interface {
	AddOutput(chan<- *xsql.BufferOrEvent, string)
}

type Source interface {
	Emitter
	checkpoint.StreamTask
	Open(context context2.StreamContext) error
}

type Collector interface {
	GetInput() (chan<- *xsql.BufferOrEvent, string)
}

type Sink interface {
	Collector
	checkpoint.StreamTask
	Open(context2.StreamContext, chan<- error)
}

type Operator interface{
	Emitter
	Collector
	checkpoint.StreamTask
	Exec(context context2.StreamContext) error
}

type TopNode interface{
	GetName() string
}

type Rule struct{
	Id string `json:"id"`
	Sql string `json:"sql"`
	Actions []map[string]interface{} `json:"actions"`
	Options map[string]interface{} `json:"options"`
}