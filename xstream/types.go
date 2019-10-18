package xstream

import (
	"context"
	"engine/xsql"
	"engine/xstream/checkpoint"
)

type Emitter interface {
	AddOutput(chan<- *xsql.BufferOrEvent, string)
}

type Source interface {
	Emitter
	checkpoint.StreamTask
	Open(context context.Context) error
}

type Collector interface {
	GetInput() (chan<- *xsql.BufferOrEvent, string)
}

type Sink interface {
	Collector
	checkpoint.StreamTask
	Open(context.Context, chan<- error)
}

type Operator interface{
	Emitter
	Collector
	checkpoint.StreamTask
	Exec(context context.Context) error
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