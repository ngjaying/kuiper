package connectors

import "engine/xstream/context"

type Input interface{
	Open(streamContext context.StreamContext, handler func(data interface{})) error
	GetOffset() interface{}
	Rewind(interface{}) bool
}
