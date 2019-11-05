package context

import (
	"context"
	"engine/common"
	"engine/xstream/state"
	"github.com/sirupsen/logrus"
	"sync"
)

type stateMap struct{
	sync.RWMutex
	m map[string]interface{}
}

func NewStateMap(m map[string]interface{}) *stateMap {
	return &stateMap{
		m: m,
	}
}

func (m *stateMap) get(key string) (interface{}, bool){
	m.RLock()
	r, ok := m.m[key]
	m.RUnlock()
	return r, ok
}

func (m *stateMap) set(key string, value interface{}){
	m.Lock()
	m.m[key] = value
	m.Unlock()
}

type StreamContext interface{
	GetContext() context.Context
	GetLogger()  *logrus.Entry
	GetState(key string) (interface{}, bool)
	PutState(key string, value interface{})
	SaveToCheckpoint(checkpointId int64) error
}

type StreamContextImpl struct {
	ruleId string
	opId   string
	ctx    context.Context
	logger *logrus.Entry
	store  state.Store
	state  *stateMap
}

func NewStreamContextImpl(ruleId string, opId string, store state.Store, ctx context.Context) *StreamContextImpl{
	c := &StreamContextImpl{
		ruleId: ruleId,
		opId:	opId,
		ctx:    ctx,
		logger: common.GetLogger(ctx),
		store:  store,
	}
	c.state = NewStateMap(c.store.RestoreState(opId))
	return c
}

func (c *StreamContextImpl) GetContext() context.Context {
	return c.ctx
}

func (c *StreamContextImpl) GetLogger() *logrus.Entry {
	return c.logger
}

func (c *StreamContextImpl) GetState(key string) (interface{}, bool){
	return c.state.get(key)
}

func (c *StreamContextImpl) PutState(key string, value interface{}){
	c.state.set(key, value)
}

func (c *StreamContextImpl) SaveToCheckpoint(checkpointId int64) error{
	c.store.SaveState(c.logger, checkpointId, c.opId, c.state.m)
	return nil
}

