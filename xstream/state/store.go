package state

import (
	"bytes"
	"encoding/gob"
	"engine/common"
	"fmt"
	"github.com/sirupsen/logrus"
	"sync"
)

func init(){
	gob.Register(map[string]interface{}{})
}

//The manager for checkpoint storage. Right now, only support to store in badgerDB
type Store interface{
	SaveState(logger *logrus.Entry, checkpointId int64, opId string, state map[string]interface{}) error
	RestoreState(opId string) map[string]interface{} //Get the state of an op, should only be used in initialization
	SaveCheckpoint(checkpointId int64) error         //Save the whole checkpoint state into storage like badger
}

//Store in path ./data/checkpoint/$ruleId
//Store 2 things:
//A queue for completed checkpoint id
//A map with key of checkpoint id and value of snapshot(gob serialized)
//The snapshot is a map also with key of opId and value of map
//Assume each operator only has one instance
func GetBadgerStore(ruleId string) *BadgerStore{
	dir, err := common.GetAndCreateDataLoc("checkpoint/" + ruleId)
	if err != nil{
		panic(err)
	}
	s := &BadgerStore{dir: dir, max: 3}
	//read data from badger db
	done := restore(dir, s)
	if !done {
		s.mapStore = &sync.Map{}
	}
	return s
}

func restore(dir string, s *BadgerStore) bool {
	db, err := common.DbOpen(dir)
	if err != nil {
		return false
	}
	defer common.DbClose(db)
	bytes, err := common.DbGet(db, "checkpoints")
	if err != nil {
		return false
	}
	cs, ok := bytesToSlice(bytes)
	if !ok {
		return false
	}
	s.checkpoints = cs
	bytes, err = common.DbGet(db, string(cs[len(cs)-1]))
	if err != nil {
		return false
	}
	m, ok := bytesToMap(bytes)
	if !ok {
		return false
	}
	s.mapStore = m
	return true
}


type BadgerStore struct{
	dir string
	mapStore *sync.Map
	checkpoints []int64
	max int
}

func (s *BadgerStore) SaveState(logger *logrus.Entry,checkpointId int64, opId string, state map[string]interface{}) error{
	logger.Debugf("Save state for checkpoint %d, op %s, value %v", checkpointId, opId, state)
	var cstore *sync.Map
	if v, ok := s.mapStore.Load(checkpointId); !ok{
		cstore = &sync.Map{}
		s.mapStore.Store(checkpointId, cstore)
	}else{
		if cstore, ok = v.(*sync.Map); !ok{
			return fmt.Errorf("invalid BadgerStore for checkpointId %d with value %v: should be *sync.Map type", checkpointId, v)
		}
	}
	cstore.Store(opId, state)
	return nil
}

func (s *BadgerStore) SaveCheckpoint(checkpointId int64) error{
	if v, ok := s.mapStore.Load(checkpointId); !ok{
		return fmt.Errorf("store for checkpoint %d not found", checkpointId)
	}else{
		if m, ok := v.(*sync.Map); !ok{
			return fmt.Errorf("invalid BadgerStore for checkpointId %d with value %v: should be *sync.Map type", checkpointId, v)
		}else{
			db, err := common.DbOpen(s.dir)
			if err != nil {
				return fmt.Errorf("save checkpoint err: %v", err)
			}
			defer common.DbClose(db)
			b, ok := mapToBytes(m)
			if !ok {
				return fmt.Errorf("save checkpoint err: fail to encode states")
			}
			err = common.DbSet(db, string(checkpointId), b)
			if err != nil {
				return fmt.Errorf("save checkpoint err: %v", err)
			}
			m.Delete(checkpointId)
			s.checkpoints = append(s.checkpoints, checkpointId)
			//TODO is the order promised?
			if len(s.checkpoints) > s.max{
				cp := s.checkpoints[0]
				s.checkpoints = s.checkpoints[1:]
				go func(){
					common.DbDelete(db, string(cp))
				}()
			}
			cs, ok := sliceToBytes(s.checkpoints)
			if !ok {
				return fmt.Errorf("save checkpoint err: fail to encode checkpoint counts")
			}
			err = common.DbSet(db, "checkpoints", cs)
			if err != nil {
				return fmt.Errorf("save checkpoint err: %v", err)
			}
		}
	}
	return nil
}

func mapToBytes(sm *sync.Map) ([]byte, bool){
	m := make(map[string]interface{})
	sm.Range(func(k interface{}, v interface{}) bool {
		m[k.(string)] = v
		return true
	})
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(m); err != nil {
		return nil, false
	}
	return buf.Bytes(), true
}

func bytesToMap(input []byte) (*sync.Map, bool){
	var result map[string]interface{}
	buf := bytes.NewBuffer(input)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&result); err != nil {
		return nil, false
	}
	var f sync.Map
	for key, value := range result{
		f.Store(key, value)
	}
	return &f, true
}

func sliceToBytes(s []int64) ([]byte, bool){
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(s); err != nil {
		return nil, false
	}
	return buf.Bytes(), true
}

func bytesToSlice(input []byte) ([]int64, bool){
	result := make([]int64, 3)
	buf := bytes.NewBuffer(input)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(result); err != nil {
		return nil, false
	}
	return result, true
}

//TODO how to get the current checkpoint?
func (s *BadgerStore) RestoreState(opId string) map[string]interface{} {
	if sm, ok := s.mapStore.Load(opId);ok{
		if m, ok := sm.(map[string]interface{}); ok{
			return m
		}else{
			panic(fmt.Sprintf("invalid state %v stored for op %s: data type is not map[string]interface{}", sm, opId))
		}
	}else{
		return make(map[string]interface{})
	}
}
