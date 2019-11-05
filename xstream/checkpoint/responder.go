package checkpoint

import (
	context2 "engine/xstream/context"
)

type StreamTask interface {
	Broadcast(data interface{}) error
	GetName() string
	GetStreamContext() context2.StreamContext
	SetBarrierHandler(BarrierHandler)
	GetInputCount() int
	AddInputCount()
}

type Responder interface {
	TriggerCheckpoint(checkpointId int64) error
	GetName() string
}

type ResponderExecutor struct{
	responder chan<- *Signal
	task      StreamTask
}

func NewResponderExecutor(responder chan<- *Signal, task StreamTask) *ResponderExecutor{
	return &ResponderExecutor{
		responder: responder,
		task:      task,
	}
}

func (re *ResponderExecutor) GetName() string{
	return re.task.GetName()
}

func (re *ResponderExecutor) TriggerCheckpoint(checkpointId int64) error{
	sctx := re.task.GetStreamContext()
	log := sctx.GetLogger()
	name := re.GetName()
	log.Debugf("Starting checkpoint %d on task %s", checkpointId, name)
	//create
	barrier := &Barrier{
		CheckpointId: checkpointId,
		OpId:         name,
	}
	//broadcast barrier
	re.task.Broadcast(barrier)
	//Save key state to the global state
	go func(){
		state := ACK
		err := sctx.SaveToCheckpoint(checkpointId)
		if err != nil{
			log.Infof("save checkpoint error %s", err)
			state = DEC
		}

		signal := &Signal{
			Message: state,
			Barrier: Barrier{CheckpointId:checkpointId, OpId:name},
		}
		re.responder <- signal
		log.Debugf("Complete checkpoint %d on task %s", checkpointId, name)
	}()
	return nil
}
