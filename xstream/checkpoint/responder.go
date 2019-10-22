package checkpoint

import (
	"context"
	"engine/common"
)

type StreamTask interface {
	Broadcast(data interface{}) error
	GetName() string
	SetBarrierHandler(BarrierHandler)
	GetInputCount() int
	AddInputCount()
}

type Responder interface {
	TriggerCheckpoint(checkpointId int64, ctx context.Context) error
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

func (re *ResponderExecutor) TriggerCheckpoint(checkpointId int64, ctx context.Context) error{
	log := common.GetLogger(ctx)
	name := re.GetName()
	log.Debugf("Starting checkpoint %d on task %s", checkpointId, name)
	//create
	barrier := &Barrier{
		CheckpointId: checkpointId,
		OpId:         name,
	}
	//broadcast barrier
	re.task.Broadcast(barrier)

	go func(){
		//TODO take the snapshot, record the offset and change the state
		state := ACK

		signal := &Signal{
			Message: state,
			Barrier: Barrier{CheckpointId:checkpointId, OpId:name},
		}
		re.responder <- signal
		log.Debugf("Complete checkpoint %d on task %s", checkpointId, name)
	}()
	return nil
}
