package checkpoint

import (
	"context"
	"engine/common"
	"time"
)

type pendingCheckpoint struct {
	checkpointId int64
	isDiscarded	 bool
	notYetAckTasks map[string]bool
}

func newPendingCheckpoint(checkpointId int64, tasksToWaitFor []Responder) *pendingCheckpoint{
	pc := &pendingCheckpoint{checkpointId: checkpointId}
	nyat := make(map[string]bool)
	for _, r := range tasksToWaitFor{
		nyat[r.GetName()] = true
	}
	pc.notYetAckTasks = nyat
	return pc
}

func (c *pendingCheckpoint) ack(opId string) bool{
	if c.isDiscarded{
		return false
	}
	delete(c.notYetAckTasks, opId)
	//TODO serialize state
	return true
}

func (c *pendingCheckpoint) isFullyAck() bool{
	return len(c.notYetAckTasks) == 0
}

func (c *pendingCheckpoint) finalize() *completedCheckpoint{
	ccp := &completedCheckpoint{checkpointId: c.checkpointId}
	return ccp
}

func (c *pendingCheckpoint) dispose(releaseState bool) {
	c.isDiscarded = true
}

type completedCheckpoint struct {
	checkpointId int64
}

type checkpointStore struct {
	maxNum int
	checkpoints []*completedCheckpoint
}

func (s *checkpointStore) add(c *completedCheckpoint){
	s.checkpoints = append(s.checkpoints, c)
	if len(s.checkpoints) > s.maxNum{
		s.checkpoints = s.checkpoints[1:]
	}
}

func (s *checkpointStore) getLatest() *completedCheckpoint{
	if len(s.checkpoints) > 0{
		return s.checkpoints[len(s.checkpoints) - 1]
	}
	return nil
}

type Coordinator struct {
	tasksToTrigger          []Responder
	tasksToWaitFor          []Responder
	pendingCheckpoints      map[int64]*pendingCheckpoint
	completedCheckpoints 	*checkpointStore
	ruleId                  string
	baseInterval            int
	timeout                 int
	advanceToEndOfEventTime bool
	ticker                  common.Ticker  //For processing time only
	signal                  chan *Signal
	ctx                     context.Context
}

func NewCoordinator(ruleId string, sources []StreamTask, operators []StreamTask, sinks []StreamTask, qos int, ctx context.Context) *Coordinator {
	signal := make(chan *Signal, 1024)
	var allResponders, sourceResponders []Responder
	for _, r := range sources{
		re := NewResponderExecutor(signal, r)
		allResponders = append(allResponders, re)
		sourceResponders = append(sourceResponders, re)
	}
	for _, r := range operators{
		re := NewResponderExecutor(signal, r)
		handler := createBarrierHandler(re, r.GetInputCount(), qos)
		r.SetBarrierHandler(handler)
		allResponders = append(allResponders, re)
	}
	for _, r := range sinks{
		re := NewResponderExecutor(signal, r)
		handler := NewBarrierTracker(re, r.GetInputCount())
		r.SetBarrierHandler(handler)
		allResponders = append(allResponders, re)
	}
	return &Coordinator{
		tasksToTrigger:     sourceResponders,
		tasksToWaitFor:     allResponders,
		pendingCheckpoints: make(map[int64]*pendingCheckpoint),
		completedCheckpoints: &checkpointStore{
			maxNum:      3,
		},
		ruleId:             ruleId,
		signal:             signal,
		baseInterval:       300000, //5 minutes by default
		timeout:            200000,
		ctx:				ctx,
	}
}

func createBarrierHandler(re Responder, inputCount int, qos int) BarrierHandler{
	if qos == 1{
		return NewBarrierTracker(re, inputCount)
	}else if qos == 2{
		return NewBarrierAligner(re, inputCount)
	}else{
		return nil
	}
}

func (c *Coordinator) Activate() error {
	exeCtx, cancel := context.WithCancel(c.ctx)
	log := common.GetLogger(c.ctx)
	if c.ticker != nil{
		c.ticker.Stop()
	}
	c.ticker = common.GetTicker(c.baseInterval)
	tc := c.ticker.GetC()
	go func(){
		for {
			select {
			case <- tc:
				//trigger checkpoint
				//TODO pose max attempt and min pause check for consequent pendingCheckpoints

				// TODO Check if all tasks are running

				//Create a pending checkpoint
				checkpointId := common.GetMockNow()
				checkpoint := newPendingCheckpoint(checkpointId, c.tasksToWaitFor)
				log.Debugf("Create checkpoint %d", checkpointId)
				c.pendingCheckpoints[checkpointId] = checkpoint
				//Let the sources send out a barrier
				for _, r := range c.tasksToTrigger{
					go func() {
						if err := r.TriggerCheckpoint(checkpointId, c.ctx); err != nil{
							log.Infof("Fail to trigger checkpoint for source %s with error %v", r.GetName(), err)
							c.cancel(checkpointId)
						}else{
							time.Sleep(time.Duration(c.timeout) * time.Microsecond)
							c.cancel(checkpointId)
						}
					}()
				}
			case s := <- c.signal:
				switch s.Message {
				case STOP:
					log.Debug("Stop checkpoint scheduler")
					if c.ticker != nil{
						c.ticker.Stop()
					}
					return
				case ACK:
					log.Debugf("Receive ack from %s for checkpoint %d", s.OpId, s.CheckpointId)
					if checkpoint, ok := c.pendingCheckpoints[s.CheckpointId]; ok{
						checkpoint.ack(s.OpId)
						if checkpoint.isFullyAck(){
							c.complete(s.CheckpointId)
						}
					}else{
						log.Debugf("Receive ack from %s for non existing checkpoint %d", s.OpId, s.CheckpointId)
					}
				case DEC:
					log.Debugf("Receive dec from %s for checkpoint %d", s.OpId, s.CheckpointId)
					c.cancel(s.CheckpointId)
				}
			case <-exeCtx.Done():
				log.Println("Cancelling coordinator....")
				if c.ticker != nil{
					c.ticker.Stop()
				}
				cancel()
				return
			}
		}
	}()
	return nil
}

func (c *Coordinator) Deactivate() error {
	if c.ticker != nil{
		c.ticker.Stop()
	}
	c.signal <- &Signal{Message: STOP}
	return nil
}

func (c *Coordinator) cancel(checkpointId int64){
	log := common.GetLogger(c.ctx)
	if checkpoint, ok := c.pendingCheckpoints[checkpointId]; ok{
		delete(c.pendingCheckpoints, checkpointId)
		checkpoint.dispose(true)
	}else{
		log.Debugf("Cancel for non existing checkpoint %d. Just ignored", checkpointId)
	}
}

func (c *Coordinator) complete(checkpointId int64){
	log := common.GetLogger(c.ctx)

	if ccp, ok := c.pendingCheckpoints[checkpointId];ok{
		//TODO save the checkpoint

		c.completedCheckpoints.add(ccp.finalize())
		delete(c.pendingCheckpoints, checkpointId)
		//Drop the previous pendingCheckpoints
		for cid, cp := range c.pendingCheckpoints {
			if cid < checkpointId{
				//TODO revisit how to abort a checkpoint, discard callback
				cp.isDiscarded = true
				delete(c.pendingCheckpoints, cid)
			}
		}
		log.Debugf("Complete checkpoint %d", checkpointId)
	}else{
		log.Infof("Cannot find checkpoint %d to complete", checkpointId)
	}
}

//For testing
func (c *Coordinator) GetCompleteCount() int {
	return len(c.completedCheckpoints.checkpoints)
}

func (c *Coordinator) GetLatest() int64 {
	return c.completedCheckpoints.getLatest().checkpointId
}