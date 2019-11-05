package sinks

import (
	"context"
	"engine/xsql"
	"engine/xstream/checkpoint"
	context2 "engine/xstream/context"
	"fmt"
	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
)

type MQTTSink struct {
	srv      string
	tpc      string
	clientid string

	input chan *xsql.BufferOrEvent
	conn MQTT.Client
	ruleId   string
	name 	 string
	barrierHandler checkpoint.BarrierHandler
	inputCount int
	sctx     context2.StreamContext
}

func NewMqttSink(name string, ruleId string, properties interface{}) (*MQTTSink, error) {
	ps, ok := properties.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("expect map[string]interface{} type for the mqtt sink properties")
	}
	srv, ok := ps["server"]
	if !ok {
		return nil, fmt.Errorf("mqtt sink is missing property server")
	}
	tpc, ok := ps["topic"]
	if !ok {
		return nil, fmt.Errorf("mqtt sink is missing property topic")
	}
	clientid, ok := ps["clientId"]
	if !ok{
		if uuid, err := uuid.NewUUID(); err != nil {
			return nil, fmt.Errorf("mqtt sink fails to get uuid, the error is %s", err)
		}else{
			clientid = uuid.String()
		}
	}
	ms := &MQTTSink{name:name, ruleId: ruleId, input: make(chan *xsql.BufferOrEvent), srv: srv.(string), tpc: tpc.(string), clientid: clientid.(string)}
	return ms, nil
}

func (ms *MQTTSink) GetName() string {
	return ms.name
}

func (ms *MQTTSink) GetInput() (chan<- *xsql.BufferOrEvent, string)  {
	return ms.input, ms.name
}

func (ms *MQTTSink) Open(sctx context2.StreamContext, result chan<- error) {
	ms.sctx = sctx
	log := sctx.GetLogger()
	log.Printf("Opening mqtt sink for rule %s", ms.ruleId)

	go func() {
		exeCtx, cancel := context.WithCancel(sctx.GetContext())
		opts := MQTT.NewClientOptions().AddBroker(ms.srv).SetClientID(ms.clientid)

		c := MQTT.NewClient(opts)
		if token := c.Connect(); token.Wait() && token.Error() != nil {
			result <- fmt.Errorf("Found error: %s", token.Error())
			cancel()
		}
		log.Printf("The connection to server %s was established successfully", ms.srv)
		ms.conn = c

		for {
			select {
			case item := <-ms.input:
				if ms.barrierHandler != nil && !item.Processed{
					//may be blocking
					isProcessed := ms.barrierHandler.Process(item, sctx)
					if isProcessed{
						break
					}
				}
				log.Infof("publish %s", item.Data)
				if token := c.Publish(ms.tpc, 0, false, item.Data); token.Wait() && token.Error() != nil {
					result <- fmt.Errorf("Publish error: %s", token.Error())
				}

			case <-exeCtx.Done():
				c.Disconnect(5000)
				log.Infof("Closing mqtt sink")
				cancel()
				return
			}
		}

	}()
}

func (ms *MQTTSink) Broadcast(data interface{}) error{
	//do nothing
	return nil
}

func (ms *MQTTSink) SetBarrierHandler(handler checkpoint.BarrierHandler) {
	ms.barrierHandler = handler
}

func (ms *MQTTSink) AddInputCount(){
	ms.inputCount++
}

func (ms *MQTTSink) GetInputCount() int{
	return ms.inputCount
}
func (ms *MQTTSink) GetStreamContext() context2.StreamContext{
	return ms.sctx
}
