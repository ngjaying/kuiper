package extensions

import (
	"context"
	"encoding/json"
	"engine/common"
	"engine/xsql"
	"engine/xstream/checkpoint"
	context2 "engine/xstream/context"
	"fmt"
	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/go-yaml/yaml"
	"github.com/google/uuid"
	"time"
)

type MQTTSource struct {
	srv      string
	tpc      string
	clientid string
	schema   map[string]interface{}
	outs  map[string]chan<- *xsql.BufferOrEvent
	conn MQTT.Client
	name 		string
	sctx      context2.StreamContext
}


type MQTTConfig struct {
	Qos string `yaml:"qos"`
	Sharedsubscription string `yaml:"sharedsubscription"`
	Servers []string `yaml:"servers"`
	Clientid string `yaml:"clientid"`
}


const confName string = "mqtt_source.yaml"

func NewWithName(name string, topic string, confKey string) (*MQTTSource, error) {
	b := common.LoadConf(confName)
	var cfg map[string]MQTTConfig
	if err := yaml.Unmarshal(b, &cfg); err != nil {
		return nil, err
	}

	ms := &MQTTSource{tpc: topic, name: name}
	ms.outs = make(map[string]chan<- *xsql.BufferOrEvent)
	if srvs := cfg[confKey].Servers; srvs != nil && len(srvs) > 1 {
		return nil, fmt.Errorf("It only support one server in %s section.", confKey)
	} else if srvs == nil {
		srvs = cfg["default"].Servers
		if srvs != nil && len(srvs) == 1 {
			ms.srv = srvs[0]
		} else {
			return nil, fmt.Errorf("Wrong configuration in default section!")
		}
	} else {
		ms.srv = srvs[0]
	}

	if cid := cfg[confKey].Clientid; cid != "" {
		ms.clientid = cid
	} else {
		ms.clientid = cfg["default"].Clientid
	}
	return ms, nil
}

func (ms *MQTTSource) WithSchema(schema string) *MQTTSource {
	return ms
}

func (ms *MQTTSource) GetName() string {
	return ms.name
}

func (ms *MQTTSource) AddOutput(output chan<- *xsql.BufferOrEvent, name string) {
	if _, ok := ms.outs[name]; !ok{
		ms.outs[name] = output
	}else{
		common.Log.Warnf("fail to add output %s, operator %s already has an output of the same name", name, ms.name)
	}
}

func (ms *MQTTSource) Open(sctx context2.StreamContext) error {
	ms.sctx = sctx
	log := sctx.GetLogger()
	go func() {
		exeCtx, cancel := context.WithCancel(sctx.GetContext())
		opts := MQTT.NewClientOptions().AddBroker(ms.srv)

		if ms.clientid == "" {
			if uuid, err := uuid.NewUUID(); err != nil {
				log.Printf("Failed to get uuid, the error is %s", err)
				cancel()
				return
			} else {
				opts.SetClientID(uuid.String())
			}
		} else {
			opts.SetClientID(ms.clientid)
		}

		h := func(client MQTT.Client, msg MQTT.Message) {
			if ms.tpc != msg.Topic() {
				return
			} else {
				log.Infof("received %s", msg.Payload())

				result := make(map[string]interface{})
				//The unmarshal type can only be bool, float64, string, []interface{}, map[string]interface{}, nil
				if e := json.Unmarshal(msg.Payload(), &result); e != nil {
					log.Errorf("Invalid data format, cannot convert %s into JSON with error %s", string(msg.Payload()), e)
					return
				}
				//Convert the keys to lowercase
				result = xsql.LowercaseKeyMap(result)
				tuple := &xsql.Tuple{Emitter: ms.tpc, Message:result, Timestamp: common.TimeToUnixMilli(time.Now())}
				ms.Broadcast(tuple)
			}
		}

		opts.SetDefaultPublishHandler(h)
		c := MQTT.NewClient(opts)
		if token := c.Connect(); token.Wait() && token.Error() != nil {
			log.Printf("Found error when connecting to %s for %s: %s", ms.srv, ms.name, token.Error())
			cancel()
			return
		}
		log.Printf("The connection to server %s was established successfully", ms.srv)
		ms.conn = c
		if token := c.Subscribe(ms.tpc, 0, nil); token.Wait() && token.Error() != nil {
			log.Printf("Found error: %s", token.Error())
			cancel()
			return
		}
		log.Printf("Successfully subscribe to topic %s", ms.tpc)
		select {
		case <-exeCtx.Done():
			log.Println("Mqtt Source Done")
			ms.conn.Disconnect(5000)
			cancel()
		}
	}()

	return nil
}

func (ms *MQTTSource) Broadcast(data interface{}) error{
	boe := &xsql.BufferOrEvent{
		Data:      data,
		Channel:   ms.name,
		Processed: false,
	}
	for _, out := range ms.outs{
		out <- boe
	}
	return nil
}

func (ms *MQTTSource) SetBarrierHandler(checkpoint.BarrierHandler) {
	//DO nothing for sources as it only emits barrier
}

func (ms *MQTTSource) AddInputCount(){
	//Do nothing
}

func (ms *MQTTSource) GetInputCount() int{
	return 0
}

func (ms *MQTTSource) GetStreamContext() context2.StreamContext{
	return ms.sctx
}