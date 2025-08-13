package fvt

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/stretchr/testify/suite"
)

type IDLTestSuite struct {
	suite.Suite
	mqttClient mqtt.Client
}

func (s *IDLTestSuite) SetupTest() {
	opts := mqtt.NewClientOptions().AddBroker(MQTTBroker)
	mqttClient := mqtt.NewClient(opts)
	if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}
	s.mqttClient = mqttClient
}

func (s *IDLTestSuite) TearDownTest() {
	if s.mqttClient != nil {
		s.mqttClient.Disconnect(0)
	}
}

func (s *IDLTestSuite) TestIDLRules() {
	s.Run("clean", func() {
		_, err := client.DeleteRule("idl1")
		s.NoError(err)
		_, err = client.DeleteStream("idldemo")
		s.NoError(err)
		_, err = client.Delete("schemas/idl/idl")
		s.NoError(err)
	})
	s.Run("creating schema", func() {
		idlPath := filepath.Join(PWD, "test", "fvt", "data", "idl", "test.idl")
		idlUrl, err := FilePathToURL(idlPath)
		s.Require().NoError(err)
		resp, err := client.Post("schemas/idl", fmt.Sprintf(`{"name":"idl","type":"idl","file":"%s"}`, idlUrl))
		s.Require().NoError(err)
		s.Require().Equal(201, resp.StatusCode)
	})
	s.Run("creating streams", func() {
		streamJson, err := os.ReadFile(filepath.Join(PWD, RulesPath, "idlStream.json"))
		s.Require().NoError(err)
		resp, err := client.CreateStream(string(streamJson))
		s.Require().NoError(err)
		s.Require().Equal(201, resp.StatusCode)
	})
	// Create the simplest rule
	var result []string
	s.Run("setup rule", func() {
		ruleStr, err := os.ReadFile(filepath.Join(PWD, RulesPath, "idlRule.json"))
		s.Require().NoError(err)
		resp, err := client.CreateRule(string(ruleStr))
		s.Require().NoError(err)
		s.Require().Equal(201, resp.StatusCode)
		s.mqttClient.Subscribe("ek/result1", 2, func(c mqtt.Client, message mqtt.Message) {
			result = append(result, string(message.Payload()))
		})
	})
	time.Sleep(ConstantInterval)
	// Check rule status
	s.Run("check status before feeding data", func() {
		metrics, err := client.GetRuleStatus("idl1")
		fmt.Println(metrics)
		s.Require().NoError(err)
		s.Require().Equal("running", metrics["status"])
		s.Require().Equal(0.0, metrics["source_idldemo_0_records_in_total"])
	})
	/// Publish shared data
	s.Run("publish data", func() {
		pubIDL(s.mqttClient)
	})
	time.Sleep(ConstantInterval)
	// Check rule status after feeding data
	s.Run("check idl1 status after feeding data", func() {
		metrics, err := client.GetRuleStatus("idl1")
		s.NoError(err)
		s.Require().Equal("running", metrics["status"])
		s.Require().Equal(5.0, metrics["source_idldemo_0_records_in_total"])
		s.Require().Equal(5.0, metrics["sink_mqtt_0_0_records_out_total"])
		s.Require().NoError(err)
		s.Require().Equal([]string{`{"id1":41,"id2":42}`, `{"id1":41,"id2":42}`, `{"id1":41,"id2":42}`, `{"id1":41,"id2":42}`, `{"id1":41,"id2":42}`}, result)
	})
	s.Run("clean", func() {
		resp, err := client.DeleteRule("idl1")
		s.NoError(err)
		s.Equal(200, resp.StatusCode)
		resp, err = client.DeleteStream("idldemo")
		s.NoError(err)
		s.Equal(200, resp.StatusCode)
		resp, err = client.Delete("schemas/idl/idl")
		s.NoError(err)
		s.Equal(200, resp.StatusCode)
	})
}

func pubIDL(client mqtt.Client) {
	topic := "idldemo"
	fmt.Printf("start publishing 5 data to %s, topic %s\n", MQTTBroker, topic)
	count := 5
	b := []byte{41, 42}
	for i := 0; i < count; i++ {
		if token := client.Publish(topic, 0, false, b); token.Wait() && token.Error() != nil {
			fmt.Println(token.Error())
		}
	}
}

func TestIdlTestSuite(t *testing.T) {
	suite.Run(t, new(IDLTestSuite))
}
