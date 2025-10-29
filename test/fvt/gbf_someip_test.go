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

type SomeipTestSuite struct {
	suite.Suite
	mqttClient mqtt.Client
}

func (s *SomeipTestSuite) SetupTest() {
	opts := mqtt.NewClientOptions().AddBroker(MQTTBroker)
	mqttClient := mqtt.NewClient(opts)
	if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}
	s.mqttClient = mqttClient
}

func (s *SomeipTestSuite) TearDownTest() {
	if s.mqttClient != nil {
		s.mqttClient.Disconnect(0)
	}

}

func (s *SomeipTestSuite) TestSomeip() {
	s.Run("creating schema", func() {
		idlPath := filepath.Join(PWD, "test", "fvt", "data", "gbf", "ar.zip")
		idlUrl, err := FilePathToURL(idlPath)
		s.Require().NoError(err)
		resp, err := client.Post("schemas/gbf", fmt.Sprintf(`{"name":"ar","type":"gbf","file":"%s"}`, idlUrl))
		s.Require().NoError(err)
		s.Require().Equal(201, resp.StatusCode)
	})
	s.Run("creating streams", func() {
		streamJson, err := os.ReadFile(filepath.Join(PWD, RulesPath, "sipStream.json"))
		s.Require().NoError(err)
		resp, err := client.CreateStream(string(streamJson))
		s.Require().NoError(err)
		s.Require().Equal(201, resp.StatusCode)
	})
	// Create the simplest rule
	var result []string
	s.Run("setup rule gbf1", func() {
		ruleStr, err := os.ReadFile(filepath.Join(PWD, RulesPath, "sipRule.json"))
		s.Require().NoError(err)
		resp, err := client.CreateRule(string(ruleStr))
		s.Require().NoError(err)
		s.Require().Equal(201, resp.StatusCode)
		s.mqttClient.Subscribe("ek/sip1", 2, func(c mqtt.Client, message mqtt.Message) {
			result = append(result, string(message.Payload()))
		})
	})
	s.Run("creating 1s streams", func() {
		streamJson, err := os.ReadFile(filepath.Join(PWD, RulesPath, "sip1sStream.json"))
		s.Require().NoError(err)
		resp, err := client.CreateStream(string(streamJson))
		s.Require().NoError(err)
		s.Require().Equal(201, resp.StatusCode)
	})
	// Create the simplest rule
	var result2 []string
	s.Run("setup rule sip2", func() {
		ruleStr, err := os.ReadFile(filepath.Join(PWD, RulesPath, "sip2Rule.json"))
		s.Require().NoError(err)
		resp, err := client.CreateRule(string(ruleStr))
		s.Require().NoError(err)
		s.Require().Equal(201, resp.StatusCode)
		s.mqttClient.Subscribe("ek/sip2", 2, func(c mqtt.Client, message mqtt.Message) {
			result2 = append(result2, string(message.Payload()))
		})
	})
	time.Sleep(ConstantInterval)
	// Check rule status
	s.Run("check sip1 status before feeding data", func() {
		metrics, err := client.GetRuleStatus("sip1")
		fmt.Println(metrics)
		s.Require().NoError(err)
		s.Require().Equal("running", metrics["status"])
		s.Require().Equal(0.0, metrics["source_sipStream_0_records_in_total"])
	})
	/// Publish shared data
	s.Run("publish data", func() {
		pubgbf(s.mqttClient, filepath.Join(PWD, DataPath, "sip.lines"))
	})
	time.Sleep(ConstantInterval)
	// Check rule status after feeding data
	s.Run("check sip1 status after feeding data", func() {
		metrics, err := client.GetRuleStatus("sip1")
		s.NoError(err)
		s.Require().Equal("running", metrics["status"])
		s.Require().Equal(17.0, metrics["source_sipStream_0_records_in_total"])
		s.Require().Equal(17.0, metrics["sink_mqtt_0_0_records_out_total"])
		s.Require().NoError(err)
		exp := []string{"{\"firstSlot\":70,\"tplen\":0,\"ts\":29168173514840}", "{\"tplen\":0,\"ts\":29168173515352}", "{\"tplen\":0,\"ts\":29168173515864}", "{\"tplen\":0,\"ts\":29168173516376}", "{\"tplen\":0,\"ts\":29168173516888}", "{\"tplen\":0,\"ts\":29168173518936}", "{\"tplen\":0,\"ts\":29168173519448,\"tsrst\":1}", "{\"tplen\":0,\"ts\":29168173519960}", "{\"firstSlot\":70,\"tplen\":0,\"ts\":29168173520472}", "{\"tplen\":0,\"ts\":29168173520984}", "{\"tplen\":0,\"ts\":29168173523032}", "{\"tplen\":0,\"ts\":29168173523544}", "{\"tplen\":9,\"ts\":29168173524056}", "{\"firstSlot\":70,\"tplen\":0,\"ts\":29168173524568}", "{\"tplen\":0,\"ts\":29168173525080}", "{\"tplen\":0,\"ts\":29168173527128}", "{\"tplen\":0,\"ts\":29168173527640}"}
		s.Require().Equal(exp, result)
	})
	s.Run("check sip2 status after feeding data", func() {
		time.Sleep(time.Second)
		metrics, err := client.GetRuleStatus("sip2")
		s.NoError(err)
		s.Require().Equal("running", metrics["status"])
		s.Require().Equal(17.0, metrics["source_sip1sStream_0_records_in_total"])
		s.Require().Equal(1.0, metrics["sink_mqtt_0_0_records_out_total"])
		s.Require().NoError(err)
		exp := []string{"{\"firstSlot\":70,\"tplen\":9,\"ts\":29168173527640,\"tsrst\":1}"}
		s.Require().Equal(exp, result2)
	})
	s.Run("clean", func() {
		resp, err := client.DeleteRule("sip1")
		s.NoError(err)
		s.Equal(200, resp.StatusCode)
		resp, err = client.DeleteStream("sipStream")
		s.NoError(err)
		s.Equal(200, resp.StatusCode)
		resp, err = client.DeleteRule("sip2")
		s.NoError(err)
		s.Equal(200, resp.StatusCode)
		resp, err = client.DeleteStream("sip1sStream")
		s.NoError(err)
		s.Equal(200, resp.StatusCode)
		resp, err = client.Delete("schemas/gbf/ar")
		s.NoError(err)
		s.Equal(200, resp.StatusCode)
	})
}

func TestSomeipTestSuite(t *testing.T) {
	suite.Run(t, new(SomeipTestSuite))
}
