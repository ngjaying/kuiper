package fvt

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/stretchr/testify/suite"
)

type GBFTestSuite struct {
	suite.Suite
	mqttClient mqtt.Client
}

func (s *GBFTestSuite) SetupTest() {
	opts := mqtt.NewClientOptions().AddBroker(MQTTBroker)
	mqttClient := mqtt.NewClient(opts)
	if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}
	s.mqttClient = mqttClient
}

func (s *GBFTestSuite) TearDownTest() {
	if s.mqttClient != nil {
		s.mqttClient.Disconnect(0)
	}
}

func (s *GBFTestSuite) TestIDLRules() {
	s.Run("creating schema", func() {
		idlPath := filepath.Join(PWD, "test", "fvt", "data", "gbf", "gbf.zip")
		idlUrl, err := FilePathToURL(idlPath)
		s.Require().NoError(err)
		resp, err := client.Post("schemas/gbf", fmt.Sprintf(`{"name":"gbf","type":"gbf","file":"%s"}`, idlUrl))
		s.Require().NoError(err)
		s.Require().Equal(201, resp.StatusCode)
	})
	s.Run("creating streams", func() {
		streamJson, err := os.ReadFile(filepath.Join(PWD, RulesPath, "gbfStream.json"))
		s.Require().NoError(err)
		resp, err := client.CreateStream(string(streamJson))
		s.Require().NoError(err)
		s.Require().Equal(201, resp.StatusCode)
	})
	// Create the simplest rule
	var result []string
	s.Run("setup rule gbf1", func() {
		ruleStr, err := os.ReadFile(filepath.Join(PWD, RulesPath, "gbfRule.json"))
		s.Require().NoError(err)
		resp, err := client.CreateRule(string(ruleStr))
		s.Require().NoError(err)
		s.Require().Equal(201, resp.StatusCode)
		s.mqttClient.Subscribe("ek/result1", 2, func(c mqtt.Client, message mqtt.Message) {
			result = append(result, string(message.Payload()))
		})
	})
	s.Run("creating 1s streams", func() {
		streamJson, err := os.ReadFile(filepath.Join(PWD, RulesPath, "gbf1sStream.json"))
		s.Require().NoError(err)
		resp, err := client.CreateStream(string(streamJson))
		s.Require().NoError(err)
		s.Require().Equal(201, resp.StatusCode)
	})
	// Create the simplest rule
	var result2 []string
	s.Run("setup rule gbf2", func() {
		ruleStr, err := os.ReadFile(filepath.Join(PWD, RulesPath, "gbf2Rule.json"))
		s.Require().NoError(err)
		resp, err := client.CreateRule(string(ruleStr))
		s.Require().NoError(err)
		s.Require().Equal(201, resp.StatusCode)
		s.mqttClient.Subscribe("ek/result2", 2, func(c mqtt.Client, message mqtt.Message) {
			result2 = append(result2, string(message.Payload()))
		})
	})
	time.Sleep(ConstantInterval)
	// Check rule status
	s.Run("check gbf1 status before feeding data", func() {
		metrics, err := client.GetRuleStatus("gbf1")
		fmt.Println(metrics)
		s.Require().NoError(err)
		s.Require().Equal("running", metrics["status"])
		s.Require().Equal(0.0, metrics["source_gbfStream_0_records_in_total"])
	})
	/// Publish shared data
	s.Run("publish data", func() {
		pubgbf(s.mqttClient, filepath.Join(PWD, DataPath, "gbf.lines"))
	})
	time.Sleep(ConstantInterval)
	// Check rule status after feeding data
	s.Run("check gbf1 status after feeding data", func() {
		metrics, err := client.GetRuleStatus("gbf1")
		s.NoError(err)
		s.Require().Equal("running", metrics["status"])
		s.Require().Equal(5.0, metrics["source_gbfStream_0_records_in_total"])
		s.Require().Equal(5.0, metrics["sink_mqtt_0_0_records_out_total"])
		s.Require().NoError(err)
		s.Require().Equal([]string{"{\"ZCUDZCUCANFD2Fr36$BswAppVersion\":18446744073709551615,\"ts\":1731316891295}", "{\"Mess0$Mess0_Sig2\":1,\"ZCUDZCUCANFD2Fr36$BswAppVersion\":18446744073709551615,\"ts\":1731316895391}", "{\"Mess0$Mess0_Sig2\":1,\"ts\":1731316899487}", "{\"Mess0$Mess0_Sig2\":1,\"ZCUDZCUCANFD2Fr36$BswAppVersion\":18446744073709551615,\"ts\":1731316903583}", "{\"Mess0$Mess0_Sig2\":1,\"ts\":1731316907679}"}, result)
	})
	s.Run("check gbf2 status after feeding data", func() {
		time.Sleep(time.Second)
		metrics, err := client.GetRuleStatus("gbf2")
		s.NoError(err)
		s.Require().Equal("running", metrics["status"])
		s.Require().Equal(5.0, metrics["source_gbf1sStream_0_records_in_total"])
		s.Require().Equal(1.0, metrics["sink_mqtt_0_0_records_out_total"])
		s.Require().NoError(err)
		s.Require().Equal([]string{"{\"Mess0$Mess0_Sig2\":1,\"ts\":1731316907679}"}, result2)
	})
	s.Run("clean", func() {
		resp, err := client.DeleteRule("gbf1")
		s.NoError(err)
		s.Equal(200, resp.StatusCode)
		resp, err = client.DeleteStream("gbfStream")
		s.NoError(err)
		s.Equal(200, resp.StatusCode)
		resp, err = client.DeleteRule("gbf2")
		s.NoError(err)
		s.Equal(200, resp.StatusCode)
		resp, err = client.DeleteStream("gbf1sStream")
		s.NoError(err)
		s.Equal(200, resp.StatusCode)
		resp, err = client.Delete("schemas/gbf/gbf")
		s.NoError(err)
		s.Equal(200, resp.StatusCode)
	})
}

func pubgbf(client mqtt.Client, s string) {
	topic := "cangbf"
	interval := 200
	fmt.Printf("start publishing to %s, topic %s with interval %d ms\n", MQTTBroker, topic, interval)
	count := 1
	ticker := time.NewTicker(time.Duration(interval) * time.Millisecond)
	file, err := os.Open(s)
	if err != nil {
		panic(err)
	}
	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)
	for scanner.Scan() {
		line := scanner.Text()
		b, _ := hex.DecodeString(line)
		if token := client.Publish(topic, 0, false, b); token.Wait() && token.Error() != nil {
			fmt.Println(token.Error())
		}
		// Make sure the gap is always the interval for each 5 data
		if count%5 == 0 {
			now := <-ticker.C
			fmt.Printf("publish at %v with data %x \n", now, b)
		} else {
			var wait int
			// Make sure no tuple lies in the boundary
			if count/5%2 == 0 {
				wait = interval/10 - 10
			} else {
				wait = interval/10 + 10
			}
			time.Sleep(time.Duration(wait) * time.Millisecond)
		}
		count++
	}
	_ = file.Close()
}

func TestGBFTestSuite(t *testing.T) {
	suite.Run(t, new(GBFTestSuite))
}
