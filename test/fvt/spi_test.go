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
	"github.com/otiai10/copy"
	"github.com/stretchr/testify/suite"
)

type SpiTestSuite struct {
	suite.Suite
	mqttClient mqtt.Client
}

func (s *SpiTestSuite) SetupTest() {
	opts := mqtt.NewClientOptions().AddBroker(MQTTBroker)
	mqttClient := mqtt.NewClient(opts)
	if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}
	s.mqttClient = mqttClient
}

func (s *SpiTestSuite) TearDownTest() {
	if s.mqttClient != nil {
		s.mqttClient.Disconnect(0)
	}
}

func (s *SpiTestSuite) TestIDLRules() {
	s.Run("creating streams", func() {
		// EKPWD = "C:/repo/go/ekbuild"
		// Copy idl
		srcName := filepath.Join(PWD, "test", "fvt", "data", "spi")
		dstName := filepath.Join(EKPWD, "data", "uploads", "spi")
		err := copy.Copy(srcName, dstName)
		s.Require().NoError(err)

		streamJson, err := os.ReadFile(filepath.Join(PWD, RulesPath, "spiStream.json"))
		s.Require().NoError(err)
		resp, err := client.CreateStream(string(streamJson))
		s.Require().NoError(err)
		s.Require().Equal(201, resp.StatusCode)
	})
	// Create the simplest rule
	var result []string
	s.Run("setup rule spi1", func() {
		ruleStr, err := os.ReadFile(filepath.Join(PWD, RulesPath, "spiRule.json"))
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
	s.Run("check spi1 status before feeding data", func() {
		metrics, err := client.GetRuleStatus("spi1")
		fmt.Println(metrics)
		s.Require().NoError(err)
		s.Require().Equal("running", metrics["status"])
		s.Require().Equal(0.0, metrics["source_spiStream_0_records_in_total"])
	})
	/// Publish shared data
	s.Run("publish data", func() {
		pubSpi(s.mqttClient, filepath.Join(PWD, DataPath, "spi.lines"))
	})
	time.Sleep(ConstantInterval)
	// Check rule status after feeding data
	s.Run("check spi1 status after feeding data", func() {
		metrics, err := client.GetRuleStatus("spi1")
		s.NoError(err)
		s.Require().Equal("running", metrics["status"])
		s.Require().Equal(5.0, metrics["source_spiStream_0_records_in_total"])
		s.Require().Equal(5.0, metrics["sink_mqtt_0_0_records_out_total"])
		s.Require().NoError(err)
		s.Require().Equal([]string{"{\"ZCUDZCUCANFD2Fr36$BswAppVersion\":18446744073709551615,\"ts\":1731316891295}", "{\"Mess0$Mess0_Sig2\":1,\"ZCUDZCUCANFD2Fr36$BswAppVersion\":18446744073709551615,\"ts\":1731316895391}", "{\"Mess0$Mess0_Sig2\":1,\"ts\":1731316899487}", "{\"Mess0$Mess0_Sig2\":1,\"ZCUDZCUCANFD2Fr36$BswAppVersion\":18446744073709551615,\"ts\":1731316903583}", "{\"Mess0$Mess0_Sig2\":1,\"ts\":1731316907679}"}, result)
	})
	s.Run("clean", func() {
		resp, err := client.DeleteRule("spi1")
		s.NoError(err)
		s.Equal(200, resp.StatusCode)
		resp, err = client.DeleteStream("spiStream")
		s.NoError(err)
		s.Equal(200, resp.StatusCode)
		err = os.RemoveAll(filepath.Join(EKPWD, "data/uploads/spi"))
		if err != nil {
			fmt.Printf("remove idl error: %v\n", err)
		} else {
			fmt.Printf("remove idl done\n")
		}
	})
}

func pubSpi(client mqtt.Client, s string) {
	topic := "canspi"
	interval := 1000
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

func TestGeeTestSuite(t *testing.T) {
	suite.Run(t, new(SpiTestSuite))
}
