// Copyright 2024 EMQ Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fvt

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/suite"
	"github.com/udhos/equalfile"
	"go.nanomsg.org/mangos/v3"
	"go.nanomsg.org/mangos/v3/protocol/pair"
	_ "go.nanomsg.org/mangos/v3/transport/tcp"
)

type GeelyTestSuite struct {
	suite.Suite
	mqttClient mqtt.Client
}

func (s *GeelyTestSuite) SetupTest() {
	opts := mqtt.NewClientOptions().AddBroker(MQTTBroker)
	mqttClient := mqtt.NewClient(opts)
	if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}
	s.mqttClient = mqttClient
}

func (s *GeelyTestSuite) TearDownTest() {
	if s.mqttClient != nil {
		s.mqttClient.Disconnect(0)
	}
}

func (s *GeelyTestSuite) TestSPIRules() {
	/// Prepare periodic rule
	// Create stream
	s.Run("creating stream spiOneSec", func() {
		streamJson := `{"sql": "CREATE STREAM spiOneSec() WITH (TYPE=\"mqtt\",FORMAT=\"spi\",DATASOURCE=\"canudp\",SCHEMAID=\"dbc\/geely\/geely.json\", SHARED=\"true\", CONF_KEY=\"spi_1sec\");"}`
		resp, err := client.CreateStream(streamJson)
		s.Require().NoError(err)
		s.Require().Equal(201, resp.StatusCode)
	})
	// Create rule
	s.Run("creating rule rule10secondCSV_ZSTD", func() {
		ruleStr, err := os.ReadFile(filepath.Join(PWD, RulesPath, "rule10Sec.json"))
		s.Require().NoError(err)
		resp, err := client.CreateRule(string(ruleStr))
		s.Require().NoError(err)
		s.Require().Equal(201, resp.StatusCode)
	})
	/// Prepare fetch rule
	// Create stream
	s.Run("creating stream spi", func() {
		streamJson := `{"sql": "CREATE STREAM spiStream() WITH (TYPE=\"mqtt\",FORMAT=\"spi\",DATASOURCE=\"canudp\",SCHEMAID=\"dbc\/geely\/geely.json\", SHARED=\"true\");"}`
		resp, err := client.CreateStream(streamJson)
		s.Require().NoError(err)
		s.Require().Equal(201, resp.StatusCode)
	})
	// Create rule
	s.Run("creating rule rulePick1", func() {
		ruleStr, err := os.ReadFile(filepath.Join(PWD, RulesPath, "rulePick.json"))
		s.Require().NoError(err)
		resp, err := client.CreateRule(string(ruleStr))
		s.Require().NoError(err)
		s.Require().Equal(201, resp.StatusCode)
	})
	// Subscribe to result topic
	var result []byte
	s.Run("subscribing to rulePick1", func() {
		s.mqttClient.Subscribe("result/pick1", 2, func(c mqtt.Client, message mqtt.Message) {
			result = message.Payload()
			s.mqttClient.Unsubscribe("result/pick1")
		})
	})
	/// Prepare diagnose rule
	// Create rule
	s.Run("creating rule d10001", func() {
		ruleStr, err := os.ReadFile(filepath.Join(PWD, RulesPath, "ruleDiagnose.json"))
		s.Require().NoError(err)
		resp, err := client.CreateRule(string(ruleStr))
		s.Require().NoError(err)
		s.Require().Equal(201, resp.StatusCode)
	})
	// Subscribe to result topic
	d1result := make([]string, 0, 5)
	s.Run("subscribing to d10001", func() {
		count := 5
		s.mqttClient.Subscribe("result/dia10001", 2, func(c mqtt.Client, message mqtt.Message) {
			result = message.Payload()
			d1result = append(d1result, string(message.Payload()))
			count--
			if count == 0 {
				s.mqttClient.Unsubscribe("result/dia10001")
			}
		})
	})
	time.Sleep(ContantInterval)
	// Check rule status
	s.Run("check rule10secondCSV_ZSTD status before feeding data", func() {
		metrics, err := client.GetRulStatus("rule10secondCSV_ZSTD")
		s.Require().NoError(err)
		s.Require().Equal("running", metrics["status"])
		s.Require().Equal(0.0, metrics["source_spiOneSec_0_records_in_total"])
	})
	// Check rule status
	s.Run("check rulePick1 status before feeding data", func() {
		metrics, err := client.GetRulStatus("rulePick1")
		s.Require().NoError(err)
		s.Require().Equal("running", metrics["status"])
		s.Require().Equal(0.0, metrics["source_spiStream_0_records_in_total"])
	})
	// Check rule status
	s.Run("check d10001 status before feeding data", func() {
		metrics, err := client.GetRulStatus("d10001")
		s.Require().NoError(err)
		s.Require().Equal("running", metrics["status"])
		s.Require().Equal(0.0, metrics["source_spiStream_0_records_in_total"])
	})
	/// Publish shared data
	s.Run("publish data", func() {
		pubSpi(s.mqttClient, filepath.Join(PWD, DataPath, "spi100.lines"))
	})
	time.Sleep(ContantInterval)
	/// Assert periodic rule
	// Check rule status after feeding data
	s.Run("check periodic rule status after feeding data", func() {
		metrics, err := client.GetRulStatus("rule10secondCSV_ZSTD")
		s.NoError(err)
		s.Require().Equal("running", metrics["status"])
		s.Require().Equal(100.0, metrics["source_spiOneSec_0_records_in_total"])
		s.Require().Equal(20.0, metrics["sink_file_0_0_records_out_total"])
	})
	// Check file
	s.Run("check generate file", func() {
		cmp := equalfile.New(nil, equalfile.Options{Debug: true})
		equal, err := cmp.CompareFile(filepath.Join(PWD, ResultPath, "mock.zstd"), filepath.Join(EKPWD, "data", "mock.zstd"))
		s.NoError(err)
		passed := s.True(equal, "files differ")
		if passed {
			_ = os.Remove(filepath.Join(EKPWD, "data", "mock.zstd"))
		}
	})
	/// assert fetch rule
	s.Run("check fetch rule status after feeding data", func() {
		metrics, err := client.GetRulStatus("rulePick1")
		s.NoError(err)
		s.Require().Equal("running", metrics["status"])
		s.Require().Equal(100.0, metrics["source_spiStream_0_records_in_total"])
		s.Require().Equal(1.0, metrics["sink_mqtt_0_0_records_out_total"])
	})
	// Check result
	s.Run("check result after feeding data", func() {
		// Create a new zstd decoder
		decoder, err := zstd.NewReader(nil)
		s.Require().NoError(err)
		defer decoder.Close()

		// Decompress the data
		decompressedData, err := decoder.DecodeAll(result, nil)
		s.Require().NoError(err)
		exp, err := os.ReadFile(filepath.Join(PWD, ResultPath, "rulePick.csv"))
		s.Require().NoError(err)
		s.Equal(string(exp), string(decompressedData))
	})
	/// assert diagnose rule
	s.Run("check diagnose rule status after feeding data", func() {
		metrics, err := client.GetRulStatus("d10001")
		s.NoError(err)
		s.Require().Equal("running", metrics["status"])
		s.Equal(100.0, metrics["source_spiStream_0_records_in_total"])
		s.Equal(52.0, metrics["sink_mqtt_0_0_records_out_total"])
	})
	// Check result
	s.Run("check diagnose result after feeding data", func() {
		exp := []string{"{\"ZCU_CANFD2.0x138.DrvrSeatBtnPsd\":0}", "{\"ZCU_CANFD2.0x138.DrvrSeatBtnPsd\":1}", "{\"ZCU_CANFD2.0x138.DrvrSeatBtnPsd\":0}", "{\"ZCU_CANFD2.0x138.DrvrSeatBtnPsd\":1}", "{\"ZCU_CANFD2.0x138.DrvrSeatBtnPsd\":0}"}
		s.Equal(exp, d1result)
	})
	/// clean up
	s.Run("delete rule rule10secondCSV_ZSTD", func() {
		resp, err := client.DeleteRule("rule10secondCSV_ZSTD")
		s.NoError(err)
		s.Equal(200, resp.StatusCode)
	})
	s.Run("delete stream spiOneSec", func() {
		resp, err := client.DeleteStream("spiOneSec")
		s.NoError(err)
		s.Equal(200, resp.StatusCode)
	})
	s.Run("delete rule rulePick1", func() {
		resp, err := client.DeleteRule("rulePick1")
		s.NoError(err)
		s.Equal(200, resp.StatusCode)
	})
	s.Run("delete diagnose rule", func() {
		resp, err := client.DeleteRule("d10001")
		s.NoError(err)
		s.Equal(200, resp.StatusCode)
	})
	s.Run("delete stream spiStream", func() {
		resp, err := client.DeleteStream("spiStream")
		s.NoError(err)
		s.Equal(200, resp.StatusCode)
	})
}

func pubSpi(client mqtt.Client, s string) {
	topic := "canudp"
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

func (s *GeelyTestSuite) TestLEDRules() {
	s.Run("creating stream sigStream", func() {
		streamJson := `{"sql": "CREATE STREAM sigStream() WITH (TYPE=\"mqtt\",FORMAT=\"sigl\",DATASOURCE=\"signal\", SHARED=\"true\");"}`
		resp, err := client.CreateStream(streamJson)
		s.Require().NoError(err)
		s.Require().Equal(201, resp.StatusCode)
	})
	s.Run("creating rule ruleSig", func() {
		ruleStr, err := os.ReadFile(filepath.Join(PWD, RulesPath, "ruleSig.json"))
		s.Require().NoError(err)
		resp, err := client.CreateRule(string(ruleStr))
		s.Require().NoError(err)
		s.Require().Equal(201, resp.StatusCode)
	})
	// Subscribe to result topic
	limit := 3
	sigResult := make([]string, 0, limit)
	s.Run("subscribing to sig", func() {
		s.mqttClient.Subscribe("result/sig", 2, func(c mqtt.Client, message mqtt.Message) {
			sigResult = append(sigResult, string(message.Payload()))
			limit--
			if limit == 0 {
				s.mqttClient.Unsubscribe("result/sig")
			}
		})
	})
	time.Sleep(ContantInterval)
	// Check rule status
	s.Run("check ruleSig status before feeding data", func() {
		metrics, err := client.GetRulStatus("ruleSig")
		s.Require().NoError(err)
		s.Require().Equal("running", metrics["status"])
		ok := s.Equal(0.0, metrics["source_sigStream_0_records_in_total"])
		if !ok {
			s.T().Log(metrics)
		}
	})
	// Publish led data
	s.Run("publish led data", func() {
		pubLed(s.mqttClient, filepath.Join(PWD, DataPath, "led100.lines"))
	})
	time.Sleep(ContantInterval)
	// Check rule status
	s.Run("check ruleSig status after feeding data", func() {
		metrics, err := client.GetRulStatus("ruleSig")
		s.Require().NoError(err)
		s.Require().Equal("running", metrics["status"])
		ok := s.Equal(100.0, metrics["source_sigStream_0_records_out_total"])
		s.Equal(40.0, metrics["sink_mqtt_0_0_records_out_total"])
		if !ok {
			s.T().Log(metrics)
		}
	})
	// Compare result
	s.Run("check ruleSig result", func() {
		exp := []string{"{\"n\":[\"00 fe 0c c2 98 59 7c 02\",\"01 1b 84 f4 50 7d 51 88\",\"02 10 7d 0a be 02 cc 8e\",\"03 89 f7 88 47 c4 0c e5\",\"04 a7 f9 f3 7c 30 7c 5c\",\"05 59 ea 75 25 7c fd 09\",\"06 32 6f 0c e1 f2 05 b0\",\"07 b1 08 e7 47 e3 ce 6b\",\"08 9d fa 59 b7 e2 00 dc\",\"09 ad c2 98 11 9b 12 05\",\"0a 44 73 b7 fd 16 fc 8d\",\"0b 62 35 4c ad f0 1d 74\",\"0c 60 21 86 b7 a9 97 cc\",\"0d e3 59 ec f2 12 30 10\",\"0e 4c 09 e3 0d 42 f6 27\",\"0f 29 a1 55 a6 e9 6e e0\",\"10 04 36 a2 36 df 13 8b\",\"11 6e 0b 4a 8e b3 0e bd\",\"12 17 52 c9 b4 1f 77 68\",\"13 0c 52 8a 75 71 70 15\"],\"ts\":1723536192547}", "{\"n\":[\"00 fe 0c 80 98 59 7c 02\",\"01 1b 80 f4 50 7d 51 88\",\"06 32 6f 0c e1 42 05 b0\",\"07 00 08 e7 47 e3 ce 6b\",\"08 9d a2 59 b7 e2 00 dc\",\"0a 44 73 b7 fd 04 fc 8d\",\"0d e3 59 e8 f2 12 30 10\",\"10 04 36 a2 36 df 13 89\",\"12 17 52 08 b4 1f 77 68\"],\"ts\":1723536193708}", "{\"n\":[\"00 c6 0c 80 98 59 7c 02\",\"07 00 08 e7 47 e3 ce 21\",\"09 ad c2 08 11 9b 12 05\",\"0a 44 21 b7 fd 04 fc 8d\",\"0d e3 00 e8 f2 12 30 10\",\"11 6e 0b 4a 8e b3 00 85\",\"12 17 52 08 b4 1f 15 68\"],\"ts\":1723536194800}"}
		s.Equal(exp, sigResult)
	})
	// Cleanup
	s.Run("delete ruleSig", func() {
		resp, err := client.DeleteRule("ruleSig")
		s.NoError(err)
		s.Equal(200, resp.StatusCode)
	})
	s.Run("delete stream sigStream", func() {
		resp, err := client.DeleteStream("sigStream")
		s.NoError(err)
		s.Equal(200, resp.StatusCode)
	})
}

func pubLed(client mqtt.Client, s string) {
	topic := "signal"
	interval := 1
	fmt.Printf("start publishing to %s, topic %s with interval %d ms\n", MQTTBroker, topic, interval)
	ticker := time.NewTicker(time.Duration(interval) * time.Millisecond)
	file, err := os.Open(s)
	if err != nil {
		panic(err)
	}
	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)
	count := 0
	for scanner.Scan() {
		count++
		line := scanner.Text()
		b, _ := hex.DecodeString(line)
		if token := client.Publish(topic, 0, false, b); token.Wait() && token.Error() != nil {
			fmt.Println(token.Error())
		}
		// Make sure the gap is always the interval for each 5 data
		now := <-ticker.C
		if count%10 == 0 {
			fmt.Printf("publish %dth at %v with data %x \n", count, now, b)
		}
	}
	_ = file.Close()
}

func (s *GeelyTestSuite) TestHistoryQuery() {
	// Start mock server
	var pairServer mangos.Socket
	s.Run("start mock server", func() {
		raw, err := os.ReadFile(filepath.Join(PWD, DataPath, "spi100.bin"))
		s.Require().NoError(err)
		pairServer, err = mockPair("tcp://0.0.0.0:10000", raw)
		s.Require().NoError(err)
	})
	s.Run("creating triggerStream", func() {
		streamJson := `{"sql": "CREATE STREAM triggerStream() WITH (TYPE=\"mqtt\",FORMAT=\"json\",DATASOURCE=\"trigger\",SHARED=\"true\");"}`
		resp, err := client.CreateStream(streamJson)
		s.Require().NoError(err)
		ok := s.Equal(201, resp.StatusCode)
		if !ok {
			s.T().Log(resp.Body)
		}
	})
	s.Run("creating ruleTrigger", func() {
		ruleStr, err := os.ReadFile(filepath.Join(PWD, RulesPath, "ruleTrigger.json"))
		s.Require().NoError(err)
		resp, err := client.CreateRule(string(ruleStr))
		s.Require().NoError(err)
		ok := s.Equal(201, resp.StatusCode)
		if !ok {
			s.T().Log(resp.Body)
		}
	})
	s.Run("creating queryStream", func() {
		streamJson := `{"sql": "CREATE STREAM queryStream() WITH (TYPE=\"nanoquery\",FORMAT=\"spi\",SCHEMAID=\"dbc\/geely\/geely.json\",SHARED=\"true\");"}`
		resp, err := client.CreateStream(streamJson)
		s.Require().NoError(err)
		ok := s.Equal(201, resp.StatusCode)
		if !ok {
			s.T().Log(resp.Body)
		}
	})
	// Create rule before mock server started to test auto connect
	s.Run("creating queryRule", func() {
		ruleStr, err := os.ReadFile(filepath.Join(PWD, RulesPath, "ruleHistoryQuery.json"))
		s.Require().NoError(err)
		resp, err := client.CreateRule(string(ruleStr))
		s.Require().NoError(err)
		ok := s.Equal(201, resp.StatusCode)
		if !ok {
			s.T().Log(resp.Body)
		}
	})
	time.Sleep(ContantInterval)
	// Check rule status
	s.Run("check queryRule status before feeding data", func() {
		metrics, err := client.GetRulStatus("ruleHistoryQuery1")
		s.Require().NoError(err)
		s.Require().Equal("running", metrics["status"])
		ok := s.Equal(0.0, metrics["op_queryStream_3_decoder_0_messages_processed_total"])
		if !ok {
			s.T().Log(metrics)
		}
	})
	// Send query request
	s.Run("send query request", func() {
		s.mqttClient.Publish("trigger", 2, false, "{\"ts1\":1723529200011, \"ts2\": 1723529300011}")
	})
	// wait until query is received
	s.Run("check trigger status after feeding data", func() {
		ticker := time.NewTicker(ContantInterval)
		defer ticker.Stop()
		count := 100
		for count > 0 {
			<-ticker.C
			count--
			metrics, err := client.GetRulStatus("ruleTrigger")
			s.Require().NoError(err)
			if metrics["sink_nanoquery_0_0_records_out_total"] == 1.0 {
				break
			}
		}
		metrics, err := client.GetRulStatus("ruleTrigger")
		s.Require().NoError(err)
		s.Require().Equal("running", metrics["status"])
		ok := s.Equal(1.0, metrics["sink_nanoquery_0_0_records_out_total"])
		if !ok {
			s.T().Log(metrics)
		}
		s.Require().True(ok)
	})
	// Check result until match
	s.Run("check result status after feeding data", func() {
		ticker := time.NewTicker(ContantInterval)
		defer ticker.Stop()
		count := 100
		for count > 0 {
			<-ticker.C
			count--
			metrics, err := client.GetRulStatus("ruleHistoryQuery1")
			s.Require().NoError(err)
			if metrics["status"] == "stopped" {
				break
			}
		}
		metrics, err := client.GetRulStatus("ruleHistoryQuery1")
		s.Require().NoError(err)
		s.Require().Equal("stopped", metrics["status"])
		//ok := s.Equal(2.0, metrics["sink_file_0_0_records_out_total"])
		//if !ok {
		//	s.T().Log(metrics)
		//}
	})
	time.Sleep(ContantInterval)
	// compare result file
	s.Run("check generate file", func() {
		cmp := equalfile.New(nil, equalfile.Options{Debug: true})
		equal, err := cmp.CompareFile(filepath.Join(PWD, ResultPath, "ruleHistoryQuery1.zstd"), filepath.Join(EKPWD, "data", "ruleHistoryQuery1.zstd"))
		s.NoError(err)
		passed := s.True(equal, "files differ")
		if passed {
			_ = os.Remove(filepath.Join(EKPWD, "data", "ruleHistoryQuery1.zstd"))
		}
	})
	// Cleanup
	s.Run("delete ruleTrigger", func() {
		resp, err := client.DeleteRule("ruleTrigger")
		s.NoError(err)
		s.Equal(200, resp.StatusCode)
	})
	s.Run("delete ruleHistoryQuery1", func() {
		resp, err := client.DeleteRule("ruleHistoryQuery1")
		s.NoError(err)
		s.Equal(200, resp.StatusCode)
	})
	s.Run("delete stream queryStream", func() {
		resp, err := client.DeleteStream("queryStream")
		s.NoError(err)
		s.Equal(200, resp.StatusCode)
	})
	s.Run("delete stream triggerStream", func() {
		resp, err := client.DeleteStream("triggerStream")
		s.NoError(err)
		s.Equal(200, resp.StatusCode)
	})
	s.Run("close mock server", func() {
		if pairServer != nil {
			pairServer.Close()
		}
	})
}

func mockPair(url string, raw []byte) (mangos.Socket, error) {
	var (
		sock mangos.Socket
		err  error
	)
	if sock, err = pair.NewSocket(); err != nil {
		log.Fatalf("can't get new pair socket: %s", err)
	}
	if err = sock.Listen(url); err != nil {
		log.Fatalf("can't listen on pair socket: %s", err.Error())
	}
	log.Printf("listen on pair socket")
	go func() {
		eof, _ := hex.DecodeString("0bad")
		for {
			// Could also use sock.RecvMsg to get header
			msg, err := sock.Recv()
			if err != nil {
				log.Printf("cannot receive on pair socket: %s", err.Error())
				return
			}
			fmt.Printf("NODE0: RECEIVED DATE REQUEST %s\n", msg)
			cols := strings.Split(string(msg), "-")
			if len(cols) == 3 { // no need to terminate
				for i := 0; i < 5; i++ {
					fmt.Printf("sends %d piece\n", i)
					err = sock.Send(raw)
					if err != nil {
						log.Fatalf("can't send reply: %s", err.Error())
					}
					time.Sleep(100 * time.Millisecond)
				}
				_ = sock.Send(eof)
				fmt.Printf("NODE0: SEND DATE REPLY %d\n", 5)
			}
		}
	}()
	return sock, nil
}

func TestGeelyTestSuite(t *testing.T) {
	suite.Run(t, new(GeelyTestSuite))
}
