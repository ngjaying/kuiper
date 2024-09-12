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
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/stretchr/testify/suite"
)

type CIMETestSuite struct {
	suite.Suite
	mqttClient mqtt.Client
}

func (s *CIMETestSuite) SetupTest() {
	err := copyDir(filepath.Join(PWD, DataPath, "cime"), filepath.Join(EKPWD, "data", "cime"))
	s.Require().NoError(err)
	opts := mqtt.NewClientOptions().AddBroker(MQTTBroker)
	mqttClient := mqtt.NewClient(opts)
	if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}
	s.mqttClient = mqttClient

}

func (s *CIMETestSuite) TearDownTest() {
	if s.mqttClient != nil {
		s.mqttClient.Disconnect(0)
	}
}

func TestCIMETestSuite(t *testing.T) {
	suite.Run(t, new(CIMETestSuite))
}

func (s *CIMETestSuite) TestGuangdong() {
	/// Prepare periodic rule
	// Create conf
	s.Run("creating conf", func() {
		conf := map[string]any{
			"fileType":         "cime",
			"path":             filepath.Join(EKPWD, "data", "cime"),
			"delimiter":        "\t",
			"ignoreStartLines": 4,
			"ignoreEndLines":   1,
			"columns":          []string{"ns", "id", "power", "cap"},
		}
		resp, err := client.CreateConf("sources/file/confKeys/cime", conf)
		s.Require().NoError(err)
		s.Require().Equal(http.StatusOK, resp.StatusCode)
	})
	// Create stream
	s.Run("creating stream", func() {
		streamJson := `{"sql": "CREATE STREAM cimeStream() WITH (TYPE=\"file\",FORMAT=\"delimited\",DELIMITER=\"\t\",DATASOURCE=\"gd.dat\",CONF_KEY=\"cime\",SHARED=\"true\");"}`
		resp, err := client.CreateStream(streamJson)
		s.Require().NoError(err)
		s.Require().Equal(201, resp.StatusCode)
	})
	// Subscribe to result topic
	var results []string
	s.Run("subscribing to result", func() {
		s.mqttClient.Subscribe("result/ruleCimE", 2, func(c mqtt.Client, message mqtt.Message) {
			if len(results) >= 3 {
				s.mqttClient.Unsubscribe("result/ruleCimE")
			} else {
				results = append(results, string(message.Payload()))
			}

		})
	})
	// Create rule
	s.Run("creating rule", func() {
		ruleStr := `
			{
			  "id": "ruleCimE",
			  "name": "Read CIM E file and replay with ts",
			  "sql": "SELECT * FROM cimeStream",
			  "actions": [
				{
				  "mqtt": {
					"server": "tcp://127.0.0.1:1883",
					"topic": "result/ruleCimE",
					"sendSingle": true
				  }
				}
			  ]
			}
			`
		resp, err := client.CreateRule(ruleStr)
		s.Require().NoError(err)
		s.Require().Equal(201, resp.StatusCode)
	})
	/// assert fetch rule
	s.Run("check fetch rule status", func() {
		try := 10
		for i := 0; i < try; i++ {
			time.Sleep(100 * time.Millisecond)
			metrics, err := client.GetRulStatus("ruleCimE")
			s.NoError(err)
			if metrics["status"] == "stopped" {
				break
			}
		}
		metrics, err := client.GetRulStatus("ruleCimE")
		s.NoError(err)
		s.T().Log(metrics)
		s.Require().Equal("stopped", metrics["status"])
		s.Require().Equal(3.0, metrics["source_cimeStream_0_records_out_total"])
		s.Require().Equal(3.0, metrics["sink_mqtt_0_0_records_out_total"])

	})
	// Check result
	s.Run("check result", func() {
		exp := []string{
			"{\"cap\":\"30.00\",\"id\":\"1\",\"ns\":\"#\",\"power\":\"0.00\",\"ts\":1697068800000}",
			"{\"cap\":\"30.00\",\"id\":\"2\",\"ns\":\"#\",\"power\":\"0.00\",\"ts\":1697069700000}",
			"{\"cap\":\"30.00\",\"id\":\"3\",\"ns\":\"#\",\"power\":\"0.00\",\"ts\":1697070600000}",
		}
		s.Equal(exp, results)
	})
	/// clean up
	s.Run("delete rule", func() {
		resp, err := client.DeleteRule("ruleCimE")
		s.NoError(err)
		s.Equal(200, resp.StatusCode)
	})
	s.Run("delete stream", func() {
		resp, err := client.DeleteStream("cimeStream")
		s.NoError(err)
		s.Equal(200, resp.StatusCode)
	})
}

func (s *CIMETestSuite) TestHenan() {
	/// Prepare periodic rule
	// Create conf
	s.Run("creating conf", func() {
		conf := map[string]any{
			"fileType":         "cime",
			"path":             filepath.Join(EKPWD, "data", "cime"),
			"delimiter":        "\t",
			"ignoreStartLines": 2,
			"ignoreEndLines":   1,
			"offset":           "1m",
			"columns":          []string{"id", "code", "offset", "value"},
		}
		resp, err := client.CreateConf("sources/file/confKeys/cime", conf)
		s.Require().NoError(err)
		s.Require().Equal(http.StatusOK, resp.StatusCode)
	})
	// Create stream
	s.Run("creating stream", func() {
		streamJson := `{"sql": "CREATE STREAM cimeStream() WITH (TYPE=\"file\",FORMAT=\"delimited\",DELIMITER=\"\t\",DATASOURCE=\"hn.rb\",CONF_KEY=\"cime\",SHARED=\"true\");"}`
		resp, err := client.CreateStream(streamJson)
		s.Require().NoError(err)
		s.Require().Equal(201, resp.StatusCode)
	})
	// Subscribe to result topic
	var results []string
	s.Run("subscribing to result", func() {
		s.mqttClient.Subscribe("result/ruleCimE", 2, func(c mqtt.Client, message mqtt.Message) {
			if len(results) >= 3 {
				s.mqttClient.Unsubscribe("result/ruleCimE")
			} else {
				results = append(results, string(message.Payload()))
			}

		})
	})
	// Create rule
	s.Run("creating rule", func() {
		ruleStr := `
			{
			  "id": "ruleCimE",
			  "name": "Read CIM E file and replay with ts",
			  "sql": "SELECT * FROM cimeStream",
			  "actions": [
				{
				  "mqtt": {
					"server": "tcp://127.0.0.1:1883",
					"topic": "result/ruleCimE",
					"sendSingle": true
				  }
				}
			  ]
			}
			`
		resp, err := client.CreateRule(ruleStr)
		s.Require().NoError(err)
		s.Require().Equal(201, resp.StatusCode)
	})
	/// assert fetch rule
	s.Run("check fetch rule status", func() {
		try := 10
		for i := 0; i < try; i++ {
			time.Sleep(100 * time.Millisecond)
			metrics, err := client.GetRulStatus("ruleCimE")
			s.NoError(err)
			if metrics["status"] == "stopped" {
				break
			}
		}
		metrics, err := client.GetRulStatus("ruleCimE")
		s.NoError(err)
		s.T().Log(metrics)
		s.Require().Equal("stopped", metrics["status"])
		s.Require().Equal(3.0, metrics["source_cimeStream_0_records_out_total"])
		s.Require().Equal(3.0, metrics["sink_mqtt_0_0_records_out_total"])

	})
	// Check result
	s.Run("check result", func() {
		exp := []string{
			"{\"code\":\"HNP\",\"id\":\"#1\",\"offset\":\"1\",\"ts\":1361984400000,\"value\":\"14.82\"}",
			"{\"code\":\"HNP\",\"id\":\"#2\",\"offset\":\"2\",\"ts\":1361984460000,\"value\":\"14.82\"}",
			"{\"code\":\"HNP\",\"id\":\"#3\",\"offset\":\"3\",\"ts\":1361984520000,\"value\":\"14.82\"}",
		}
		s.Equal(exp, results)
	})
	/// clean up
	s.Run("delete rule", func() {
		resp, err := client.DeleteRule("ruleCimE")
		s.NoError(err)
		s.Equal(200, resp.StatusCode)
	})
	s.Run("delete stream", func() {
		resp, err := client.DeleteStream("cimeStream")
		s.NoError(err)
		s.Equal(200, resp.StatusCode)
	})
}

func (s *CIMETestSuite) TestHubei() {
	/// Prepare periodic rule
	// Create conf
	s.Run("creating conf", func() {
		conf := map[string]any{
			"fileType":         "cime",
			"path":             filepath.Join(EKPWD, "data", "cime"),
			"delimiter":        "\t",
			"ignoreStartLines": 10,
			"ignoreEndLines":   1,
			"columns":          []string{"id", "power"},
		}
		resp, err := client.CreateConf("sources/file/confKeys/cime", conf)
		s.Require().NoError(err)
		s.Require().Equal(http.StatusOK, resp.StatusCode)
	})
	// Create stream
	s.Run("creating stream", func() {
		streamJson := `{"sql": "CREATE STREAM cimeStream() WITH (TYPE=\"file\",FORMAT=\"delimited\",DELIMITER=\"\t\",DATASOURCE=\"hb.PVD\",CONF_KEY=\"cime\",SHARED=\"true\");"}`
		resp, err := client.CreateStream(streamJson)
		s.Require().NoError(err)
		s.Require().Equal(201, resp.StatusCode)
	})
	// Subscribe to result topic
	var results []string
	s.Run("subscribing to result", func() {
		s.mqttClient.Subscribe("result/ruleCimE", 2, func(c mqtt.Client, message mqtt.Message) {
			if len(results) >= 3 {
				s.mqttClient.Unsubscribe("result/ruleCimE")
			} else {
				results = append(results, string(message.Payload()))
			}

		})
	})
	// Create rule
	s.Run("creating rule", func() {
		ruleStr := `
			{
			  "id": "ruleCimE",
			  "name": "Read CIM E file and replay with ts",
			  "sql": "SELECT * FROM cimeStream",
			  "actions": [
				{
				  "mqtt": {
					"server": "tcp://127.0.0.1:1883",
					"topic": "result/ruleCimE",
					"sendSingle": true
				  }
				}
			  ]
			}
			`
		resp, err := client.CreateRule(ruleStr)
		s.Require().NoError(err)
		s.Require().Equal(201, resp.StatusCode)
	})
	/// assert fetch rule
	s.Run("check fetch rule status", func() {
		try := 10
		for i := 0; i < try; i++ {
			time.Sleep(100 * time.Millisecond)
			metrics, err := client.GetRulStatus("ruleCimE")
			s.NoError(err)
			if metrics["status"] == "stopped" {
				break
			}
		}
		metrics, err := client.GetRulStatus("ruleCimE")
		s.NoError(err)
		s.T().Log(metrics)
		s.Require().Equal("stopped", metrics["status"])
		s.Require().Equal(3.0, metrics["source_cimeStream_0_records_out_total"])
		s.Require().Equal(3.0, metrics["sink_mqtt_0_0_records_out_total"])

	})
	// Check result
	s.Run("check result", func() {
		exp := []string{
			"{\"id\":\"#1\",\"power\":\"162.24\",\"ts\":1702685700000}",
			"{\"id\":\"#2\",\"power\":\"150.86\",\"ts\":1702686600000}",
			"{\"id\":\"#3\",\"power\":\"140.30\",\"ts\":1702687500000}",
		}
		s.Equal(exp, results)
	})
	/// clean up
	s.Run("delete rule", func() {
		resp, err := client.DeleteRule("ruleCimE")
		s.NoError(err)
		s.Equal(200, resp.StatusCode)
	})
	s.Run("delete stream", func() {
		resp, err := client.DeleteStream("cimeStream")
		s.NoError(err)
		s.Equal(200, resp.StatusCode)
	})
}

// copyDir copies files from source to destination directory.
func copyDir(source string, dest string) error {
	// Create destination directory if it doesn't exist
	err := os.MkdirAll(dest, os.ModePerm)
	if err != nil {
		return err
	}

	// Read the contents of the source directory
	entries, err := os.ReadDir(source)
	if err != nil {
		return err
	}

	// Iterate over each entry in the source directory
	for _, entry := range entries {
		if entry.Type().IsRegular() { // Check if it's a regular file
			sourceFile := filepath.Join(source, entry.Name())
			destFile := filepath.Join(dest, entry.Name())

			// Copy the file
			err := copyFile(sourceFile, destFile)
			if err != nil {
				return err
			}
			fmt.Printf("Copied %s to %s\n", entry.Name(), destFile)
		}
	}
	return nil
}

// copyFile copies a single file from source to destination.
func copyFile(sourceFile string, destFile string) error {
	// Open the source file
	src, err := os.Open(sourceFile)
	if err != nil {
		return err
	}
	defer src.Close()

	// Create the destination file
	dst, err := os.Create(destFile)
	if err != nil {
		return err
	}
	defer dst.Close()

	// Copy the contents from source to destination
	_, err = io.Copy(dst, src)
	return err
}
