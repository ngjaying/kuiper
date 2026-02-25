package fvt

import (
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type GBFV2TestSuite struct {
	suite.Suite
	client MQTT.Client
}

func (s *GBFV2TestSuite) SetupSuite() {
	opts := MQTT.NewClientOptions().AddBroker("tcp://127.0.0.1:1883")
	s.client = MQTT.NewClient(opts)
	if token := s.client.Connect(); token.Wait() && token.Error() != nil {
		s.T().Fatalf("Failed to connect to MQTT broker: %v", token.Error())
	}
}

func (s *GBFV2TestSuite) TearDownSuite() {
	if s.client != nil && s.client.IsConnected() {
		s.client.Disconnect(250)
	}
}

func (s *GBFV2TestSuite) SetupTest() {
	// Clean up any existing schemas or streams created from tests
	client.DeleteStream("gbftest")
	client.DeleteRule("rule_gbf_e2e")
	client.Delete("schemas/gbf/unified_v1")
	client.Delete("schemas/abi/test_abi")
}

func (s *GBFV2TestSuite) TearDownTest() {
	client.DeleteStream("gbftest")
	client.DeleteRule("rule_gbf_e2e")
	client.Delete("schemas/gbf/unified_v1")
	client.Delete("schemas/abi/test_abi")
}

func (s *GBFV2TestSuite) TestGBFV2() {
	var result []string
	var resultMutex sync.Mutex

	s.Run("creating schema", func() {
		// 1. Register ABI underlying schema and mapping
		abiContent, err := os.ReadFile("data/gbf_v2/unified_v1.abi.json")
		require.NoError(s.T(), err)

		gbfContent, err := os.ReadFile("data/gbf_v2/unified_v1.gbf.json")
		require.NoError(s.T(), err)

		abiContentStr, _ := json.Marshal(string(abiContent))
		_, err = client.Post("schemas/abi", `{"name":"test_abi","content":`+string(abiContentStr)+`}`)
		require.NoError(s.T(), err)

		var gbfMap map[string]interface{}
		err = json.Unmarshal(gbfContent, &gbfMap)
		require.NoError(s.T(), err)

		abiSchemaPath := "/tmp/unified_v1.abi.json"
		abiMappingPath := "/tmp/msg_id_map.json"

		err = os.WriteFile(abiSchemaPath, abiContent, 0644)
		require.NoError(s.T(), err)

		abiMapContent, err := os.ReadFile("data/gbf_v2/msg_id_map.json")
		require.NoError(s.T(), err)
		err = os.WriteFile(abiMappingPath, abiMapContent, 0644)
		require.NoError(s.T(), err)

		types := gbfMap["types"].(map[string]interface{})
		payloadConfig := types["PayloadConfig"].(map[string]interface{})
		config2 := payloadConfig["2"].(map[string]interface{})
		config2["schema_id"] = abiSchemaPath
		extProps := config2["ext_props"].(map[string]interface{})
		extProps["mapping_path"] = abiMappingPath

		gbfContentModified, _ := json.Marshal(gbfMap)
		gbfContentStr, _ := json.Marshal(string(gbfContentModified))
		_, err = client.Post("schemas/gbf", `{"name":"unified_v1","content":`+string(gbfContentStr)+`}`)
		require.NoError(s.T(), err)
	})

	s.Run("creating stream", func() {
		streamJson := `{"sql":"CREATE STREAM gbftest() WITH (DATASOURCE=\"gbftest\", FORMAT=\"gbf\", SCHEMAID=\"unified_v1\");"}`
		resp, err := client.CreateStream(streamJson)
		s.Require().NoError(err)
		s.Require().Equal(201, resp.StatusCode)
	})

	s.Run("create rule", func() {
		ruleStr := `{
			"id": "rule_gbf_e2e",
			"sql": "SELECT * FROM gbftest",
			"actions": [
				{
					"mqtt": {
						"server": "tcp://127.0.0.1:1883",
						"topic": "ek/gbf_result",
						"sendSingle": true
					}
				}
			]
		}`
		resp, err := client.CreateRule(ruleStr)
		s.Require().NoError(err)
		if resp.StatusCode != 201 {
			body, _ := io.ReadAll(resp.Body)
			s.T().Logf("Failed to create rule %s", string(body))
		}
		s.Require().Equal(201, resp.StatusCode)

		s.client.Subscribe("ek/gbf_result", 2, func(c MQTT.Client, message MQTT.Message) {
			resultMutex.Lock()
			result = append(result, string(message.Payload()))
			resultMutex.Unlock()
		})
	})

	time.Sleep(ConstantInterval)

	s.Run("check status before feeding data", func() {
		metrics, err := client.GetRuleStatus("rule_gbf_e2e")
		s.Require().NoError(err)
		s.Require().Equal("running", metrics["status"])
		s.Require().Equal(0.0, metrics["source_gbftest_0_records_in_total"])
	})

	s.Run("publish data", func() {
		lines, err := os.ReadFile("data/gbf_v2/123.lines")
		s.Require().NoError(err)

		for _, line := range strings.Split(string(lines), "\n") {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			b := decodeHex(line)
			if token := s.client.Publish("gbftest", 1, false, b); token.Wait() && token.Error() != nil {
				s.T().Fatalf("Failed to publish to MQTT: %v", token.Error())
			}
			time.Sleep(10 * time.Millisecond)
		}
	})

	time.Sleep(ConstantInterval)

	s.Run("check status after feeding data", func() {
		metrics, err := client.GetRuleStatus("rule_gbf_e2e")
		s.Require().NoError(err)
		s.Require().Equal("running", metrics["status"])

		// Expecting 4 records: 3x EPS (msg_id 12345) + 1x Steer (msg_id 9999)
		s.Require().Equal(4.0, metrics["source_gbftest_0_records_in_total"])
		s.Require().Equal(4.0, metrics["sink_mqtt_0_0_records_out_total"])

		// Verify results received via MQTT
		resultMutex.Lock()
		resultsCopy := make([]string, len(result))
		copy(resultsCopy, result)
		resultMutex.Unlock()

		s.Require().Equal(4, len(resultsCopy))

		// result[0] → row 1: EPS msg_id=12345, TgtMotorMotorTorq=20683, MeasuredTorsionBarTorque=7889
		var epsResult map[string]interface{}
		err = json.Unmarshal([]byte(resultsCopy[0]), &epsResult)
		s.Require().NoError(err)
		s.T().Logf("EPS result: %s", resultsCopy[0])
		expectedEPS := map[string]interface{}{
			"TgtMotorMotorTorq":        float64(20683), // 0x50cb
			"MeasuredTorsionBarTorque": float64(7889),  // 0x1ed1
			"LostComFltSts1":           float64(0),
			"Reserved1":                float64(0),
		}
		s.Require().Equal(expectedEPS, epsResult)

		// result[3] → row 4: Steer msg_id=9999, SteerWheelAngle=-1234, SteerWheelAngleSpeed=5678
		var steerResult map[string]interface{}
		err = json.Unmarshal([]byte(resultsCopy[3]), &steerResult)
		s.Require().NoError(err)
		s.T().Logf("Steer result: %s", resultsCopy[3])
		expectedSteer := map[string]interface{}{
			"SteerWheelAngle":      float64(-1234),
			"SteerWheelAngleSpeed": float64(5678),
		}
		for k, v := range expectedSteer {
			s.Require().Equal(v, steerResult[k], "field %s", k)
		}
	})
}

func decodeHex(s string) []byte {
	b, err := hex.DecodeString(s)
	if err != nil {
		panic("invalid hex payload: " + err.Error())
	}
	return b
}

func TestGBFV2(t *testing.T) {
	suite.Run(t, new(GBFV2TestSuite))
}
