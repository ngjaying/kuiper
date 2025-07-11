package fvt

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

type SqlTestSuite struct {
	suite.Suite
}

func TestSqlTestSuite(t *testing.T) {
	suite.Run(t, new(SqlTestSuite))
}

func (s *SqlTestSuite) TestReadWrite() {
	s.Run("creating connection", func() {
		// EKPWD = "C:/repo/go/ekbuild"
		// Copy
		srcName := filepath.Join(PWD, "test", "fvt", "data", "sql", "assetInfoDB.db")
		src, err := os.Open(srcName)
		s.Require().NoError(err)
		defer src.Close()

		dstName := filepath.Join(EKPWD, "data", "uploads", "assetInfoDB.db")
		dst, err := os.Create(dstName)
		s.Require().NoError(err)
		defer dst.Close()
		_, err = io.Copy(dst, src)
		s.NoError(err)

		resp, err := client.Post("connections", fmt.Sprintf(`{
				"id": "sqlc",
				"typ": "sql",
				"props": {
					"dburl": "sqlite3://@%s/data/uploads/assetInfoDB.db"
				}
			}`, path.Clean(EKPWD)))
		s.Require().NoError(err)
		s.Require().Equal(http.StatusCreated, resp.StatusCode)
		time.Sleep(100 * time.Millisecond)
		resp.Body.Close()
		resp, err = client.Get("connections/sqlc")
		s.Require().NoError(err)
		s.Require().Equal(http.StatusOK, resp.StatusCode)
		defer resp.Body.Close()
		fmt.Println(resp.Body)
		body, err := io.ReadAll(resp.Body)
		s.Require().NoError(err)
		result := make(map[string]any)
		err = json.Unmarshal(body, &result)
		s.Require().NoError(err)
		s.Require().Equal("connected", result["status"])
	})
	s.Run("creating read rule", func() {
		conf := map[string]any{
			"connectionSelector": "sqlc",
			"interval":           "25ms",
			"sourceType":         "stream",
			"templateSqlQueryCfg": map[string]any{
				"TemplateSql": "SELECT * from assetInfo",
			},
		}
		resp, err := client.CreateConf("sources/sql/confKeys/sqLiteConfig", conf)
		s.Require().NoError(err)
		s.Require().Equal(http.StatusOK, resp.StatusCode)

		streamSql := `{"sql":"CREATE STREAM sqLiteStream() WITH (TYPE=\"sql\", CONF_KEY=\"sqLiteConfig\", FORMAT=\"json\")"}`
		resp, err = client.CreateStream(streamSql)
		s.Require().NoError(err)
		s.Require().Equal(http.StatusCreated, resp.StatusCode)

		ruleStr, err := os.ReadFile(filepath.Join(PWD, RulesPath, "readsql.json"))
		s.Require().NoError(err)
		resp, err = client.CreateRule(string(ruleStr))
		s.Require().NoError(err)
		s.Require().Equal(201, resp.StatusCode)
	})
	time.Sleep(10 * time.Millisecond)
	s.Run("read no data before", func() {
		metrics, err := client.GetRuleStatus("read")
		s.NoError(err)
		s.T().Log(metrics)
		s.Require().Equal("running", metrics["status"])
		s.Require().Equal(0.0, metrics["source_sqLiteStream_0_records_in_total"])
	})
	s.Run("creating write rule", func() {
		conf := map[string]any{
			"interval": "2ms",
			"loop":     false,
			"data": []map[string]any{
				{
					"humidity": 20,
				},
				{
					"uId":      1,
					"humidity": 21,
				},
				{
					"uId":      2,
					"humidity": 22,
				},
				{
					"temperature": 30,
				},
				{
					"uId":      2,
					"action":   "update",
					"humidity": 42,
				},
			},
		}
		resp, err := client.CreateConf("sources/simulator/confKeys/test", conf)
		s.Require().NoError(err)
		s.Require().Equal(http.StatusOK, resp.StatusCode)

		streamSql := `{"sql":"CREATE STREAM sim() WITH (TYPE=\"simulator\", CONF_KEY=\"test\", FORMAT=\"json\")"}`
		resp, err = client.CreateStream(streamSql)
		s.Require().NoError(err)
		s.Require().Equal(http.StatusCreated, resp.StatusCode)

		ruleStr, err := os.ReadFile(filepath.Join(PWD, RulesPath, "writesql.json"))
		s.Require().NoError(err)
		resp, err = client.CreateRule(string(ruleStr))
		s.Require().NoError(err)
		s.Require().Equal(201, resp.StatusCode)
	})
	s.Run("creating qos rule", func() {
		conf := map[string]any{
			"connectionSelector": "sqlc",
			"interval":           "25ms",
			"sourceType":         "stream",
			"indexFields": []map[string]any{
				{
					"indexField":     "uId",
					"indexFieldType": "string",
					"indexValue":     "0",
				},
			},
			"templateSqlQueryCfg": map[string]any{
				"TemplateSql": "SELECT * FROM assetInfo WHERE uId > '{{.uId}}' ORDER BY uId ASC LIMIT 1",
				"indexFields": []map[string]any{
					{
						"indexField":     "uId",
						"indexFieldType": "string",
						"indexValue":     "0",
					},
				},
			},
		}
		resp, err := client.CreateConf("sources/sql/confKeys/index_sqlconfigkey", conf)
		s.Require().NoError(err)
		s.Require().Equal(http.StatusOK, resp.StatusCode)

		streamSql := `{"sql":"CREATE STREAM index_sqlcdcstream() WITH (TYPE=\"sql\", CONF_KEY=\"index_sqlconfigkey\", FORMAT=\"json\")"}`
		resp, err = client.CreateStream(streamSql)
		s.Require().NoError(err)
		s.Require().Equal(http.StatusCreated, resp.StatusCode)

		ruleStr, err := os.ReadFile(filepath.Join(PWD, RulesPath, "readsqlqos.json"))
		s.Require().NoError(err)
		resp, err = client.CreateRule(string(ruleStr))
		s.Require().NoError(err)
		s.Require().Equal(201, resp.StatusCode)
	})
	s.Run("check fetch rule status", func() {
		try := 10
		for i := 0; i < try; i++ {
			time.Sleep(100 * time.Millisecond)
			metrics, err := client.GetRuleStatus("write")
			s.NoError(err)
			if metrics["status"] == "stopped" {
				break
			}
		}
		metrics, err := client.GetRuleStatus("write")
		s.NoError(err)
		s.T().Log(metrics)
		s.Require().Equal("stopped", metrics["status"])
		s.Require().Equal(5.0, metrics["source_sim_0_records_in_total"])
		s.Require().Equal(5.0, metrics["sink_sql_0_0_records_out_total"])

		time.Sleep(100 * time.Millisecond)
		try = 10
		for i := 0; i < try; i++ {
			time.Sleep(100 * time.Millisecond)
			metrics, err = client.GetRuleStatus("read")
			s.NoError(err)
			if metrics["source_sqLiteStream_0_records_in_total"].(float64) > 3.0 {
				break
			}
		}
		metrics, err = client.GetRuleStatus("read")
		s.NoError(err)
		s.T().Log(metrics)
		s.Require().Equal("running", metrics["status"])
		s.True(metrics["source_sqLiteStream_0_records_in_total"].(float64) > 3.0)

		_, err = client.StopRule("read")
		s.NoError(err)

		metrics, err = client.GetRuleStatus("readq")
		s.NoError(err)
		s.T().Log(metrics)
		s.Require().Equal("running", metrics["status"])
		s.Require().Equal(2.0, metrics["source_index_sqlcdcstream_0_records_in_total"])

		_, err = client.StopRule("readq")
		s.Require().NoError(err)
	})
	s.Run("check checkpoint result", func() {
		time.Sleep(100 * time.Millisecond)
		_, err := client.RestartRule("readq")
		s.Require().NoError(err)
		time.Sleep(100 * time.Millisecond)
		metrics, err := client.GetRuleStatus("readq")
		s.NoError(err)
		s.T().Log(metrics)
		s.Require().Equal("running", metrics["status"])
		s.Require().Equal(0.0, metrics["source_index_sqlcdcstream_0_records_in_total"])
	})
	s.Run("Clean", func() {
		resp, err := client.DeleteRule("readq")
		s.NoError(err)
		s.Equal(200, resp.StatusCode)
		resp, err = client.DeleteRule("read")
		s.NoError(err)
		s.Equal(200, resp.StatusCode)
		resp, err = client.DeleteRule("write")
		s.NoError(err)
		s.Equal(200, resp.StatusCode)
		resp, err = client.DeleteStream("index_sqlcdcstream")
		s.NoError(err)
		s.Equal(200, resp.StatusCode)
		resp, err = client.DeleteStream("sqLiteStream")
		s.NoError(err)
		s.Equal(200, resp.StatusCode)
		resp, err = client.Delete("connections/sqlc")
		s.NoError(err)
		s.Equal(200, resp.StatusCode)
		dstName := filepath.Join(EKPWD, "data", "uploads", "assetInfoDB.db")
		os.Remove(dstName)
	})
}
