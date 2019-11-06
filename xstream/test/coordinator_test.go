package test

import (
	"bytes"
	"encoding/gob"
	"engine/common"
	"engine/xsql"
	"engine/xsql/processors"
	"engine/xstream"
	"engine/xstream/connectors"
	"engine/xstream/sources"
	"fmt"
	"path"
	"reflect"
	"strings"
	"testing"
	"time"
)

var BadgerDir string
var log = common.Log
func init(){
	dataDir, err := common.GetDataLoc()
	if err != nil {
		log.Panic(err)
	}else{
		log.Infof("db location is %s", dataDir)
	}
	BadgerDir = path.Join(path.Dir(dataDir), "checkpointTest")
	log.Infof("badge location is %s", BadgerDir)
}

func createStreams(t *testing.T){
	demo := `CREATE STREAM demo (
					color STRING,
					size BIGINT,
					ts BIGINT
				) WITH (DATASOURCE="demo", FORMAT="json", KEY="ts");`
	_, err := processors.NewStreamProcessor(demo, path.Join(BadgerDir, "stream")).Exec()
	if err != nil{
		t.Log(err)
	}
}

func dropStreams(t *testing.T){
	demo := `DROP STREAM demo`
	_, err := processors.NewStreamProcessor(demo, path.Join(BadgerDir, "stream")).Exec()
	if err != nil{
		t.Log(err)
	}
}

func getMockSource(name string, done chan<- struct{}, size int) xstream.Source {
	var data []*xsql.Tuple
	switch name{
	case "demo":
		data = []*xsql.Tuple{
			{
				Emitter: name,
				Message: map[string]interface{}{
					"color": "red",
					"size": 3,
					"ts": 1541152486013,
				},
				Timestamp: 1541152486013,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"color": "blue",
					"size": 6,
					"ts": 1541152486822,
				},
				Timestamp: 1541152686822,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"color": "blue",
					"size": 2,
					"ts": 1541152487632,
				},
				Timestamp: 1541152887632,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"color": "yellow",
					"size": 4,
					"ts": 1541152488442,
				},
				Timestamp: 1541153088442,
			},
			{
				Emitter: name,
				Message: map[string]interface{}{
					"color": "red",
					"size": 1,
					"ts": 1541152489252,
				},
				Timestamp: 1541153289252,
			},
		}
	}
	return sources.NewSourceWrapper(name, connectors.NewMemoryInputConnector(data[:size], done, false, 5000))
}

func getCheckpoint(checkpointId int64, ruleId string) map[string]interface{}{
	dir, err := common.GetAndCreateDataLoc("checkpoint/" + ruleId)
	if err != nil{
		panic(err)
	}
	db, err := common.DbOpen(dir)
	if err != nil {
		return nil
	}
	defer common.DbClose(db)
	bytes, err := common.DbGet(db, string(checkpointId))
	if err != nil {
		return nil
	}
	m, ok := bytesToMap(bytes)
	if !ok {
		return nil
	}
	return m
}

func bytesToMap(input []byte) (map[string]interface{}, bool){
	var result map[string]interface{}
	buf := bytes.NewBuffer(input)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&result); err != nil {
		return nil, false
	}
	return result, true
}

func TestCheckpointCount(t *testing.T) {
	common.IsTesting = true
	var tests = []struct {
		name    string
		sql 	string
		r       int
		cp      map[string]interface{}
	}{
		{
			name: `rule1`,
			sql: `SELECT * FROM demo`,
			r: 2,
			cp: map[string]interface{}{
				"offset": 3,
			},
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	createStreams(t)
	defer dropStreams(t)
	done := make(chan struct{})
	defer close(done)
	for i, tt := range tests {
		p := processors.NewRuleProcessor(BadgerDir)
		parser := xsql.NewParser(strings.NewReader(tt.sql))
		var sources []xstream.Source
		if stmt, err := xsql.Language.Parse(parser); err != nil{
			t.Errorf("parse sql %s error: %s", tt.sql , err)
		}else {
			if selectStmt, ok := stmt.(*xsql.SelectStatement); !ok {
				t.Errorf("sql %s is not a select statement", tt.sql)
			} else {
				streams := xsql.GetStreams(selectStmt)
				for _, stream := range streams{
					source := getMockSource(stream, done, 5)
					sources = append(sources, source)
				}
			}
		}
		tp, inputs, err := p.CreateTopoWithSources(&xstream.Rule{
			Id:tt.name, Sql: tt.sql,Options: map[string]interface{}{
			"qos": 1,
		}}, sources)
		if err != nil{
			t.Error(err)
		}
		sink := NewMockSink("mockSink", tt.name)
		tp.AddSink(inputs, sink)
		count := len(sources)
		errCh := tp.Open()
		func() {
			for {
				select {
				case err = <-errCh:
					t.Log(err)
					tp.Cancel()
					return
				case <-done:
					count--
					log.Infof("%d sources remaining", count)
					if count <= 0 {
						log.Info("stream stopping")
						time.Sleep(1 * time.Second)
						tp.Cancel()
						return
					}
				default:
				}
			}
		}()

		actual := tp.GetCoordinator().GetCompleteCount()
		if !reflect.DeepEqual(tt.r, actual) {
			t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%d\n\n", i, tt.sql, tt.r, actual)
		}
		//Get checkpoint content
		cp := getCheckpoint(tp.GetCoordinator().GetLatest(), tt.name)
		if !reflect.DeepEqual(tt.cp, cp["demo"]) {
			t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%v\n\n", i, tt.sql, tt.cp, cp)
		}
	}
}