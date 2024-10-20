// Copyright 2021-2023 EMQ Technologies Co., Ltd.
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

package test

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"testing"
	"time"

	"github.com/lf-edge/ekuiper/v2/internal/binder"
	"github.com/lf-edge/ekuiper/v2/internal/binder/function"
	"github.com/lf-edge/ekuiper/v2/internal/binder/io"
	"github.com/lf-edge/ekuiper/v2/internal/io/memory/pubsub"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/plugin/portable"
	"github.com/lf-edge/ekuiper/v2/internal/plugin/portable/runtime"
	"github.com/lf-edge/ekuiper/v2/internal/processor"
	"github.com/lf-edge/ekuiper/v2/internal/topo"
	"github.com/lf-edge/ekuiper/v2/internal/topo/planner"
	"github.com/lf-edge/ekuiper/v2/internal/topo/topotest"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
)

func init() {
	m, err := portable.InitManager()
	if err != nil {
		panic(err)
	}
	entry := binder.FactoryEntry{Name: "portable plugin", Factory: m}
	err = function.Initialize([]binder.FactoryEntry{entry})
	if err != nil {
		panic(err)
	}
	err = io.Initialize([]binder.FactoryEntry{entry})
	if err != nil {
		panic(err)
	}
}

func TestSourceAndFunc(t *testing.T) {
	t.Skip()
	streamList := []string{"ext", "extpy"}
	topotest.HandleStream(false, streamList, t)
	tests := []struct {
		Name string
		Rule string
		R    [][]map[string]interface{}
		M    map[string]interface{}
	}{
		{
			Name: `TestPortableRule1`,
			Rule: `{"sql":"SELECT count as ee FROM ext","actions":[{"memory":{"topic":"cache"}}]}`,
			R: [][]map[string]interface{}{
				{{
					"ee": int64(50),
				}},
				{{
					"ee": int64(50),
				}},
				{{
					"ee": int64(50),
				}},
			},
			M: map[string]interface{}{
				"sink_memory_0_0_records_out_total": int64(3),
			},
		}, {
			Name: `TestPythonRule`,
			Rule: `{"sql":"SELECT revert(name) as ee FROM extpy","actions":[{"memory":{"topic":"cache"}},{"print":{}}]}`,
			R: [][]map[string]interface{}{
				{{
					"ee": "nosjyp",
				}},
				{{
					"ee": "nosjyp",
				}},
				{{
					"ee": "nosjyp",
				}},
			},
			M: map[string]interface{}{
				"sink_memory_0_0_records_out_total": int64(3),
			},
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	defer func() {
		time.Sleep(3 * time.Second)
		runtime.GetPluginInsManager().KillAll()
	}()
	for i, tt := range tests {
		topotest.HandleStream(true, streamList[i:i+1], t)
		rs, err := CreateRule(tt.Name, tt.Rule)
		if err != nil {
			t.Errorf("failed to create rule: %s.", err)
			continue
		}
		tp, err := planner.Plan(rs)
		if err != nil {
			t.Errorf("fail to init rule: %v", err)
			continue
		}
		var mm [][]map[string]interface{}
		ch := pubsub.CreateSub("cache", nil, fmt.Sprintf("cache%d", i+1), 10)
		ctx, cancel := context.WithTimeout(context.Background(), 10000*time.Second)
		go func(ctx context.Context) {
			select {
			case err := <-tp.Open():
				log.Println(err)
				tp.Cancel()
			case <-ctx.Done():
				log.Printf("ctx done %v\n", ctx.Err())
				tp.Cancel()
			}
			fmt.Println("all exit")
		}(ctx)
		go func(ctx context.Context) {
			for {
				select {
				case s := <-ch:
					log.Printf("get %v", s)
					tuple, ok := s.(*xsql.Tuple)
					if ok {
						mm = append(mm, []map[string]interface{}{tuple.Message})
					}
				case <-ctx.Done():
					log.Printf("ctx done %v\n", ctx.Err())
					return
				}
			}
		}(ctx)
		topotest.HandleStream(false, streamList[i:i+1], t)
		for {
			if ctx.Err() != nil {
				t.Errorf("Exiting with error %v", ctx.Err())
				break
			}
			time.Sleep(10 * time.Millisecond)
			if compareMetrics(tp, tt.M) {
				cancel()
				if !reflect.DeepEqual(tt.R, mm) {
					t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.Rule, tt.R, mm)
				}
				break
			}
		}
		cancel()
	}
}

func compareMetrics(tp *topo.Topo, m map[string]interface{}) bool {
	keys, values := tp.GetMetrics()
	for k, v := range m {
		var (
			index   int
			key     string
			matched bool
		)
		for index, key = range keys {
			if k == key {
				va, ok := values[index].(int64)
				if !ok {
					continue
				}
				ve := v.(int64)
				if va < ve {
					return false
				}
				matched = true
			}
		}
		if !matched {
			return false
		}
	}
	return true
}

func CreateRule(name, sql string) (*def.Rule, error) {
	p := processor.NewRuleProcessor()
	p.ExecDrop(name)
	return p.ExecCreateWithValidation(name, sql)
}
