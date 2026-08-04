// Copyright 2025 EMQ Technologies Co., Ltd.
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

package planner

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/store"
	"github.com/lf-edge/ekuiper/v2/internal/topo/node"
	"github.com/lf-edge/ekuiper/v2/internal/topo/operator"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func findWindowPlan(p LogicalPlan) *WindowPlan {
	if w, ok := p.(*WindowPlan); ok {
		return w
	}
	for _, c := range p.Children() {
		if r := findWindowPlan(c); r != nil {
			return r
		}
	}
	return nil
}

func findFilterPlan(p LogicalPlan) *FilterPlan {
	if fp, ok := p.(*FilterPlan); ok {
		return fp
	}
	for _, c := range p.Children() {
		if r := findFilterPlan(c); r != nil {
			return r
		}
	}
	return nil
}

func setupVehicleStatusStream(t *testing.T) {
	kv, err := store.GetKV("stream")
	require.NoError(t, err)
	s, err := json.Marshal(&xsql.StreamInfo{
		StreamType: ast.TypeStream,
		Statement:  `CREATE STREAM vehicle_status (soc BIGINT, charge_status string) WITH (DATASOURCE="vehicle_status", FORMAT="json");`,
	})
	require.NoError(t, err)
	require.NoError(t, kv.Set("vehicle_status", string(s)))
}

// TestStateWindowWhereNotPushedDown verifies that a WHERE clause on a state
// window is NOT pushed below the window. The FilterPlan must sit above the
// WindowPlan (so the filter runs after the window emits), and the WindowPlan
// must not carry the WHERE as its own condition (which would create a
// windowFilter operator before the window).
func TestStateWindowWhereNotPushedDown(t *testing.T) {
	setupVehicleStatusStream(t)

	sql := `SELECT collect(*) AS charge_data FROM vehicle_status WHERE soc % 10 = 0 AND changed_col(true, soc % 10 = 0) GROUP BY statewindow(charge_status = 'charging', charge_status = 'discharging')`
	stmt, err := xsql.GetStatementFromSql(sql)
	require.NoError(t, err)

	kv, err := store.GetKV("stream")
	require.NoError(t, err)
	p, err := CreateLogicalPlan(stmt, &def.RuleOption{BufferLength: 1024}, kv)
	require.NoError(t, err)

	explain, err := ExplainFromLogicalPlan(p, "charge_monitor")
	require.NoError(t, err)
	fmt.Println("==== EXPLAIN ====\n" + explain)

	// Root is Project; its direct child must be the FilterPlan (WHERE after window).
	root := p
	require.IsType(t, &ProjectPlan{}, root)
	var filterAbove *FilterPlan
	for _, c := range root.Children() {
		if fp, ok := c.(*FilterPlan); ok {
			filterAbove = fp
		}
	}
	require.NotNil(t, filterAbove, "expected FilterPlan directly under Project (WHERE after window)")

	w := findWindowPlan(p)
	require.NotNil(t, w)
	require.Nil(t, w.condition, "state window should not carry the WHERE condition")

	// No FilterPlan may live below the window (the old windowFilter placement).
	var hasFilterBelow func(LogicalPlan) bool
	hasFilterBelow = func(n LogicalPlan) bool {
		if _, ok := n.(*FilterPlan); ok {
			return true
		}
		for _, c := range n.Children() {
			if hasFilterBelow(c) {
				return true
			}
		}
		return false
	}
	require.False(t, hasFilterBelow(w), "state window should not have a FilterPlan descendant")
}

// TestStateWindowWhereAfterRuntime verifies end-to-end that the WHERE clause
// is evaluated AFTER the state window emits.
//
// The begin/emit rows deliberately have soc % 10 != 0 so that, under the old
// WHERE-before-window behavior, the discharging row would be filtered out and
// the window would never emit.
//
//	t1 soc=5  charge_status=charging     -> begin window
//	t2 soc=10 charge_status=charging     -> collected
//	t3 soc=15 charge_status=discharging  -> emit window
//
// Expected (WHERE after window): the window emits [soc=5, soc=10, soc=15] and
// the filter keeps only the row with soc % 10 == 0, i.e. [soc=10].
func TestStateWindowWhereAfterRuntime(t *testing.T) {
	setupVehicleStatusStream(t)

	sql := `SELECT collect(*) AS charge_data FROM vehicle_status WHERE soc % 10 = 0 GROUP BY statewindow(charge_status = 'charging', charge_status = 'discharging')`
	stmt, err := xsql.GetStatementFromSql(sql)
	require.NoError(t, err)

	o := &def.RuleOption{BufferLength: 1024}
	kv, err := store.GetKV("stream")
	require.NoError(t, err)
	p, err := CreateLogicalPlan(stmt, o, kv)
	require.NoError(t, err)

	wp := findWindowPlan(p)
	require.NotNil(t, wp)
	fp := findFilterPlan(p)
	require.NotNil(t, fp)
	require.Nil(t, wp.condition)

	winOp, err := node.NewWindowV2Op("window", node.WindowConfig{
		Type:           wp.WindowType(),
		BeginCondition: wp.GetBeginCondition(),
		EmitCondition:  wp.GetEmitCondition(),
	}, o)
	require.NoError(t, err)

	fp.ExtractStateFunc()
	filterOp := Transform(&operator.FilterOp{
		Condition:  fp.condition,
		StateFuncs: fp.stateFuncs,
	}, "filter", o)

	filterInput, _ := filterOp.GetInput()
	require.NoError(t, winOp.AddOutput(filterInput, "to_filter"))
	output := make(chan any, 10)
	require.NoError(t, filterOp.AddOutput(output, "output"))

	ctx, cancel := mockContext.NewMockContext("1", "2").WithCancel()
	errCh := make(chan error, 10)
	winOp.Exec(ctx, errCh)
	filterOp.Exec(ctx, errCh)
	time.Sleep(50 * time.Millisecond)

	now := time.Now()
	winIn, _ := winOp.GetInput()
	winIn <- &xsql.Tuple{Message: map[string]any{"soc": int64(5), "charge_status": "charging"}, Timestamp: now}
	winIn <- &xsql.Tuple{Message: map[string]any{"soc": int64(10), "charge_status": "charging"}, Timestamp: now.Add(1 * time.Second)}
	winIn <- &xsql.Tuple{Message: map[string]any{"soc": int64(15), "charge_status": "discharging"}, Timestamp: now.Add(2 * time.Second)}
	time.Sleep(100 * time.Millisecond)

	select {
	case got := <-output:
		wt, ok := got.(*xsql.WindowTuples)
		require.True(t, ok, "expected *xsql.WindowTuples, got %T", got)
		maps := wt.ToMaps()
		require.Equal(t, []map[string]any{
			{"soc": int64(10), "charge_status": "charging"},
		}, maps)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for window output")
	}

	cancel()
	time.Sleep(50 * time.Millisecond)
	winOp.Close()
	filterOp.Close()
}
