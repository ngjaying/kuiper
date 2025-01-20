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

package batcher

import (
	"fmt"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

func TestColumnar(t *testing.T) {
	tests := []struct {
		name   string
		inputs []any
		output any
	}{
		{
			name: "normal",
			inputs: []any{
				&xsql.Tuple{
					Message: map[string]any{
						"ts":   int64(1234567),
						"sig1": 34.2,
						"sig2": 44,
					},
				},
				&xsql.Tuple{
					Message: map[string]any{
						"ts":   int64(1234667),
						"sig1": 34.5,
						"sig2": 45,
					},
				},
				&xsql.Tuple{
					Message: map[string]any{
						"ts":   int64(1234767),
						"sig1": 34.6,
						"sig2": 46,
					},
				},
			},
			output: &xsql.Tuple{
				Timestamp: timex.GetNow(),
				Message: map[string]any{
					"ts":   []any{int64(1234567), int64(1234667), int64(1234767)},
					"sig1": []any{34.2, 34.5, 34.6},
					"sig2": []any{44, 45, 46},
				},
			},
		},
	}
	columnar := NewColumnar(map[string]*ast.JsonStreamField{
		"ts":   nil,
		"sig1": nil,
		"sig2": nil,
	}, 10)
	ctx := mockContext.NewMockContext("colRule", "op1")
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for _, input := range test.inputs {
				switch it := input.(type) {
				case xsql.Collection:
					_ = it.Range(func(i int, r xsql.ReadonlyRow) (bool, error) {
						x := r.(xsql.Row)
						columnar.Ingest(ctx, x)
						return true, nil
					})
				case xsql.Row:
					columnar.Ingest(ctx, it)
				}
			}
			result := columnar.Flush(ctx)
			assert.Equal(t, test.output, result)
		})
	}
}

func TestMem(t *testing.T) {
	var memStats runtime.MemStats
	//runtime.ReadMemStats(&memStats)
	//fmt.Printf("Before: Alloc = %v MiB\n", memStats.Alloc/1024/1024)
	//buffer := make([][]any, 9284)
	//for j := 0; j < 9284; j++ {
	//	buffer[j] = make([]any, 0, 300)
	//}
	//runtime.ReadMemStats(&memStats)
	//fmt.Printf("After: Alloc = %v MiB\n", memStats.Alloc/1024/1024)

	runtime.ReadMemStats(&memStats)
	fmt.Printf("Before: Alloc = %v MiB\n", memStats.Alloc/1024/1024)
	cols := make([]strings.Builder, 9082)
	//for j := 0; j < 9082; j++ {
	//	cols[j] = strings.Builder{}
	//}
	runtime.ReadMemStats(&memStats)
	fmt.Printf("After: Alloc = %v MiB\n", memStats.Alloc/1024/1024)
	fmt.Println(len(cols))
}
