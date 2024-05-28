// Copyright 2021-2024 EMQ Technologies Co., Ltd.
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

package node

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

func TestJSON(t *testing.T) {
	tests := []struct {
		name        string
		concurrency int
	}{
		{"single", 1},
		{"multi", 10},
	}
	ctx := mockContext.NewMockContext("test", "Test")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op, err := NewDecodeOp(ctx, false, "test", "streamName", "test1", &def.RuleOption{BufferLength: 10, SendError: true, Concurrency: tt.concurrency}, &ast.Options{FORMAT: "json"}, false, true, nil, map[string]any{
				"sendInterval": "10ms",
			})
			assert.NoError(t, err)
			out := make(chan any, 100)
			err = op.AddOutput(out, "test")
			assert.NoError(t, err)
			ctx := mockContext.NewMockContext("test1", "decode_test")
			errCh := make(chan error)
			op.Exec(ctx, errCh)

			cases := []any{
				&xsql.RawTuple{Emitter: "test", Rawdata: []byte("{\"a\":1,\"b\":2}"), Timestamp: time.UnixMilli(111), Metadata: map[string]any{"topic": "demo", "qos": 1}},
				&xsql.RawTuple{Emitter: "test", Rawdata: []byte("[{\"a\":1,\"b\":2},{\"a\":3,\"b\":4,\"c\":\"hello\"}]"), Timestamp: time.UnixMilli(111), Metadata: map[string]any{"topic": "demo", "qos": 1}},
				errors.New("go through error"),
				&xsql.RawTuple{Emitter: "test", Rawdata: []byte("\"a\":1,\"b\":2},{\"a\":3,\"b\":4,\"c\":\"hello\"}]"), Timestamp: time.UnixMilli(111), Metadata: map[string]any{"topic": "demo", "qos": 1}},
				&xsql.RawTuple{Emitter: "test", Rawdata: []byte("[\"hello\"]"), Timestamp: time.UnixMilli(111), Metadata: map[string]any{"topic": "demo", "qos": 1}},
				&xsql.RawTuple{Emitter: "test", Rawdata: []byte("\"hello\""), Timestamp: time.UnixMilli(111), Metadata: map[string]any{"topic": "demo", "qos": 1}},
				"invalid",
			}
			expects := [][]any{
				{&xsql.Tuple{Emitter: "test", Message: map[string]interface{}{"a": 1.0, "b": 2.0}, Timestamp: time.UnixMilli(111), Metadata: map[string]any{"topic": "demo", "qos": 1}}},
				{
					&xsql.Tuple{Emitter: "test", Message: map[string]interface{}{"a": 1.0, "b": 2.0}, Timestamp: time.UnixMilli(111), Metadata: map[string]any{"topic": "demo", "qos": 1}},
					&xsql.Tuple{Emitter: "test", Message: map[string]interface{}{"a": 3.0, "b": 4.0, "c": "hello"}, Timestamp: time.UnixMilli(111), Metadata: map[string]any{"topic": "demo", "qos": 1}},
				},
				{errors.New("go through error")},
				{errors.New("invalid character ':' after top-level value")},
				{errors.New("only map[string]any inside a list is supported but got: hello")},
				{errors.New("unsupported decode result: hello")},
				{errors.New("unsupported data received: invalid")},
			}
			timex.Add(2 * time.Second)
			for i, c := range cases {
				op.input <- c
				for _, e := range expects[i] {
					r := <-out
					switch tr := r.(type) {
					case error:
						require.Equal(t, e.(error).Error(), tr.Error())
					default:
						assert.Equal(t, e, r)
					}
				}
			}
		})
	}
}

// Concurrency 1 - BenchmarkThrougput-16                  1        1548680100 ns/op
// Concurrency 10 - BenchmarkThrougput-16           1000000000               0.1553 ns/op
// This is useful when a node is much slower
func BenchmarkThrougput(b *testing.B) {
	ctx := mockContext.NewMockContext("test1", "decode_test")
	op, err := NewDecodeOp(ctx, false, "test", "streamName", "test1", &def.RuleOption{BufferLength: 10, SendError: true, Concurrency: 10, Debug: true}, &ast.Options{FORMAT: "mock"}, false, true, nil, nil)
	assert.NoError(b, err)
	out := make(chan any, 100)
	err = op.AddOutput(out, "test")
	assert.NoError(b, err)
	errCh := make(chan error)
	op.Exec(ctx, errCh)
	go func() {
		for i := 0; i < 100; i++ {
			op.input <- &xsql.RawTuple{Emitter: "test", Rawdata: []byte("{\"a\":1,\"b\":2}"), Timestamp: time.UnixMilli(111), Metadata: map[string]any{"topic": "demo", "qos": 1}}
		}
	}()
	for i := 0; i < 100; i++ {
		_ = <-out
	}
}

func TestJSONWithSchema(t *testing.T) {
	tests := []struct {
		name        string
		concurrency int
		schema      map[string]*ast.JsonStreamField
	}{
		{"single", 1, map[string]*ast.JsonStreamField{
			"b": {
				Type: "float",
			},
		}},
		{"multi", 10, map[string]*ast.JsonStreamField{
			"b": {
				Type: "float",
			},
		}},
	}
	ctx := mockContext.NewMockContext("test1", "decode_test")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op, err := NewDecodeOp(ctx, false, "test", "streamName", "test1", &def.RuleOption{BufferLength: 10, SendError: true, Concurrency: tt.concurrency}, &ast.Options{FORMAT: "json", SHARED: true}, false, false, map[string]*ast.JsonStreamField{
				"a": {
					Type: "bigint",
				},
			}, map[string]any{
				"payloadField": "c", "payloadFormat": "json",
			})
			// payload field will add to schema automatically
			assert.NoError(t, err)
			out := make(chan any, 100)
			err = op.AddOutput(out, "test")
			assert.NoError(t, err)
			errCh := make(chan error)
			op.Exec(ctx, errCh)

			cases := []any{
				&xsql.RawTuple{Emitter: "test", Rawdata: []byte("{\"a\":1,\"b\":2}"), Timestamp: time.UnixMilli(111), Metadata: map[string]any{"topic": "demo", "qos": 1}},
				&xsql.RawTuple{Emitter: "test", Rawdata: []byte("[{\"a\":1,\"b\":2},{\"a\":3,\"b\":4,\"c\":\"hello\"}]"), Timestamp: time.UnixMilli(111), Metadata: map[string]any{"topic": "demo", "qos": 1}},
			}
			expects := [][]any{
				{&xsql.Tuple{Emitter: "test", Message: map[string]interface{}{"a": int64(1)}, Timestamp: time.UnixMilli(111), Metadata: map[string]any{"topic": "demo", "qos": 1}}},
				{
					&xsql.Tuple{Emitter: "test", Message: map[string]interface{}{"a": int64(1)}, Timestamp: time.UnixMilli(111), Metadata: map[string]any{"topic": "demo", "qos": 1}},
					&xsql.Tuple{Emitter: "test", Message: map[string]interface{}{"a": int64(3), "c": "hello"}, Timestamp: time.UnixMilli(111), Metadata: map[string]any{"topic": "demo", "qos": 1}},
				},
			}

			for i, c := range cases {
				op.input <- c
				for _, e := range expects[i] {
					r := <-out
					switch tr := r.(type) {
					case error:
						assert.EqualError(t, e.(error), tr.Error())
					default:
						assert.Equal(t, e, r)
					}
				}
			}

			nctx := mockContext.NewMockContext("test2", "decode_test")
			op.AttachSchema(nctx, "streamName", tt.schema, false)
			cases = []any{
				&xsql.RawTuple{Emitter: "test", Rawdata: []byte("{\"a\":1,\"b\":2}"), Timestamp: time.UnixMilli(111), Metadata: map[string]any{"topic": "demo", "qos": 1}},
				&xsql.RawTuple{Emitter: "test", Rawdata: []byte("[{\"a\":1,\"b\":2},{\"a\":3,\"b\":4,\"c\":\"hello\"}]"), Timestamp: time.UnixMilli(111), Metadata: map[string]any{"topic": "demo", "qos": 1}},
			}
			expectsWithSchema := [][]any{
				{&xsql.Tuple{Emitter: "test", Message: map[string]interface{}{"a": int64(1), "b": 2.0}, Timestamp: time.UnixMilli(111), Metadata: map[string]any{"topic": "demo", "qos": 1}}},
				{
					&xsql.Tuple{Emitter: "test", Message: map[string]interface{}{"a": int64(1), "b": 2.0}, Timestamp: time.UnixMilli(111), Metadata: map[string]any{"topic": "demo", "qos": 1}},
					&xsql.Tuple{Emitter: "test", Message: map[string]interface{}{"a": int64(3), "b": 4.0, "c": "hello"}, Timestamp: time.UnixMilli(111), Metadata: map[string]any{"topic": "demo", "qos": 1}},
				},
			}

			for i, c := range cases {
				op.input <- c
				for _, e := range expectsWithSchema[i] {
					r := <-out
					switch tr := r.(type) {
					case error:
						assert.EqualError(t, e.(error), tr.Error())
					default:
						assert.Equal(t, e, r)
					}
				}
			}

			op.DetachSchema(ctx, ctx.GetRuleId())
			cases = []any{
				&xsql.RawTuple{Emitter: "test", Rawdata: []byte("{\"a\":1,\"b\":2}"), Timestamp: time.UnixMilli(111), Metadata: map[string]any{"topic": "demo", "qos": 1}},
				&xsql.RawTuple{Emitter: "test", Rawdata: []byte("[{\"a\":1,\"b\":2},{\"a\":3,\"b\":4,\"c\":\"hello\"}]"), Timestamp: time.UnixMilli(111), Metadata: map[string]any{"topic": "demo", "qos": 1}},
			}
			expects = [][]any{
				{&xsql.Tuple{Emitter: "test", Message: map[string]interface{}{"b": 2.0}, Timestamp: time.UnixMilli(111), Metadata: map[string]any{"topic": "demo", "qos": 1}}},
				{
					&xsql.Tuple{Emitter: "test", Message: map[string]interface{}{"b": 2.0}, Timestamp: time.UnixMilli(111), Metadata: map[string]any{"topic": "demo", "qos": 1}},
					&xsql.Tuple{Emitter: "test", Message: map[string]interface{}{"b": 4.0}, Timestamp: time.UnixMilli(111), Metadata: map[string]any{"topic": "demo", "qos": 1}},
				},
			}
			for i, c := range cases {
				op.input <- c
				for _, e := range expects[i] {
					r := <-out
					switch tr := r.(type) {
					case error:
						assert.EqualError(t, e.(error), tr.Error())
					default:
						assert.Equal(t, e, r)
					}
				}
			}
		})
	}
}

func TestValidate(t *testing.T) {
	ctx := mockContext.NewMockContext("test1", "decode_test")
	_, err := NewDecodeOp(ctx, false, "test", "streamName", "test1", &def.RuleOption{BufferLength: 10, SendError: true}, &ast.Options{FORMAT: "cann"}, false, true, nil, nil)
	assert.Error(t, err)
	assert.Equal(t, "cannot get converter from format cann, schemaId : format type cann not supported", err.Error())
	_, err = NewDecodeOp(ctx, false, "test", "streamName", "test1", &def.RuleOption{BufferLength: 10, SendError: true}, &ast.Options{FORMAT: "json"}, false, true, nil, map[string]any{"sendInterval": "none"})
	assert.Error(t, err)
	assert.EqualError(t, err, "1 error(s) decoding:\n\n* error decoding 'sendInterval': time: invalid duration \"none\"")
	do, err := NewDecodeOp(ctx, false, "test", "streamName", "test1", &def.RuleOption{BufferLength: 10, SendError: true}, &ast.Options{FORMAT: "json"}, false, true, nil, map[string]any{"sendInterval": "12s"})
	assert.NoError(t, err)
	assert.Equal(t, 12*time.Second, do.c.SendInterval)
	_, err = NewDecodeOp(ctx, true, "test", "streamName", "test1", &def.RuleOption{BufferLength: 10, SendError: true}, &ast.Options{FORMAT: "json"}, false, true, nil, map[string]any{"payloadField": "abc"})
	assert.Error(t, err)
	assert.EqualError(t, err, "payloadFormat is missing")
	_, err = NewDecodeOp(ctx, true, "test", "streamName", "test1", &def.RuleOption{BufferLength: 10, SendError: true}, &ast.Options{FORMAT: "json"}, false, true, nil, map[string]any{"payloadField": "abc", "payloadFormat": "test"})
	assert.Error(t, err)
	assert.EqualError(t, err, "cannot get converter from format test, schemaId : format type test not supported")
}

func TestPayloadDecodeWithSchema(t *testing.T) {
	tests := []struct {
		name   string
		input  any
		schema map[string]*ast.JsonStreamField
		result []any
	}{
		{
			name: "normal",
			input: &xsql.Tuple{
				Emitter:  "test",
				Message:  map[string]any{"payload": []byte(`{"a":23,"b":34}`)},
				Metadata: map[string]any{"topic": "a"},
			},
			schema: map[string]*ast.JsonStreamField{
				"b": {
					Type: "float",
				},
			},
			result: []any{&xsql.Tuple{
				Emitter:  "test",
				Metadata: map[string]any{"topic": "a"},
				Message:  map[string]any{"b": 34.0},
			}},
		},
		{
			name: "list with one payload field not found",
			input: &xsql.Tuple{
				Emitter:  "test",
				Message:  map[string]any{"n": "outside", "payload": []byte(`[{"a":23,"b":34},{"a":99},{"a":55,"b":66}]`)},
				Metadata: map[string]any{"topic": "a"},
			},
			schema: map[string]*ast.JsonStreamField{
				"b": {
					Type: "float",
				},
				"n": nil,
			},
			result: []any{
				&xsql.Tuple{
					Emitter:  "test",
					Metadata: map[string]any{"topic": "a"},
					Message:  map[string]any{"b": 34.0, "n": "outside"},
				},
				&xsql.Tuple{
					Emitter:  "test",
					Metadata: map[string]any{"topic": "a"},
					Message:  map[string]any{"n": "outside"},
				},
				&xsql.Tuple{
					Emitter:  "test",
					Metadata: map[string]any{"topic": "a"},
					Message:  map[string]any{"b": 66.0, "n": "outside"},
				},
			},
		},
		{
			name: "no payload field",
			input: &xsql.Tuple{
				Emitter:  "test",
				Message:  map[string]any{"n": "outside"},
				Metadata: map[string]any{"topic": "a"},
			},
			schema: map[string]*ast.JsonStreamField{
				"b": {
					Type: "float",
				},
			},
			result: []any{},
		},
		{
			name: "wrong payload field",
			input: &xsql.Tuple{
				Emitter:  "test",
				Message:  map[string]any{"n": "outside", "payload": 34},
				Metadata: map[string]any{"topic": "a"},
			},
			schema: map[string]*ast.JsonStreamField{
				"b": {
					Type: "float",
				},
			},
			result: []any{
				errors.New("payload is not bytes: cannot convert int(34) to bytes"),
			},
		},
		{
			name: "wrong input type",
			input: &xsql.RawTuple{
				Emitter:  "test",
				Metadata: map[string]any{"topic": "a"},
			},
			schema: map[string]*ast.JsonStreamField{
				"b": {
					Type: "float",
				},
			},
			result: []any{
				errors.New("unsupported data received: &{test 0001-01-01 00:00:00 +0000 UTC [] map[topic:a] map[]}"),
			},
		},
	}
	ctx, cancel := mockContext.NewMockContext("test1", "decode_test").WithCancel()
	op, err := NewDecodeOp(ctx, true, "test", "streamName", "test1", &def.RuleOption{BufferLength: 10, SendError: true, Concurrency: 10}, &ast.Options{FORMAT: "json", SHARED: true}, false, false, nil, map[string]any{
		"payloadField": "payload", "payloadFormat": "json",
	})

	assert.NoError(t, err)
	out := make(chan any, 100)
	err = op.AddOutput(out, "test")
	assert.NoError(t, err)
	errCh := make(chan error)
	op.Exec(ctx, errCh)
	for i, tt := range tests {
		if i > 0 {
			op.DetachSchema(ctx, fmt.Sprintf("stream%d", i))
		}
		op.AttachSchema(ctx, fmt.Sprintf("stream%d", i), tt.schema, false)
		t.Run(tt.name, func(t *testing.T) {
			op.input <- tt.input
			for _, exp := range tt.result {
				r := <-out
				assert.Equal(t, exp, r)
			}
		})
	}
	cancel()
}

func TestPayloadBatchDecodeWithSchema(t *testing.T) {
	tests := []struct {
		name   string
		input  any
		schema map[string]*ast.JsonStreamField
		result []any
	}{
		{
			name: "normal",
			input: &xsql.Tuple{
				Emitter: "test",
				Message: map[string]any{
					"n": "outside", "frames": []any{
						map[string]any{
							"payload": []byte(`{"a":23,"b":34}`),
							"inner":   123,
						},
						map[string]any{
							"payload": []byte(`{"a":33,"b":44}`),
							"inner":   123,
						},
					},
				},
				Metadata: map[string]any{"topic": "a"},
			},
			schema: map[string]*ast.JsonStreamField{
				"b":     nil,
				"n":     nil,
				"inner": nil,
			},
			result: []any{&xsql.Tuple{
				Emitter:  "test",
				Metadata: map[string]any{"topic": "a"},
				Message:  map[string]any{"b": 44.0, "inner": 123, "n": "outside"},
			}},
		},
		{
			name: "list with one payload field not found",
			input: &xsql.Tuple{
				Emitter: "test",
				Message: map[string]any{"frames": []any{
					map[string]any{
						"payload": []byte(`{"a":23,"b":34}`),
						"inner":   123,
					},
					map[string]any{
						"payload": []byte(`[{"a":23,"b":54},{"a":99},{"a":55,"b":66}]`),
						"inner":   456,
					},
				}},
			},
			schema: map[string]*ast.JsonStreamField{
				"b": {
					Type: "float",
				},
				"inner": nil,
			},
			result: []any{
				&xsql.Tuple{
					Emitter: "test",
					Message: map[string]any{"b": 66.0, "inner": 456},
				},
			},
		},
		{
			name: "no batch payload field",
			input: &xsql.Tuple{
				Emitter:  "test",
				Message:  map[string]any{"n": "outside"},
				Metadata: map[string]any{"topic": "a"},
			},
			schema: map[string]*ast.JsonStreamField{
				"b": {
					Type: "float",
				},
			},
			result: []any{},
		},
		{
			name: "no payload field",
			input: &xsql.Tuple{
				Emitter: "test",
				Message: map[string]any{
					"n": "outside", "frames": []any{
						map[string]any{
							"payload": []byte(`{"a":23,"b":34}`),
							"inner":   123,
						},
						map[string]any{
							"inner2": 333,
						},
						map[string]any{
							"payload": 444,
							"inner":   243,
						},
					},
				},
			},
			schema: map[string]*ast.JsonStreamField{
				"b": {
					Type: "float",
				},
			},
			result: []any{&xsql.Tuple{
				Emitter: "test",
				Message: map[string]any{"b": 34.0, "inner": 123, "n": "outside"},
			}},
		},
		{
			name: "wrong input type",
			input: &xsql.RawTuple{
				Emitter:  "test",
				Metadata: map[string]any{"topic": "a"},
			},
			schema: map[string]*ast.JsonStreamField{
				"b": {
					Type: "float",
				},
			},
			result: []any{
				errors.New("unsupported data received: &{test 0001-01-01 00:00:00 +0000 UTC [] map[topic:a] map[]}"),
			},
		},
	}
	ctx := mockContext.NewMockContext("test1", "decode_test")
	op, err := NewDecodeOp(ctx, true, "test", "streamName", "test1", &def.RuleOption{BufferLength: 10, SendError: true, Concurrency: 10}, &ast.Options{FORMAT: "json", SHARED: true}, false, false, nil, map[string]any{
		"payloadField": "payload", "payloadFormat": "json", "payloadBatchField": "frames",
	})

	assert.NoError(t, err)
	out := make(chan any, 100)
	err = op.AddOutput(out, "test")
	assert.NoError(t, err)
	errCh := make(chan error)
	op.Exec(ctx, errCh)
	for i, tt := range tests {
		if i > 0 {
			op.DetachSchema(ctx, fmt.Sprintf("stream%d", i))
		}
		op.AttachSchema(ctx, fmt.Sprintf("stream%d", i), tt.schema, false)
		t.Run(tt.name, func(t *testing.T) {
			op.input <- tt.input
			for _, exp := range tt.result {
				r := <-out
				assert.Equal(t, exp, r)
			}
		})
	}
}
