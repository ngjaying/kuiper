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
	"sort"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

type Columnar struct {
	batchSize int
	// common schema
	cols []string
	// state, an array for each col
	buffer [][]any
	// only record the first
	props map[string]string
}

func NewColumnar(schema map[string]*ast.JsonStreamField, batchSize int) *Columnar {
	cols := make([]string, 0, len(schema))
	for k := range schema {
		cols = append(cols, k)
	}
	sort.Strings(cols)
	cc := &Columnar{
		batchSize: batchSize,
		cols:      cols,
	}
	cc.reset()
	return cc
}

func (c *Columnar) Ingest(_ api.StreamContext, data xsql.Row) any {
	if ss, ok := data.(api.HasDynamicProps); ok {
		c.props = ss.AllProps()
	}
	for i, col := range c.cols {
		v, _ := data.Value(col, "")
		c.buffer[i] = append(c.buffer[i], v)
	}
	return nil
}

func (c *Columnar) Flush(_ api.StreamContext) any {
	message := make(map[string]any, len(c.cols))
	for i, col := range c.cols {
		message[col] = c.buffer[i]
	}
	tuple := &xsql.Tuple{
		Emitter:   "",
		Message:   message,
		Timestamp: timex.GetNow(),
		Props:     c.props,
	}
	c.reset()
	return tuple
}

func (c *Columnar) Len(_ api.StreamContext) int {
	if len(c.buffer) > 0 {
		return len(c.buffer[0])
	}
	return 0
}

func (c *Columnar) reset() {
	c.buffer = make([][]any, len(c.cols))
	for i := range c.cols {
		c.buffer[i] = make([]any, 0, c.batchSize)
	}
}

var _ Batcher = &Columnar{}
