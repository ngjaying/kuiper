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
	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/xsql"
)

type Row struct {
	batchSize int
	// state
	buffer *xsql.WindowTuples
}

func NewRowBatcher(batchSize int) *Row {
	return &Row{batchSize: batchSize, buffer: &xsql.WindowTuples{
		Content: make([]xsql.Row, 0, batchSize),
	}}
}

func (r *Row) Ingest(_ api.StreamContext, data xsql.Row) any {
	r.buffer.AddTuple(data)
	return nil
}

func (r *Row) Flush(_ api.StreamContext) any {
	buffer := r.buffer
	r.buffer = &xsql.WindowTuples{
		Content: make([]xsql.Row, 0, r.batchSize),
	}
	return buffer
}

func (r *Row) Len(_ api.StreamContext) int {
	return r.buffer.Len()
}

var _ Batcher = &Row{}
