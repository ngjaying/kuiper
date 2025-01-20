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
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
)

type Batcher interface {
	Ingest(ctx api.StreamContext, data xsql.Row) any
	Flush(ctx api.StreamContext) any
	Len(ctx api.StreamContext) int
}

func GetBatcher(isColumnar bool, batchSize int, schema map[string]*ast.JsonStreamField) Batcher {
	if isColumnar {
		return NewColumnar(schema, batchSize)
	} else {
		return NewRowBatcher(batchSize)
	}
}
