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
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

type StreamRowBatcher struct {
}

func NewStreamRowBatcher() *StreamRowBatcher {
	return &StreamRowBatcher{}
}

func (r *StreamRowBatcher) Ingest(_ api.StreamContext, data xsql.Row) any {
	return data
}

func (r *StreamRowBatcher) Flush(ctx api.StreamContext) any {
	ctx.GetLogger().Debugf("send batch end")
	return xsql.BatchEOFTuple(timex.GetNow())
}

func (r *StreamRowBatcher) Len(_ api.StreamContext) int {
	// always return a number bigger than 1 to trigger send
	// send will check currIndex bigger than 0
	return 1
}

var _ Batcher = &StreamRowBatcher{}
