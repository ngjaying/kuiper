// Copyright 2024-2025 EMQ Technologies Co., Ltd.
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

//go:build ex

package modules

import (
	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/lf-edge/ekuiper/v2/extensions/impl/onnx"

	"github.com/emqx/ekuiper_can/io/file_hook/cime"
	"github.com/lf-edge/ekuiper/v2/extensions/impl/image"
	"github.com/lf-edge/ekuiper/v2/extensions/impl/influx"
	"github.com/lf-edge/ekuiper/v2/extensions/impl/influx2"
	"github.com/lf-edge/ekuiper/v2/extensions/impl/kafka"
	sql2 "github.com/lf-edge/ekuiper/v2/extensions/impl/sql"
	"github.com/lf-edge/ekuiper/v2/extensions/impl/video"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/message"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"

	converterCan "github.com/emqx/ekuiper_can/converter/can"
	"github.com/emqx/ekuiper_can/converter/canjson"
	"github.com/emqx/ekuiper_can/io/can"
)

func init() {
	// From LF
	modules.RegisterSource("video", func() api.Source { return video.GetSource() })
	modules.RegisterSource("kafka", func() api.Source { return kafka.GetSource() })
	modules.RegisterSink("kafka", func() api.Sink { return kafka.GetSink() })
	modules.RegisterSink("image", func() api.Sink { return image.GetSink() })
	modules.RegisterSink("influx", func() api.Sink { return influx.GetSink() })
	modules.RegisterSink("influx2", func() api.Sink { return influx2.GetSink() })
	modules.RegisterSource("sql", sql2.GetSource)
	modules.RegisterLookupSource("sql", sql2.GetLookupSource)
	modules.RegisterSink("sql", sql2.GetSink)
	// Form others
	modules.RegisterFileStreamReaderAlias("cime", "csv")
	modules.RegisterFileStreamDecorator("cime", func(ctx api.StreamContext) modules.FileStreamDecorator {
		return &cime.CimEDecorator{}
	})
	modules.RegisterSource("can", can.GetSource)
	modules.RegisterConverter("can", func(ctx api.StreamContext, dbcFile string, schema map[string]*ast.JsonStreamField, props map[string]any) (message.Converter, error) {
		return converterCan.NewConverter(ctx, dbcFile, schema)
	})
	modules.RegisterConverter("canjson", func(ctx api.StreamContext, dbcFile string, logicalSchema map[string]*ast.JsonStreamField, props map[string]any) (message.Converter, error) {
		return canjson.NewConverter(ctx, dbcFile, logicalSchema, props)
	})
	modules.RegisterFunc("onnx", func() api.Function {
		return &onnx.OnnxFunc{}
	})
}
