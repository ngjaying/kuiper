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

//go:build sdv

package modules

import (
	"github.com/emqx/ekuiper_can/converter/spi"
	"github.com/emqx/ekuiper_can/hack"
	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/extensions/impl/video"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/message"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"

	converterCan "github.com/emqx/ekuiper_can/converter/can"
	"github.com/emqx/ekuiper_can/converter/canjson"
	"github.com/emqx/ekuiper_can/converter/json_can_merger"
	"github.com/emqx/ekuiper_can/io/can"
	"github.com/emqx/ekuiper_can/io/file_hook"
	"github.com/emqx/ekuiper_can/io/nano"
	"github.com/emqx/ekuiper_can/io/query"
)

func init() {
	hack.SetDefaultNS2([]string{"223.5.5.5:53", "8.8.8.8:53"})
	hack.FixTimezone()
	// From LF
	modules.RegisterSource("video", video.GetSource)
	// From Others
	modules.RegisterConverter("spi", func(ctx api.StreamContext, idlPath string, schema map[string]*ast.JsonStreamField, props map[string]any) (message.Converter, error) {
		return spi.NewSpi(ctx, idlPath, schema)
	})
	modules.RegisterMerger("spi", func(ctx api.StreamContext, idlPath string, logicalSchema map[string]*ast.JsonStreamField) (modules.Merger, error) {
		f, err := spi.NewSpi(ctx, idlPath, logicalSchema)
		return f.(modules.Merger), err
	})
	modules.RegisterSource("can", can.GetSource)
	modules.RegisterConverter("can", func(ctx api.StreamContext, dbcFile string, schema map[string]*ast.JsonStreamField, props map[string]any) (message.Converter, error) {
		return converterCan.NewConverter(ctx, dbcFile, schema)
	})
	modules.RegisterConverter("canjson", func(ctx api.StreamContext, dbcFile string, logicalSchema map[string]*ast.JsonStreamField, props map[string]any) (message.Converter, error) {
		return canjson.NewConverter(ctx, dbcFile, logicalSchema, props)
	})
	modules.RegisterMerger("jsoncan", func(ctx api.StreamContext, payloadSchema string, logicalSchema map[string]*ast.JsonStreamField) (modules.Merger, error) {
		return json_can_merger.NewMerger(ctx, payloadSchema, logicalSchema)
	})
	modules.RegisterSink("nano", nano.GetSink)
	modules.RegisterFileRollHook("nano", func() modules.RollHook {
		return &file_hook.NanoMQRollingHook{}
	})
	modules.RegisterLookupSource("nano", nano.GetLookupSource)

	modules.RegisterSource("nanoquery", query.GetSource)
	modules.RegisterSink("nanoquery", query.GetQuerySink)
}
