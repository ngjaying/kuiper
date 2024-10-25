// Copyright 2024 EMQ Technologies Co., Ltd.
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

//go:build gee

package modules

import (
	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/message"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"

	"github.com/emqx/ekuiper_can/conf"
	"github.com/emqx/ekuiper_can/converter/bin/cellt"
	"github.com/emqx/ekuiper_can/converter/bin/cellu"
	"github.com/emqx/ekuiper_can/converter/bin/sigl"
	"github.com/emqx/ekuiper_can/converter/dbc"
	"github.com/emqx/ekuiper_can/converter/spi"
	"github.com/emqx/ekuiper_can/funcs"
	"github.com/emqx/ekuiper_can/io/file_hook"
	"github.com/emqx/ekuiper_can/io/nano"
	"github.com/emqx/ekuiper_can/io/query"
)

func init() {
	// Init descriptor setting
	spi.InitGee()
	dbc.Keyfunc = spi.Key

	modules.RegisterConverter("spi", func(ctx api.StreamContext, dbcFile string, schema map[string]*ast.JsonStreamField, props map[string]any) (message.Converter, error) {
		return spi.NewPacketFormat(ctx, dbcFile, schema)
	})
	modules.RegisterMerger("spi", func(ctx api.StreamContext, payloadSchema string, logicalSchema map[string]*ast.JsonStreamField) (modules.Merger, error) {
		f, err := spi.NewPacketFormat(ctx, payloadSchema, logicalSchema)
		return f.(modules.Merger), err
	})
	modules.RegisterConverter("sigl", func(ctx api.StreamContext, _ string, _ map[string]*ast.JsonStreamField, _ map[string]any) (message.Converter, error) {
		return &sigl.Packet{}, nil
	})
	modules.RegisterFunc("notifyled", funcs.NewNotifyLed)
	modules.RegisterConverter("cellu", func(ctx api.StreamContext, _ string, _ map[string]*ast.JsonStreamField, _ map[string]any) (message.Converter, error) {
		return &cellu.Cellu{}, nil
	})
	modules.RegisterFunc("collectu", funcs.NewCollectU)
	modules.RegisterConverter("cellt", func(ctx api.StreamContext, _ string, _ map[string]*ast.JsonStreamField, _ map[string]any) (message.Converter, error) {
		return &cellt.Cellt{}, nil
	})
	modules.RegisterFunc("collectt", funcs.NewCollectT)

	modules.RegisterSink("nano", nano.GetSink)
	modules.RegisterFileRollHook("nano", func() modules.RollHook {
		return &file_hook.NanoMQRollingHook{}
	})
	modules.RegisterLookupSource("nano", nano.GetLookupSource)

	modules.RegisterSource("nanoquery", query.GetSource)
	modules.RegisterSink("nanoquery", query.GetQuerySink)
	// Read in vin
	conf.InitStaticConf()
	modules.RegisterFunc("props", funcs.NewPropsFunc)
}
