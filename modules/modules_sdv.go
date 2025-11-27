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
	"fmt"

	"github.com/emqx/ekuiper_can/io/history"
	"github.com/emqx/ekuiper_can/io/kuksa"
	"github.com/emqx/ekuiper_can/schema/idl"
	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/pkg/cast"

	"github.com/emqx/ekuiper_can/converter/avro"
	converterCan "github.com/emqx/ekuiper_can/converter/can"
	"github.com/emqx/ekuiper_can/converter/canjson"
	convidl "github.com/emqx/ekuiper_can/converter/idl"
	"github.com/emqx/ekuiper_can/converter/jsonColStr"
	"github.com/emqx/ekuiper_can/converter/ocf"
	"github.com/emqx/ekuiper_can/converter/spi"
	"github.com/emqx/ekuiper_can/dynconf"
	"github.com/emqx/ekuiper_can/funcs"
	"github.com/emqx/ekuiper_can/hack"
	"github.com/emqx/ekuiper_can/io/can"
	"github.com/emqx/ekuiper_can/io/file_hook"
	"github.com/emqx/ekuiper_can/io/nano"
	"github.com/emqx/ekuiper_can/io/query"
	"github.com/emqx/ekuiper_can/schema/dbc"
	spiSchema "github.com/emqx/ekuiper_can/schema/spi"

	"github.com/lf-edge/ekuiper/v2/extensions/impl/video"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/message"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
	"github.com/lf-edge/ekuiper/v2/pkg/props"
)

type spiProp struct {
	IsLittleEndian bool   `json:"isLittleEndian"`
	Subtype        string `json:"subtype"`
}

func init() {
	hack.SetDefaultNS2([]string{"223.5.5.5:53", "8.8.8.8:53"})
	hack.FixTimezone()
	// From LF
	modules.RegisterSource("video", video.GetSource)
	// From Others
	modules.RegisterConverter("gbf", func(ctx api.StreamContext, idlPath string, schema map[string]*ast.JsonStreamField, props map[string]any) (message.Converter, error) {
		sp := &spiProp{}
		err := cast.MapToStruct(props, sp)
		if err != nil {
			return nil, fmt.Errorf("invalid prop for spi format: %v", err)
		}
		return spi.NewSpi(ctx, idlPath, schema, sp.Subtype, sp.IsLittleEndian, props)
	})
	modules.RegisterMerger("gbf", func(ctx api.StreamContext, idlPath string, logicalSchema map[string]*ast.JsonStreamField, props map[string]any) (modules.Merger, error) {
		sp := &spiProp{}
		err := cast.MapToStruct(props, sp)
		if err != nil {
			return nil, fmt.Errorf("invalid prop for spi merger: %v", err)
		}
		f, err := spi.NewSpi(ctx, idlPath, logicalSchema, sp.Subtype, sp.IsLittleEndian, props)
		if err != nil {
			return nil, err
		}
		return f.(modules.Merger), err
	})
	modules.RegisterSchemaType("gbf", &spiSchema.IdlType{}, ".idl")
	modules.RegisterSchemaType("idl", &idl.IdlType{}, ".idl")
	modules.RegisterConverterSchemas("gbf", "gbf")
	modules.RegisterConverterSchemas("idl", "idl")

	// deprecate spi, to be removed. Now point to gbf
	modules.RegisterConverter("spi", modules.Converters["gbf"])
	modules.RegisterMerger("spi", modules.Mergers["gbf"])
	modules.RegisterSchemaType("spi", &spiSchema.IdlType{}, ".idl")
	modules.RegisterConverterSchemas("spi", "spi")

	modules.RegisterSource("can", can.GetSource)
	modules.RegisterConverter("can", func(ctx api.StreamContext, dbcFile string, schema map[string]*ast.JsonStreamField, props map[string]any) (message.Converter, error) {
		return converterCan.NewConverter(ctx, dbcFile, schema)
	})
	modules.RegisterConverter("canjson", func(ctx api.StreamContext, dbcFile string, logicalSchema map[string]*ast.JsonStreamField, props map[string]any) (message.Converter, error) {
		return canjson.NewConverter(ctx, dbcFile, logicalSchema, props)
	})
	modules.RegisterSchemaType("dbc", &dbc.DbcType{}, ".dbc")
	// In order to be compatible with the current implementation, do not require schema for now
	// modules.RegisterConverterSchemas("can", "dbc")
	// modules.RegisterConverterSchemas("canjson", "dbc")

	modules.RegisterConverter("idl", func(ctx api.StreamContext, idlPath string, _ map[string]*ast.JsonStreamField, props map[string]any) (message.Converter, error) {
		// Required to register schema firstly
		m, ok := props["$$messageName"]
		if !ok {
			return nil, fmt.Errorf("schema message name not found")
		}
		isLittleEndian := false
		vv, ok := props["isLittleEndian"]
		if ok {
			vvBool, isBool := vv.(bool)
			if isBool {
				isLittleEndian = vvBool
			} else {
				return nil, fmt.Errorf("isLittleEndian property should be a bool")
			}
		}
		return convidl.NewIDLConverter(m.(string), idlPath, isLittleEndian)
	})

	modules.RegisterSink("nano", nano.GetSink)
	modules.RegisterFileRollHook("nano", func() modules.RollHook {
		return &file_hook.NanoMQRollingHook{}
	})
	modules.RegisterLookupSource("nano", nano.GetLookupSource)

	modules.RegisterSource("nanoquery", query.GetSource)
	modules.RegisterSink("nanoquery", query.GetQuerySink)
	// Read in vin
	props.InitProps()
	dynconf.InitDynconfSub("tcp://127.0.0.1:1883", "ekprops", "$local/notification")
	modules.RegisterConverter("avro", func(ctx api.StreamContext, avscPath string, schema map[string]*ast.JsonStreamField, props map[string]any) (message.Converter, error) {
		return avro.NewConverter(ctx, avscPath, schema)
	})
	modules.RegisterWriterConverter("ocf", func(ctx api.StreamContext, avscPath string, schema map[string]*ast.JsonStreamField, props map[string]any) (message.ConvertWriter, error) {
		return ocf.NewWriter(ctx, avscPath, schema, props)
	})
	modules.RegisterWriterConverter("jsoncolstr", func(ctx api.StreamContext, _ string, schema map[string]*ast.JsonStreamField, _ map[string]any) (message.ConvertWriter, error) {
		return jsonColStr.NewWriter(ctx, schema)
	})
	modules.RegisterConverter("jsoncolstr", func(ctx api.StreamContext, _ string, schema map[string]*ast.JsonStreamField, props map[string]any) (message.Converter, error) {
		return jsonColStr.NewConverter(ctx, schema)
	})
	modules.RegisterFunc("bit", funcs.NewBitFunc)
	modules.RegisterSink("kuksa", func() api.Sink {
		return &kuksa.KuksaSink{}
	})
	modules.RegisterSource("history", history.GetSource)
}
