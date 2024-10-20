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

package nng

import (
	"fmt"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"go.nanomsg.org/mangos/v3"
	"go.nanomsg.org/mangos/v3/protocol/pair"
	"go.nanomsg.org/mangos/v3/protocol/push"
	"go.nanomsg.org/mangos/v3/protocol/req"
	_ "go.nanomsg.org/mangos/v3/transport/ipc"
	_ "go.nanomsg.org/mangos/v3/transport/tcp"

	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
)

type SockConf struct {
	Url      string `json:"url"`
	Protocol string `json:"protocol"`
}

type Sock struct {
	mangos.Socket
	url       string
	connected atomic.Bool
}

var nngTimeout = 5 * time.Second

func CreateConnection(ctx api.StreamContext, props map[string]any) (modules.Connection, error) {
	c, err := ValidateConf(props)
	if err != nil {
		return nil, err
	}
	var sock mangos.Socket
	switch c.Protocol {
	case "pair":
		sock, err = pair.NewSocket()
	case "push":
		sock, err = push.NewSocket()
	case "req":
		sock, err = req.NewSocket()
	default:
		return nil, fmt.Errorf("unsupported nng protocol %s", c.Protocol)
	}

	if err != nil {
		return nil, err
	}
	// options consider to export
	_ = sock.SetOption(mangos.OptionSendDeadline, nngTimeout)
	_ = sock.SetOption(mangos.OptionRecvDeadline, nngTimeout)
	cli := &Sock{
		url:       c.Url,
		Socket:    sock,
		connected: atomic.Bool{},
	}
	sock.SetPipeEventHook(func(ev mangos.PipeEvent, p mangos.Pipe) {
		switch ev {
		case mangos.PipeEventAttached:
			cli.connected.Store(true)
			ctx.GetLogger().Infof("nano connection attached")
		case mangos.PipeEventAttaching:
			ctx.GetLogger().Infof("nano connection is attaching")
		case mangos.PipeEventDetached:
			cli.connected.Store(false)
			ctx.GetLogger().Warnf("nano connection detached")
			// TODO how to let connection send error event?
		}
	})
	// sock.SetOption(mangos.OptionWriteQLen, 100)
	// sock.SetOption(mangos.OptionReadQLen, 100)
	// sock.SetOption(mangos.OptionBestEffort, false)
	if err = sock.DialOptions(c.Url, map[string]interface{}{
		mangos.OptionDialAsynch:       true, // will not report error and keep connecting
		mangos.OptionMaxReconnectTime: 5 * time.Second,
		mangos.OptionReconnectTime:    100 * time.Millisecond,
		mangos.OptionMaxRecvSize:      0,
	}); err != nil {
		return nil, fmt.Errorf("please make sure nng server side has started and configured, can't dial: %s", err.Error())
	}
	return cli, nil
}

func ValidateConf(props map[string]any) (*SockConf, error) {
	c := &SockConf{
		Protocol: "pair",
	}
	err := cast.MapToStruct(props, c)
	if err != nil {
		return nil, err
	}
	if c.Url == "" {
		return nil, fmt.Errorf("url is required")
	} else {
		// Parse the URL
		parsedURL, err := url.Parse(c.Url)
		if err != nil {
			return nil, fmt.Errorf("error parsing url %s: %s", c.Url, err)
		}
		if parsedURL.Scheme != "tcp" && parsedURL.Scheme != "ipc" {
			return nil, fmt.Errorf("only tcp and ipc scheme are supported")
		}
	}
	if c.Protocol != "pair" && c.Protocol != "push" && c.Protocol != "req" {
		return nil, fmt.Errorf("unsupported protocol %s", c.Protocol)
	}
	return c, nil
}

func (s *Sock) Ping(_ api.StreamContext) error {
	if !s.connected.Load() {
		return fmt.Errorf("not connected")
	}
	return nil
}

func (s *Sock) Close(_ api.StreamContext) error {
	return s.Socket.Close()
}

func (s *Sock) DetachSub(_ api.StreamContext, _ map[string]any) {
	// do nothing
}

func (s *Sock) Send(ctx api.StreamContext, data []byte) error {
	ctx.GetLogger().Debugf("ngg publish %x", data)
	if s.Socket != nil && s.connected.Load() {
		return s.Socket.Send(data)
	}
	return errorx.NewIOErr(`nng connection is not established`)
}

var _ modules.Connection = &Sock{}
