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

package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"go.nanomsg.org/mangos/v3"
	"go.nanomsg.org/mangos/v3/protocol/rep"
	_ "go.nanomsg.org/mangos/v3/transport/tcp"
)

func main() {
	raw, err := os.ReadFile("canpacket_demo.raw")
	if err != nil {
		panic(err)
	}
	mockNanoLookup("tcp://127.0.0.1:10000", raw)
}

// mockNeuron start the nng rep server
func mockNanoLookup(url string, raw []byte) {
	var (
		sock mangos.Socket
		err  error
	)
	if sock, err = rep.NewSocket(); err != nil {
		log.Fatalf("can't get new rep socket: %s", err)
	}
	if err = sock.Listen(url); err != nil {
		log.Fatalf("can't listen on rep socket: %s", err.Error())
	}
	log.Printf("listen on rep socket")

	for {
		// Could also use sock.RecvMsg to get header
		msg, err := sock.Recv()
		if err != nil {
			log.Fatalf("cannot receive on rep socket: %s", err.Error())
		}
		fmt.Printf("NODE0: RECEIVED DATE REQUEST %s\n", msg)
		cols := strings.Split(string(msg), "-")
		if len(cols) == 2 { // no need to terminate
			start, _ := strconv.ParseInt(cols[0], 10, 32)
			end, _ := strconv.ParseInt(cols[1], 10, 32)
			l := int(float64((end-start)/1000) / 60.0 * float64(len(raw)))
			err = sock.Send(raw[0:l])
			if err != nil {
				log.Fatalf("can't send reply: %s", err.Error())
			}
			fmt.Printf("NODE0: SEND DATE REPLY %d\n", l)
		}
	}
}
