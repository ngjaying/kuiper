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
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"time"

	"go.nanomsg.org/mangos/v3"
	"go.nanomsg.org/mangos/v3/protocol/pair"
	_ "go.nanomsg.org/mangos/v3/transport/tcp"
)

func main() {
	raw, err := os.ReadFile("spi1000")
	if err != nil {
		panic(err)
	}
	mockNanoQuery("tcp://0.0.0.0:10000", raw)
}

// mockNeuron start the nng pair server
func mockNanoQuery(url string, raw []byte) {
	var (
		sock mangos.Socket
		err  error
	)
	if sock, err = pair.NewSocket(); err != nil {
		log.Fatalf("can't get new pair socket: %s", err)
	}
	if err = sock.Listen(url); err != nil {
		log.Fatalf("can't listen on pair socket: %s", err.Error())
	}
	log.Printf("listen on pair socket")
	eof, _ := hex.DecodeString("0bad")
	for {
		// Could also use sock.RecvMsg to get header
		msg, err := sock.Recv()
		if err != nil {
			log.Fatalf("cannot receive on pair socket: %s", err.Error())
		}
		fmt.Printf("NODE0: RECEIVED DATE REQUEST %s\n", msg)
		//cols := strings.Split(string(msg), "-")
		//if len(cols) == 3 { // no need to terminate
		//	start, _ := strconv.ParseInt(cols[1], 10, 32)
		//	end, _ := strconv.ParseInt(cols[2], 10, 32)
		//	// at least search 200 seconds
		//	alllen := int((end - start) / 1000 / 200)
		for i := 0; i < 9; i++ {
			fmt.Printf("sends %d piece\n", i)
			err = sock.Send(raw)
			if err != nil {
				log.Fatalf("can't send reply: %s", err.Error())
			}
			time.Sleep(time.Second)
		}
		_ = sock.Send(eof)
		fmt.Printf("NODE0: SEND DATE REPLY %d\n", 10)
		//}
	}
}
