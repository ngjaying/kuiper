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

package fvt

import (
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/lf-edge/ekuiper/v2/fvt"
)

const (
	URL              = "http://127.0.0.1:9081"
	ContentTypeJson  = "application/json"
	DataPath         = "test/fvt/data"
	ResultPath       = "test/fvt/result"
	RulesPath        = "test/fvt/rules"
	MQTTBroker       = "tcp://127.0.0.1:1883"
	ConstantInterval = 100 * time.Millisecond
)

var (
	PWD    string
	EKPWD  string
	client *fvt.SDK
)

func init() {
	// Get pwd
	dir, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}
	dir = filepath.Join(dir, "..", "..")
	fmt.Println("Current PWD:", dir)
	PWD = dir
	// Get eKuiper pwd
	EKPWD = PWD
	buildDir := filepath.Join(dir, "_build")
	info, err := os.Stat(buildDir)
	if err != nil || !info.IsDir() {
		fmt.Println("build dir not exist, use eKuiper source dir")
	} else {
		err = filepath.Walk(buildDir, func(path string, info fs.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() && strings.HasPrefix(info.Name(), "kuiper-") {
				EKPWD = path
				return nil
			}
			return nil
		})
	}
	fmt.Println("Current EKPWD:", EKPWD)
	client, err = fvt.NewSdk(URL)
	if err != nil {
		log.Fatal(err)
	}
}
