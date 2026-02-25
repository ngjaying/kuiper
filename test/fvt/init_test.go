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
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/emqx/ekuiper_build/modules"
	"github.com/lf-edge/ekuiper/v2/cmd"
	"github.com/lf-edge/ekuiper/v2/fvt"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
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

	timex.IsTesting = false
	timex.InitClock()
	// Create dirs that build_prepare creates, in case they're missing
	os.MkdirAll(filepath.Join(EKPWD, "data"), 0755)
	os.MkdirAll(filepath.Join(EKPWD, "log"), 0755)
	os.MkdirAll(filepath.Join(EKPWD, "plugins", "sources"), 0755)
	os.MkdirAll(filepath.Join(EKPWD, "plugins", "sinks"), 0755)
	os.MkdirAll(filepath.Join(EKPWD, "plugins", "functions"), 0755)
	os.MkdirAll(filepath.Join(EKPWD, "etc", "services"), 0755)
	// Pre-create schema directories for every registered schema type so that
	// This avoids the need to copy all config files to CWD and works regardless of
	// whether EKPWD is a subdirectory of CWD or not.
	os.Setenv("KuiperBaseKey", EKPWD)
	// Start eKuiper in-process.
	// Clear os.Args so eKuiper's logger doesn't see -test.* flags which would set
	// IsTesting=true and redirect all paths to "service/test" (panic if missing).
	os.Args = []string{"kuiperd"}
	cmd.Version = "fvt"
	go cmd.Main()
	count := 10
	for count > 0 {
		time.Sleep(ConstantInterval)
		resp, err := client.Get("ping")
		if err == nil && resp.StatusCode == http.StatusOK {
			fmt.Println("service ready")
			break
		}
		count--
	}
	if count == 0 {
		fmt.Println("service not ready after 10 tries")
	}
}
