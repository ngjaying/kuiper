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

package file

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

type conf struct {
	Offset time.Duration `json:"offset"`
}

type CimEDecorator struct {
	offset int64 // The offset of the timestamp for each line, default to 15 minutes
	// init date time string
	dateStr string
	timeStr string
	// the state
	ts int64 // The starting timestamp of the file, usually in the header/ignore lines
}

func (c *CimEDecorator) Provision(ctx api.StreamContext, props map[string]any) error {
	cc := &conf{
		Offset: 15 * time.Minute,
	}
	err := cast.MapToStruct(props, cc)
	if err != nil {
		return err
	}
	c.offset = cc.Offset.Milliseconds()
	return nil
}

func (c *CimEDecorator) ReadMeta(ctx api.StreamContext, line []byte) {
	switch line {
	case nil: // stop reading
		var tsstr string
		if c.dateStr != "" && c.timeStr != "" {
			tsstr = fmt.Sprintf("%s %s", c.dateStr, c.timeStr)
		} else if c.dateStr != "" {
			tsstr = c.dateStr
		} else if c.timeStr != "" {
			tsstr = c.timeStr
		}
		if tsstr == "" {
			c.ts = timex.GetNowInMilli()
		} else {
			// hack format here. The time format "2006-01-02_15:04" cannot be parsed by junzhu/now, so replace the string
			tts, err := cast.ParseTimeByFormats(strings.Replace(tsstr, "_", " ", -1), []string{"2006-01-02 15:04", "2006-01-02 15-04-05"})
			if err != nil {
				ctx.GetLogger().Errorf("Failed to parse time %s in file: %s", tsstr, err)
				return
			}
			c.ts = tts.UnixMilli()
		}
	default:
		timePattern := `(?i)time='(.*?)'`
		datePattern := `(?i)date='(.*?)'`
		re := regexp.MustCompile(timePattern)
		matches := re.FindStringSubmatch(string(line))
		if len(matches) > 1 {
			c.timeStr = matches[len(matches)-1]
		}
		re = regexp.MustCompile(datePattern)
		matches = re.FindStringSubmatch(string(line))
		if len(matches) > 1 {
			c.dateStr = matches[len(matches)-1]
		}
	}
}

func (c *CimEDecorator) Decorate(_ api.StreamContext, data any) any {
	mm, ok := data.(map[string]any)
	if !ok {
		return data
	}
	mm["ts"] = c.ts
	c.ts += c.offset
	return mm
}

var _ modules.FileStreamDecorator = (*CimEDecorator)(nil)
