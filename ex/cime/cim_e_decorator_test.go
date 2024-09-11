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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func TestCimEDecorator_ReadMeta(t *testing.T) {
	tests := []struct {
		name  string
		lines [][]byte
		ts    int64
	}{
		{
			name: "ReadMeta with time and date",
			lines: [][]byte{
				[]byte("time='12:00:00' date='2022-01-01'"),
			},
			ts: 1641038400000,
		},
		{
			name: "ReadMeta with time",
			lines: [][]byte{
				[]byte("//\tmatfile"),
				[]byte("<!\tSystem=OMS\tVersion=1.0\tCode=UTF-8\tData=1.0\t!>"),
				[]byte("<matfile::windData\ttime='2022-12-05_10:15'\tmatfile='XMHRNL2022-12-05_1000_ky4h.wpd'>\n"),
			},
			ts: 1670235300000,
		},
		{
			name: "ReadMeta with date",
			lines: [][]byte{
				[]byte("//\tmatfile"),
				[]byte("<!\tSystem=OMS\tVersion=1.0\tCode=UTF-8\tData=1.0\t!>"),
				[]byte("<matfile::windData\tdate='2023-07-26 00:30:00'\tmatfile='XMHRNL2022-12-05_1000_ky4h.wpd'>\n"),
			},
			ts: 1690331400000,
		},
		{
			name: "ReadMeta with time and date",
			lines: [][]byte{
				[]byte("//超短期功率预测信息监测"),
				[]byte("<!System=OMS\tVersion=1.0\tCode=UTF-8\tData=1.0!>\n"),
				[]byte("<CDQYC::GD.GDZJYS\tDate='2022-07-21'\tTime='03-15-00'>\n"),
			},
			ts: 1658373300000,
		},
	}
	ctx := mockContext.NewMockContext("testcime", "test")
	err := cast.SetTimeZone("UTC")
	assert.NoError(t, err)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &CimEDecorator{}
			for _, line := range tt.lines {
				c.ReadMeta(ctx, line)
			}
			c.ReadMeta(ctx, nil)
			assert.Equal(t, tt.ts, c.ts)
		})
	}
}

func TestCimEDecorator_Decorate(t *testing.T) {
	ctx := mockContext.NewMockContext("testcimedec", "test")
	t.Run("Decorate with offset", func(t *testing.T) {
		c := &CimEDecorator{
			offset: 1000,
			ts:     2000,
		}
		data := make(map[string]any)
		result := c.Decorate(ctx, data)
		assert.Equal(t, int64(2000), result.(map[string]any)["ts"])
		assert.Equal(t, int64(3000), c.ts)
		result = c.Decorate(ctx, data)
		assert.Equal(t, int64(3000), result.(map[string]any)["ts"])
		assert.Equal(t, int64(4000), c.ts)
	})
}
