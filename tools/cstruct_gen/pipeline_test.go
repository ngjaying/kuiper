package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPipeline(t *testing.T) {
	// A simple pipeline test using a fake CSV and fake headers
	dir := t.TempDir()
	
	h1 := `
	typedef struct {
		uint32_t a;
		uint8_t  b;
	} MyStruct;

	MyStruct Basc_Svc_Ntf_Meth;
	`
	err := os.WriteFile(filepath.Join(dir, "Svc.h"), []byte(h1), 0644)
	assert.NoError(t, err)

	// Create a mock all.csv (Format 0)
	// svcIdx: 17, methIdx: 19, sigIdx: 20, dtIdx: 21, baseDtIdx: 2, srcIdx: 15
	header := make([]string, 22)
	header[0] = "数据项英文名称"
	
	row := make([]string, 22)
	row[15] = "Service"
	row[17] = "Basc_Svc"
	row[19] = "Ntf_Meth"
	row[20] = "Sig1"
	row[21] = "uint32_t"
	row[2] = "STRUCT"

	csvPath := filepath.Join(dir, "all.csv")
	f, _ := os.Create(csvPath)
	fmt.Fprintf(f, "%s\n", strings.Join(header, ","))
	fmt.Fprintf(f, "%s\n", strings.Join(row, ","))
	f.Close()

	// 1. Parse CSV
	svcSet, _, dtypes, err := ParseCSVs([]string{csvPath})
	assert.NoError(t, err)
	assert.True(t, svcSet["Basc_Svc.Ntf_Meth"])

	// 2. Parse Headers
	structs, varMap, err := ParseHeadersDir(dir)
	assert.NoError(t, err)

	// 3. Resolve and Layout
	entries := make(map[string]*StructDef)
	resolvedLayouts := make(map[string]struct {
		size  int
		align int
	})
	
	canonicalKey := "Basc_Svc.Ntf_Meth"
	target, kind := ResolveStructName(canonicalKey, varMap, structs, dtypes[canonicalKey])
	assert.Equal(t, "MyStruct", target)
	assert.Equal(t, kindStruct, kind)

	layout, _, _, err := layoutStruct(target, structs, resolvedLayouts)
	assert.NoError(t, err)
	
	sd := BuildStructDef(layout, target, structs, resolvedLayouts)
	entries["Basc_Svc.Ntf_Meth"] = sd

	// 4. Write Schema
	var buf bytes.Buffer
	err = WriteSchema(&buf, entries)
	assert.NoError(t, err)

	var output map[string]interface{}
	err = json.Unmarshal(buf.Bytes(), &output)
	assert.NoError(t, err)

	structsMap, ok := output["structs"].(map[string]interface{})
	assert.True(t, ok)
	_, ok = structsMap["Basc_Svc.Ntf_Meth"]
	assert.True(t, ok)
}

func stringsJoin(s []string, sep string) string {
	res := ""
	for i, v := range s {
		if i > 0 {
			res += sep
		}
		res += v
	}
	return res
}
