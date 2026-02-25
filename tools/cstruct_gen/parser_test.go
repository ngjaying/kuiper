package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseCommonH(t *testing.T) {
	// Setup a temp dir with a copy of Common.h logic
	dir := t.TempDir()
	content := `
	typedef struct {
		uint8_t VehSpeed;
		uint32_t TgtMotorMotorTorq;
	} EPSBasInfo;

	char * Basc_TboxDataInfo_Ntf_TboxDataInfo;
	struct SomeType Basc_ACChrgMntr_Ntf_ACChrgMntrInfo;
	`
	err := os.WriteFile(filepath.Join(dir, "Test.h"), []byte(content), 0644)
	assert.NoError(t, err)

	structs, varMap, err := ParseHeadersDir(dir)
	assert.NoError(t, err)

	// Check struct extraction
	fields, ok := structs["EPSBasInfo"]
	assert.True(t, ok)
	assert.Len(t, fields, 2)
	assert.Equal(t, "VehSpeed", fields[0].Name)
	assert.Equal(t, "uint8_t", fields[0].CType)

	// Check variable declaration extraction (varMap)
	assert.Equal(t, "char*", varMap["Basc_TboxDataInfo_Ntf_TboxDataInfo"])
	assert.Equal(t, "SomeType", varMap["Basc_ACChrgMntr_Ntf_ACChrgMntrInfo"])
}
