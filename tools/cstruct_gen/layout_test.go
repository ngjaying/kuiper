package main

import (
	"testing"
	"github.com/stretchr/testify/assert"
)

func TestAlignmentPadding(t *testing.T) {
	// {uint8, uint32} -> 3 bytes padding, total=8 bytes, max align=4
	structs := map[string][]rawField{
		"Test1": {
			{Name: "a", CType: "uint8_t"},
			{Name: "b", CType: "uint32_t"},
		},
	}

	fields, totalSize, maxAlign, err := layoutStruct("Test1", structs, nil)
	assert.NoError(t, err)
	assert.Equal(t, 8, totalSize)
	assert.Equal(t, 4, maxAlign)
	assert.Len(t, fields, 3)

	assert.Equal(t, "a", fields[0].Name)
	assert.Equal(t, 0, fields[0].Offset)

	assert.Equal(t, "<pad 3B>", fields[1].Name)
	assert.Equal(t, 1, fields[1].Offset)
	assert.Equal(t, 3, fields[1].Size)

	assert.Equal(t, "b", fields[2].Name)
	assert.Equal(t, 4, fields[2].Offset)
}

func TestTrailingPadding(t *testing.T) {
	// {uint32, uint8} -> 3 bytes trailing padding, total=8 bytes, max align=4
	structs := map[string][]rawField{
		"Test2": {
			{Name: "a", CType: "uint32_t"},
			{Name: "b", CType: "uint8_t"},
		},
	}

	fields, totalSize, maxAlign, err := layoutStruct("Test2", structs, nil)
	assert.NoError(t, err)
	assert.Equal(t, 8, totalSize)
	assert.Equal(t, 4, maxAlign)
	assert.Len(t, fields, 3)

	assert.Equal(t, "a", fields[0].Name)
	assert.Equal(t, "b", fields[1].Name)
	assert.Equal(t, 4, fields[1].Offset)

	assert.Equal(t, "<trail_pad 3B>", fields[2].Name)
	assert.Equal(t, 5, fields[2].Offset)
	assert.Equal(t, 3, fields[2].Size)
}

func TestBcuOperPrmInfo(t *testing.T) {
	// Based on struct_layout_report.txt expectations
	structs := map[string][]rawField{
		"BcuOperPrmInfo": {
			{Name: "BcuBattU", CType: "uint16_t"},
			{Name: "BcuBattI", CType: "uint16_t"},
			{Name: "BcuSocDisp", CType: "uint16_t"},
			{Name: "reserved0", CType: "uint8_t", ArrayLen: 30},
			{Name: "BCUTotalChrgCpDisp", CType: "uint32_t"},
			{Name: "reserved1", CType: "uint8_t", ArrayLen: 12},
			{Name: "BCUTotalChrgCpDisp1", CType: "uint32_t"},
			{Name: "reserved2", CType: "uint8_t", ArrayLen: 2},
			{Name: "BcuBattRatedAh", CType: "uint16_t"},
		},
	}

	_, totalSize, maxAlign, err := layoutStruct("BcuOperPrmInfo", structs, nil)
	assert.NoError(t, err)
	assert.Equal(t, 60, totalSize)
	assert.Equal(t, 4, maxAlign)
}

func TestEPSBasInfo(t *testing.T) {
	// Based on struct_layout_report.txt expectations (Total=40B before secure wrapper)
	structs := map[string][]rawField{
		"EPSBasInfo": {
			{Name: "TgtMotorMotorTorq", CType: "uint16_t"},
			{Name: "MeasuredTorsionBarTorque", CType: "uint16_t"},
			{Name: "HandwheelRelang", CType: "uint16_t"},
			{Name: "SasSteerAg", CType: "int16_t"},
			{Name: "ElePowConsump", CType: "uint8_t"},
			{Name: "ElePowVolt", CType: "uint8_t"},
			{Name: "TgtMotorTorqValid", CType: "uint8_t"},
			{Name: "HandwheelRelangValid", CType: "uint8_t"},
			{Name: "MeasuredTorsionBarTorqValid", CType: "uint8_t"},
			{Name: "SteerAgRate", CType: "uint8_t"},
			{Name: "SteerAgSensFilr", CType: "uint8_t"},
			{Name: "SasSteerAgVld", CType: "uint8_t"},
			{Name: "LostComFltSts1", CType: "uint8_t"},
			{Name: "LostComFltSts2", CType: "uint8_t"},
			{Name: "reserved1", CType: "uint32_t"},
			{Name: "reserved2", CType: "uint32_t"},
			{Name: "reserved3", CType: "uint32_t"},
			{Name: "reserved4", CType: "uint32_t"},
			{Name: "reserved5", CType: "uint32_t"},
		},
	}

	fields, totalSize, maxAlign, err := layoutStruct("EPSBasInfo", structs, nil)
	assert.NoError(t, err)
	assert.Equal(t, 40, totalSize)
	assert.Equal(t, 4, maxAlign)

	// Since LostComFltSts2 is at offset 17 (10 bytes of padding+fields from uint16=8)
	// uint16 x 4 = 8.
	// uint8 x 10 = 10. Offset so far = 18.
	// Wait, at offset 18, we need 32-bit (align 4). So pad to 20. (2 bytes pad).
	// Let's check:
	foundPad := false
	for _, f := range fields {
		if f.Name == "<pad 2B>" {
			foundPad = true
			assert.Equal(t, 18, f.Offset)
		}
	}
	assert.True(t, foundPad, "Expected a 2B pad at offset 18")
}
