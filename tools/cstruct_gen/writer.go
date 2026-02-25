package main

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

type FieldDef struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type StructDef struct {
	Wrapper string     `json:"wrapper,omitempty"`
	Fields  []FieldDef `json:"fields"`
}

type SchemaFile struct {
	Endian  string               `json:"endian"`
	Structs map[string]*StructDef `json:"structs"`
}

// WriteSchema writes the generated ABI schema to the provided writer
func WriteSchema(out io.Writer, entries map[string]*StructDef) error {
	sf := SchemaFile{
		Endian:  "little",
		Structs: entries,
	}

	enc := json.NewEncoder(out)
	enc.SetIndent("", "  ")
	return enc.Encode(sf)
}

// mapCTypeToABI maps C primitive types to ABI schema types
func mapCTypeToABI(ctype string) string {
	if matches := arrayPattern.FindStringSubmatch(ctype); matches != nil {
		baseType := mapCTypeToABI(matches[1])
		return fmt.Sprintf("%s[%s]", baseType, matches[2])
	}
	
	switch ctype {
	case "uint8_t", "int8_t", "char", "bool", "_Bool":
		return "uint8"
	case "uint16_t", "short", "int16_t":
		return "uint16"
	case "uint32_t", "int", "enum", "int32_t":
		return "uint32"
	case "uint64_t", "int64_t":
		return "uint64"
	case "float":
		return "float"
	case "double":
		return "double"
	case "char*":
		return "string"
	}

	return "uint32" // default unknown
}

// flattenFields converts layout fields into a flat list of ABI FieldDefs
func flattenFields(prefix string, lf layoutField, structs map[string][]rawField, resolved map[string]struct{ size, align int }, padCounter *int, inSecureWrapper bool) []FieldDef {
	if strings.HasPrefix(lf.Name, "<pad") || strings.HasPrefix(lf.Name, "<trail_pad") {
		*padCounter++
		return []FieldDef{{
			Name: fmt.Sprintf("padding_%d", *padCounter),
			Type: fmt.Sprintf("uint8[%d]", lf.Size),
		}}
	}

	// Secure wrapper omit logic
	if inSecureWrapper {
		name := strings.ToLower(lf.Name)
		if name == "crc" || name == "counter" || strings.Contains(name, "crc_") || strings.Contains(name, "counter_") {
			return nil
		}
	}

	// Check if nested struct
	_, isStruct := structs[lf.CType]
	if isStruct {
		if lf.ArrayLen > 0 {
			// Array of structs is falling back to uint8 byte array.
			return []FieldDef{{
				Name: prefix + lf.Name,
				Type: fmt.Sprintf("uint8[%d]", lf.Size),
			}}
		}

		nestedLayout, _, _, err := layoutStruct(lf.CType, structs, resolved)
		if err == nil {
			var flat []FieldDef
			for _, nlf := range nestedLayout {
				flat = append(flat, flattenFields(prefix+lf.Name+"_", nlf, structs, resolved, padCounter, inSecureWrapper)...)
			}
			return flat
		}
	}

	// Normal field
	if lf.ArrayLen > 0 {
		return []FieldDef{{
			Name: prefix + lf.Name,
			Type: fmt.Sprintf("uint8[%d]", lf.Size),
		}}
	}
	return []FieldDef{{
		Name: prefix + lf.Name,
		Type: mapCTypeToABI(lf.CType),
	}}
}

// BuildStructDef translates ABI layout fields into the final schema definition
func BuildStructDef(layout []layoutField, target string, structs map[string][]rawField, resolved map[string]struct{ size, align int }) *StructDef {
	sd := &StructDef{}

	if strings.HasPrefix(target, "carControl_Secure_") {
		sd.Wrapper = "secure"
	}

	var fields []FieldDef
	padCount := 0

	for _, lf := range layout {
		fields = append(fields, flattenFields("", lf, structs, resolved, &padCount, sd.Wrapper == "secure")...)
	}

	// Merge consecutive padding fields
	merged := make([]FieldDef, 0, len(fields))
	var currentPadSize int
	
	for _, f := range fields {
		if strings.HasPrefix(f.Name, "padding_") {
			var size int
			fmt.Sscanf(f.Type, "uint8[%d]", &size)
			currentPadSize += size
		} else {
			if currentPadSize > 0 {
				padCount++
				merged = append(merged, FieldDef{
					Name: fmt.Sprintf("padding_%d", padCount),
					Type: fmt.Sprintf("uint8[%d]", currentPadSize),
				})
				currentPadSize = 0
			}
			merged = append(merged, f)
		}
	}

	if currentPadSize > 0 {
		padCount++
		merged = append(merged, FieldDef{
			Name: fmt.Sprintf("padding_%d", padCount),
			Type: fmt.Sprintf("uint8[%d]", currentPadSize),
		})
	}

	sd.Fields = merged
	return sd
}
