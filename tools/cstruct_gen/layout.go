package main

import (
	"fmt"
	"regexp"
	"strconv"
)

// rawField represents an un-laid-out parsed C field.
type rawField struct {
	Name     string
	CType    string
	ArrayLen int // 0 if not an array
}

// layoutField represents a pre-computed struct field's layout in memory
type layoutField struct {
	Name     string
	CType    string
	ArrayLen int
	Offset   int
	Size     int
	Align    int
}

var arrayPattern = regexp.MustCompile(`^(\w+)\[(\d+)\]$`)

// sizeAlignOf returns the byte size and alignment for a given C type.
// It matches the ARM ABI (alignment capped at 4 bytes).
// structs map is used for recursively determining the size of nested structs.
func sizeAlignOf(ctype string, structs map[string][]rawField, resolved map[string]struct{ size, align int }) (size int, align int) {
	if resolved == nil {
		resolved = make(map[string]struct{ size, align int })
	}
	if res, ok := resolved[ctype]; ok {
		return res.size, res.align
	}

	switch ctype {
	case "uint8_t", "int8_t", "char", "bool", "_Bool":
		return 1, 1
	case "uint16_t", "int16_t", "short":
		return 2, 2
	case "uint32_t", "int32_t", "float", "int", "enum":
		return 4, 4
	case "uint64_t", "int64_t", "double", "char*": // capped at 4 for ARM
		return 8, 4
	}

	// Array check e.g. "uint8_t[512]"
	if matches := arrayPattern.FindStringSubmatch(ctype); matches != nil {
		elemType := matches[1]
		count, _ := strconv.Atoi(matches[2])
		elemSize, elemAlign := sizeAlignOf(elemType, structs, resolved)
		return elemSize * count, elemAlign
	}

	// Nested struct check
	if _, ok := structs[ctype]; ok {
		// Calculate nested struct
		_, sz, al, err := layoutStruct(ctype, structs, resolved)
		if err == nil {
			resolved[ctype] = struct{ size, align int }{sz, al}
			return sz, al
		}
	}

	// Fallback to uint32 (4, 4) if unknown (common for enums or pointers).
	return 4, 4
}

func alignUp(offset, alignment int) int {
	if alignment <= 0 {
		return offset
	}
	rem := offset % alignment
	if rem == 0 {
		return offset
	}
	return offset + (alignment - rem)
}

// layoutStruct computes the ABI-compliant layout for a given struct name.
// It inserts padding gap fields as necessary.
func layoutStruct(name string, structs map[string][]rawField, resolved map[string]struct{ size, align int }) ([]layoutField, int, int, error) {
	if resolved == nil {
		resolved = make(map[string]struct{ size, align int })
	}

	rawFields, ok := structs[name]
	if !ok {
		return nil, 0, 0, fmt.Errorf("struct %s not found", name)
	}

	var fields []layoutField
	offset := 0
	maxAlign := 1

	for _, rf := range rawFields {
		sz, al := sizeAlignOf(rf.CType, structs, resolved)
		totalSz := sz
		if rf.ArrayLen > 0 {
			totalSz = sz * rf.ArrayLen
		}

		paddedOffset := alignUp(offset, al)
		if paddedOffset > offset {
			padSize := paddedOffset - offset
			fields = append(fields, layoutField{
				Name:   fmt.Sprintf("<pad %dB>", padSize),
				CType:  "",
				Offset: offset,
				Size:   padSize,
				Align:  1,
			})
		}

		fields = append(fields, layoutField{
			Name:     rf.Name,
			CType:    rf.CType,
			ArrayLen: rf.ArrayLen,
			Offset:   paddedOffset,
			Size:     totalSz,
			Align:    al,
		})

		offset = paddedOffset + totalSz
		if al > maxAlign {
			maxAlign = al
		}
	}

	// Trailing padding to reach maxAlign
	total := alignUp(offset, maxAlign)
	if total > offset {
		padSize := total - offset
		fields = append(fields, layoutField{
			Name:   fmt.Sprintf("<trail_pad %dB>", padSize),
			CType:  "",
			Offset: offset,
			Size:   padSize,
			Align:  1,
		})
	}

	return fields, total, maxAlign, nil
}
