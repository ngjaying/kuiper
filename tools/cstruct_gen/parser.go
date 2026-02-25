package main

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

var (
	// Matches /* ... */ and // ... 
	commentRe = regexp.MustCompile(`(?s)/\*.*?\*/|//[^\n]*`)
	// Extract struct definitions
	structRe = regexp.MustCompile(`(?s)typedef\s+struct\s*\w*\s*\{([^}]*)\}\s*(\w+)\s*;`)
	// Extract field definition. Handles types starting with 'struct ' and arrays, and char*.
	fieldRe = regexp.MustCompile(`(?:struct\s+)?([A-Za-z0-9_:]+(?:\s*\*)?)\s+([A-Za-z0-9_]+)(?:\[(\d+)\])?\s*;`)
	varRe = regexp.MustCompile(`(?m)\s*(struct\s+[A-Za-z0-9_]+|[A-Za-z0-9_]+)\s*(\*)?\s+([A-Za-z0-9_]+)\s*;`)
)

func ParseHeadersDir(dir string) (map[string][]rawField, map[string]string, error) {
	structs := make(map[string][]rawField)
	varMap := make(map[string]string)

	matches, err := filepath.Glob(filepath.Join(dir, "*.h"))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to glob header dir %q: %w", dir, err)
	}

	for i, path := range matches {
		if i%10 == 0 {
			fmt.Printf("   Parsing header %d/%d (%s)...\n", i, len(matches), filepath.Base(path))
		}
		content, err := os.ReadFile(path)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to read %s: %w", path, err)
		}

		text := string(content)
		text = commentRe.ReplaceAllString(text, "")

		// 1. Extract structs
		structMatches := structRe.FindAllStringSubmatch(text, -1)
		for _, m := range structMatches {
			body := m[1]
			name := m[2]

			var fields []rawField
			fieldMatches := fieldRe.FindAllStringSubmatch(body, -1)
			for _, fm := range fieldMatches {
				ctype := strings.TrimSpace(fm[1])
				ctype = strings.ReplaceAll(ctype, " ", "")
				
				fname := fm[2]
				arrayLen := 0
				if fm[3] != "" {
					fmt.Sscanf(fm[3], "%d", &arrayLen)
				}

				fields = append(fields, rawField{
					Name:     fname,
					CType:    ctype,
					ArrayLen: arrayLen,
				})
			}
			structs[name] = fields
		}

		// 2. Extract variable declarations to map variable name -> base type
		varMatches := varRe.FindAllStringSubmatch(text, -1)
		for _, vm := range varMatches {
			baseType := strings.TrimSpace(vm[1])
			baseType = strings.TrimPrefix(baseType, "struct ")
			ptr := strings.TrimSpace(vm[2])
			varName := strings.TrimSpace(vm[3])

			if ptr != "" {
				baseType = baseType + "*"
			}
			varMap[varName] = baseType
		}
	}

	return structs, varMap, nil
}
