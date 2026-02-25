package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

type SummaryEntry struct {
	DeclaredSize int      `json:"declared_size"`
	Count        int      `json:"count"`
	Samples      []string `json:"samples"`
}

type SummaryFile struct {
	TotalUniqueServices int                     `json:"total_unique_services"`
	Services            map[string]SummaryEntry `json:"services"`
}

func LoadSummary(path string) (SummaryFile, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return SummaryFile{}, err
	}
	var sf SummaryFile
	if err := json.Unmarshal(data, &sf); err != nil {
		return SummaryFile{}, err
	}
	return sf, nil
}

func ValidateSizes(summary SummaryFile, schemas map[string]*StructDef, computedSizes map[string]int, skipped map[string]bool) (bool, error) {
	matches := 0
	mismatches := 0
	unresolved := 0

	fmt.Println("\n--- Validation Report ---")

	for key, sd := range schemas {
		if skipped[key] {
			continue // skip explicitly marked entries like sequences
		}

		expectedEntry, ok := summary.Services[key]
		if !ok {
			// Try with _Proxy suffix for service part
			parts := strings.SplitN(key, ".", 2)
			if len(parts) == 2 {
				proxyKey := parts[0] + "_Proxy." + parts[1]
				expectedEntry, ok = summary.Services[proxyKey]
			}
		}

		if !ok {
			fmt.Printf("WARN:  %-50s | Not in summary file\n", key)
			unresolved++
			continue
		}

		computed := computedSizes[key]
		expected := expectedEntry.DeclaredSize

		if sd.Wrapper == "secure" {
			expected -= 4 // wrapper consumes 4 bytes before the payload
		}

		if computed == expected {
			matches++
		} else {
			mismatches++
			fmt.Printf("FAIL:  %-50s | computed=%d, expected=%d (diff=%+d)\n", key, computed, expected, computed-expected)
		}
	}

	fmt.Printf("\nMatched: %d | Mismatched: %d | Unresolved: %d | Skipped: %d\n", matches, mismatches, unresolved, len(skipped))
	
	if mismatches > 0 {
		return false, fmt.Errorf("Found %d size mismatches", mismatches)
	}
	return true, nil
}
