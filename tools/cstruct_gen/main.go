package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
)

type stringSlice []string

func (s *stringSlice) String() string { return strings.Join(*s, ", ") }
func (s *stringSlice) Set(v string) error {
	for _, part := range strings.Split(v, ",") {
		if part != "" {
			*s = append(*s, strings.TrimSpace(part))
		}
	}
	return nil
}

// computeSchemaSize calculates the expected decoded size of the final StructDef
func computeSchemaSize(sd *StructDef) int {
	total := 0
	for _, f := range sd.Fields {
		var arrSize int
		if n, _ := fmt.Sscanf(f.Type, "uint8[%d]", &arrSize); n == 1 {
			total += arrSize
		} else if f.Type == "uint8" || f.Type == "int8" {
			total += 1
		} else if f.Type == "uint16" || f.Type == "int16" {
			total += 2
		} else if f.Type == "uint32" || f.Type == "int32" || f.Type == "float" {
			total += 4
		} else if f.Type == "uint64" || f.Type == "int64" || f.Type == "double" {
			total += 8
		} else if f.Type == "string" {
			total += 8
		}
	}
	return total
}

func main() {
	var csvFiles stringSlice
	var headerDir string
	var summaryFile string
	var outFile string
	var validate bool

	flag.Var(&csvFiles, "csv", "Path to CSV file (can be specified multiple times)")
	flag.StringVar(&headerDir, "hdir", "", "Directory containing .h header files")
	flag.StringVar(&summaryFile, "summary", "", "Optional 123_summary.json for size validation")
	flag.StringVar(&outFile, "out", "abi_schema.json", "Output schema .json path")
	flag.BoolVar(&validate, "validate", false, "Exit non-zero if validation fails (requires -summary)")
	flag.Parse()

	if len(csvFiles) == 0 || headerDir == "" {
		log.Fatal("Usage: cstruct_gen -csv <path> -hdir <dir> [-out <file>]")
	}

	fmt.Println("1. Read CSVs to get authoritative target list and data types")
	serviceSet, signals, datatypes, err := ParseCSVs(csvFiles)
	if err != nil {
		fmt.Printf("ERROR: Failed to parse CSVs: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("   Found %d target services in CSVs\n", len(serviceSet))

	fmt.Println("2. Parsing Headers Directory...")
	structs, varMap, err := ParseHeadersDir(headerDir)
	if err != nil {
		fmt.Printf("ERROR: Failed to parse header dir: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("   Extracted %d struct definitions\n", len(structs))
	fmt.Printf("   Extracted %d message payload bindings\n", len(varMap))


	fmt.Println("3. Resolving & Generating Schema...")
	resolvedLayouts := make(map[string]struct{ size, align int })
	entries := make(map[string]*StructDef)
	computedSizes := make(map[string]int)
	skipped := make(map[string]bool)

	for canonicalKey := range serviceSet {
		target, kind := ResolveStructName(canonicalKey, varMap, structs, datatypes[canonicalKey])
		
		switch kind {
		case kindSequenceString:
			fmt.Printf("INFO: Converting %s sequence into string based on CSV definition\n", canonicalKey)
			fieldName := "value"
			if sigs := signals[canonicalKey]; len(sigs) > 0 {
				fieldName = sigs[0]
			}
			sd := &StructDef{
				Fields: []FieldDef{{Name: fieldName, Type: "dds_sequence_string"}},
			}
			entries[canonicalKey] = sd
			skipped[canonicalKey] = true

		case kindString:
			fieldName := "value"
			if sigs := signals[canonicalKey]; len(sigs) > 0 {
				fieldName = sigs[0]
			}
			sd := &StructDef{
				Fields: []FieldDef{{Name: fieldName, Type: "string"}},
			}
			entries[canonicalKey] = sd
			computedSizes[canonicalKey] = -1
			skipped[canonicalKey] = true

		case kindSequence:
			// Emit bytes type, skip size check
			fmt.Printf("WARN: Skipping exact layout for %s (%s) — dds_sequence (CDR)\n", canonicalKey, target)
			fieldName := "data"
			if sigs := signals[canonicalKey]; len(sigs) > 0 {
				fieldName = sigs[0]
			}
			sd := &StructDef{
				Fields: []FieldDef{{Name: fieldName, Type: "dds_sequence_bytes"}},
			}
			entries[canonicalKey] = sd
			skipped[canonicalKey] = true
			
		case kindPrimitive:
			fieldName := "value"
			if sigs := signals[canonicalKey]; len(sigs) > 0 {
				fieldName = sigs[0]
			}
			sd := &StructDef{
				Fields: []FieldDef{{Name: fieldName, Type: mapCTypeToABI(target)}},
			}
			entries[canonicalKey] = sd
			computedSizes[canonicalKey] = computeSchemaSize(sd)

		case kindStruct:
			layout, _, _, err := layoutStruct(target, structs, resolvedLayouts)
			if err != nil {
				fmt.Printf("ERROR: Failed to layout %s for %s: %v\n", target, canonicalKey, err)
				skipped[canonicalKey] = true
				continue
			}
			
			sd := BuildStructDef(layout, target, structs, resolvedLayouts)
			entries[canonicalKey] = sd
			computedSizes[canonicalKey] = computeSchemaSize(sd)

		case kindUnknown:
			fmt.Printf("ERROR: Unknown struct %s for %s. Skipping (please review mapping or headers).\n", target, canonicalKey)
			computedSizes[canonicalKey] = -1
			skipped[canonicalKey] = true
		}
	}

	fmt.Printf("   Generated schema with %d items\n", len(entries))

	if summaryFile != "" {
		fmt.Println("4. Validating Sizes...")
		summary, err := LoadSummary(summaryFile)
		if err != nil {
			log.Fatalf("Summary error: %v", err)
		}

		ok, err := ValidateSizes(summary, entries, computedSizes, skipped)
		if validate && !ok {
			log.Fatalf("Validation failed: %v", err)
		}
	}

	fmt.Println("5. Writing output to", outFile)
	f, err := os.Create(outFile)
	if err != nil {
		log.Fatalf("File creation error: %v", err)
	}
	defer f.Close()

	if err := WriteSchema(f, entries); err != nil {
		log.Fatalf("WriteSchema error: %v", err)
	}

	fmt.Println("Done.")
}
