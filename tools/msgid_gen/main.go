// Package main implements the msgid_gen tool that generates msg_id mapping files
// from all.csv / all2.csv signal definition files.
//
// The mapping file is shared between the external Data Proxy and eKuiper:
//   - Proxy uses it to populate the msg_id field in GBF frame headers.
//   - eKuiper uses it to dispatch frames to the correct decoder.
//
// Usage:
//
//	go run tools/msgid_gen/main.go -csv requirement/all.csv -csv requirement/all2.csv -o msg_id_mapping.json
package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"os"
	"sort"
	"strings"
	"time"
	"unicode/utf8"
)

// MappingEntry represents a single msg_id mapping entry.
type MappingEntry struct {
	MsgID  uint16 `json:"msg_id"`
	Type   int    `json:"type"`
	Key    string `json:"key"`
	Source string `json:"source,omitempty"` // which CSV file this came from
}

// MappingOutput is the top-level output structure.
type MappingOutput struct {
	Version   string          `json:"version"`
	Generated string          `json:"generated"`
	Entries   []*MappingEntry `json:"entries"`
	ByType    map[string]struct {
		Description string `json:"description"`
		Count       int    `json:"count"`
	} `json:"by_type"`
}

// csvFormat describes the column layout of a CSV file.
type csvFormat struct {
	dataSourceCol int // 0-indexed column for data source (e.g., "Service", "CanTransfDDS", "Proto")
	serviceCol    int // 0-indexed column for service name / CAN segment / Proto service
	methodCol     int // 0-indexed column for method name / CAN ID / Proto topic
}

// knownFormats maps header signatures to CSV formats.
// all.csv has 32 columns with data source at col 15 (0-indexed).
// all2.csv has 24 columns with data source at col 5.
var knownFormats = []struct {
	headerCheck func([]string) bool
	format      csvFormat
}{
	{
		// all.csv: col 0 = "数据项英文名称", data source at col 15
		headerCheck: func(row []string) bool {
			if len(row) < 20 {
				return false
			}
			return strings.Contains(row[0], "数据项英文名称")
		},
		format: csvFormat{dataSourceCol: 15, serviceCol: 17, methodCol: 19},
	},
	{
		// all2.csv: col 0 = "ID", col 1 = "事件名称", data source at col 5
		headerCheck: func(row []string) bool {
			if len(row) < 10 {
				return false
			}
			return strings.Contains(row[0], "ID") && strings.Contains(row[1], "事件名称")
		},
		format: csvFormat{dataSourceCol: 5, serviceCol: 7, methodCol: 9},
	},
}

type stringSlice []string

func (s *stringSlice) String() string { return strings.Join(*s, ", ") }
func (s *stringSlice) Set(v string) error {
	*s = append(*s, v)
	return nil
}

func main() {
	var csvFiles stringSlice
	var outputFile string

	flag.Var(&csvFiles, "csv", "Path to a CSV file (can be specified multiple times)")
	flag.StringVar(&outputFile, "o", "msg_id_mapping.json", "Output mapping file path")
	flag.Parse()

	if len(csvFiles) == 0 {
		log.Fatal("Usage: msgid_gen -csv <file1.csv> [-csv <file2.csv>] [-o output.json]")
	}

	seen := make(map[string]*MappingEntry) // key -> entry, for deduplication
	var allEntries []*MappingEntry

	for _, f := range csvFiles {
		entries, err := processCSV(f)
		if err != nil {
			log.Fatalf("Error processing %s: %v", f, err)
		}
		for _, e := range entries {
			if existing, ok := seen[e.Key]; ok {
				if existing.MsgID != e.MsgID || existing.Type != e.Type {
					log.Fatalf("Conflict for key %q: msg_id %d (from %s) vs %d (from %s)",
						e.Key, existing.MsgID, existing.Source, e.MsgID, e.Source)
				}
				continue // deduplicate
			}
			seen[e.Key] = e
			allEntries = append(allEntries, e)
		}
	}

	// Resolve msg_id collisions within each type using linear probing.
	// Sort entries by key first to ensure deterministic assignment.
	sort.Slice(allEntries, func(i, j int) bool {
		if allEntries[i].Type != allEntries[j].Type {
			return allEntries[i].Type < allEntries[j].Type
		}
		return allEntries[i].Key < allEntries[j].Key
	})

	byTypeMsgID := make(map[int]map[uint16]string) // type -> msg_id -> key
	collisions := 0
	for _, e := range allEntries {
		if byTypeMsgID[e.Type] == nil {
			byTypeMsgID[e.Type] = make(map[uint16]string)
		}
		original := e.MsgID
		for {
			if _, ok := byTypeMsgID[e.Type][e.MsgID]; !ok {
				break
			}
			e.MsgID++ // linear probe
		}
		if e.MsgID != original {
			collisions++
			log.Printf("Resolved collision: %q hash %d -> %d (type=%d)", e.Key, original, e.MsgID, e.Type)
		}
		byTypeMsgID[e.Type][e.MsgID] = e.Key
	}

	// Build output
	output := MappingOutput{
		Version:   "1.0",
		Generated: time.Now().UTC().Format(time.RFC3339),
		Entries:   allEntries,
		ByType: map[string]struct {
			Description string `json:"description"`
			Count       int    `json:"count"`
		}{
			"0": {Description: "CAN/DBC", Count: len(byTypeMsgID[0])},
			"1": {Description: "Proto", Count: len(byTypeMsgID[1])},
			"2": {Description: "Service/CStruct", Count: len(byTypeMsgID[2])},
		},
	}

	data, err := json.MarshalIndent(output, "", "  ")
	if err != nil {
		log.Fatalf("JSON marshal error: %v", err)
	}

	if err := os.WriteFile(outputFile, data, 0644); err != nil {
		log.Fatalf("Write error: %v", err)
	}

	fmt.Printf("Generated %s: %d entries (Service: %d, Proto: %d)\n",
		outputFile, len(allEntries),
		len(byTypeMsgID[2]), len(byTypeMsgID[1]))
}

func processCSV(path string) ([]*MappingEntry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// Strip BOM if present
	r := stripBOM(f)
	reader := csv.NewReader(r)
	reader.LazyQuotes = true
	reader.FieldsPerRecord = -1 // variable field count

	// Read header to detect format
	header, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("failed to read header: %w", err)
	}

	var format *csvFormat
	for _, kf := range knownFormats {
		if kf.headerCheck(header) {
			f := kf.format
			format = &f
			break
		}
	}
	if format == nil {
		return nil, fmt.Errorf("unrecognized CSV format (header[0]=%q, cols=%d)", header[0], len(header))
	}

	seen := make(map[string]bool)
	var entries []*MappingEntry

	basename := path
	if idx := strings.LastIndex(path, "/"); idx >= 0 {
		basename = path[idx+1:]
	}

	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue // skip malformed rows
		}
		if len(row) <= format.methodCol {
			continue
		}

		dataSource := strings.TrimSpace(row[format.dataSourceCol])
		service := strings.TrimSpace(row[format.serviceCol])
		method := strings.TrimSpace(row[format.methodCol])

		if service == "" || method == "" || service == "/" || method == "/" {
			continue
		}

		var entry *MappingEntry

		switch dataSource {
		case "Service":
			// Service: msg_id = FNV-1a hash mod 65536
			key := service + "." + method
			entry = &MappingEntry{
				MsgID:  fnv1aU16(key),
				Type:   2,
				Key:    key,
				Source: basename,
			}

		case "Proto":
			// Proto: msg_id = FNV-1a hash mod 65536
			key := service + "." + method
			entry = &MappingEntry{
				MsgID:  fnv1aU16(key),
				Type:   1,
				Key:    key,
				Source: basename,
			}

		default:
			continue // skip unknown data sources
		}

		if seen[entry.Key] {
			continue
		}
		seen[entry.Key] = true
		entries = append(entries, entry)
	}

	return entries, nil
}

// fnv1aU16 computes FNV-1a 32-bit hash and returns the lower 16 bits.
func fnv1aU16(s string) uint16 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return uint16(h.Sum32() & 0xFFFF)
}

// stripBOM returns a reader that skips the UTF-8 BOM if present.
func stripBOM(r io.Reader) io.Reader {
	buf := make([]byte, 3)
	n, err := r.Read(buf)
	if err != nil {
		return r
	}
	if n >= 3 && buf[0] == 0xEF && buf[1] == 0xBB && buf[2] == 0xBF {
		// BOM found, skip it
		return r
	}
	// No BOM, check if partial UTF-8 rune
	if n > 0 && utf8.Valid(buf[:n]) {
		return io.MultiReader(strings.NewReader(string(buf[:n])), r)
	}
	return io.MultiReader(strings.NewReader(string(buf[:n])), r)
}
