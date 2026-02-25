package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strings"
	"unicode/utf8"
)

type csvFormat struct {
	pattern   []string
	svcIdx    int
	methIdx   int
	sigIdx    int
	dtIdx     int // typically 信号数据类型 (col 21)
	baseDtIdx int // typically 数据项类型 (col 2)
	srcIdx    int // typically 数据来源 (col 16 in all, col 6 in all2)
}

var knownFormats = []struct {
	headerCheck func([]string) bool
	format      csvFormat
}{
	{
		// all.csv
		headerCheck: func(row []string) bool {
			return len(row) >= 20 && strings.Contains(row[0], "数据项英文名称")
		},
		format: csvFormat{
			pattern:   []string{"数据项英文名称", "数据项中文名称", "数据项类型", "数据项长度", "数据项精度", "数据项单位", "数据项描述", "数据项值域", "数据项值域描述", "数据项值域类型", "数据项值域长度", "数据项值域精度", "数据项值域单位", "数据项值域描述", "数据项值域类型", "数据项值域长度", "数据项值域精度", "数据项值域单位", "数据项值域描述", "数据项值域类型", "数据项值域长度", "数据项值域精度", "数据项值域单位", "数据项值域描述"},
			svcIdx:    17,
			methIdx:   19,
			sigIdx:    21, // "参数名称(英文)"
			dtIdx:     0,  // "数据项英文名称" (often contains specific type info)
			baseDtIdx: 2,  // "数据项类型" (uint8, uint32, etc.)
			srcIdx:    15, // "数据来源"
		},
	},
	{
		// all2.csv
		headerCheck: func(row []string) bool {
			return len(row) >= 10 && strings.Contains(row[0], "ID") && strings.Contains(row[1], "事件名称")
		},
		format: csvFormat{
			pattern:   []string{"项目", "所属Domain", "Service", "Signal", "Method"},
			svcIdx:    7,
			methIdx:   9,
			sigIdx:    11, // "参数名称(英文)"
			dtIdx:     2,  // "数据项英文名称"
			baseDtIdx: -1,
			srcIdx:    5,  // "数据来源"
		},
	},
}

// ParseCSVs reads the authority CSV files and extracts the target service set,
// signal names per service, and signal data types per service.
func ParseCSVs(paths []string) (serviceSet map[string]bool, signals map[string][]string, datatypes map[string]map[string]string, err error) {
	serviceSet = make(map[string]bool)
	signals = make(map[string][]string)
	datatypes = make(map[string]map[string]string)

	for _, path := range paths {
		f, err := os.Open(path)
		if err != nil {
			return nil, nil, nil, err
		}

		r := stripBOM(f)
		reader := csv.NewReader(r)
		reader.LazyQuotes = true
		reader.FieldsPerRecord = -1

		header, err := reader.Read()
		if err != nil {
			f.Close()
			return nil, nil, nil, fmt.Errorf("failed to read header for %s: %w", path, err)
		}

		var fmtIdx = -1
		for i, kf := range knownFormats {
			if kf.headerCheck(header) {
				fmtIdx = i
				break
			}
		}

		if fmtIdx == -1 {
			f.Close()
			return nil, nil, nil, fmt.Errorf("unrecognized CSV format in %s", path)
		}

		for {
			record, err := reader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				continue // skip malformed rows
			}

			// Only add if we successfully mapped to a known format
			if fmtIdx != -1 {
				svcCol := knownFormats[fmtIdx].format.svcIdx
				methCol := knownFormats[fmtIdx].format.methIdx
				sigCol := knownFormats[fmtIdx].format.sigIdx
				dtCol := knownFormats[fmtIdx].format.dtIdx
				bdtCol := knownFormats[fmtIdx].format.baseDtIdx
				srcCol := knownFormats[fmtIdx].format.srcIdx

				// Ensure record is long enough
				if len(record) > svcCol && len(record) > methCol && len(record) > sigCol && len(record) > dtCol && len(record) > srcCol {
					// Verify this is a Service message, not CAN/ETH
					if strings.TrimSpace(record[srcCol]) != "Service" {
						continue
					}

					svc := strings.TrimSpace(record[svcCol])
					meth := strings.TrimSpace(record[methCol])
					sig := strings.TrimSpace(record[sigCol])
					
					// Combine the dt entries for better string matching in ResolveStructName
					dt := strings.TrimSpace(record[dtCol])
					if bdtCol != -1 && len(record) > bdtCol {
						baseDt := strings.TrimSpace(record[bdtCol])
						dt = dt + "|" + baseDt
					}

					if svc != "" && meth != "" {
						// Key is exactly as in the CSV: ServiceName.MethodName
						canonicalKey := fmt.Sprintf("%s.%s", svc, meth)
						serviceSet[canonicalKey] = true
						if sig != "" {
							signals[canonicalKey] = append(signals[canonicalKey], sig)
							if datatypes[canonicalKey] == nil {
								datatypes[canonicalKey] = make(map[string]string)
							}
							
							// If we found the same signal again, append to its dt string
							existingDt := datatypes[canonicalKey][sig]
							if existingDt == "" {
								datatypes[canonicalKey][sig] = dt
							} else if !strings.Contains(existingDt, dt) {
								datatypes[canonicalKey][sig] = existingDt + "|" + dt
							}
						}
					}
				}
			}
		}
		f.Close()
	}

	return serviceSet, signals, datatypes, nil
}

// stripBOM extracted from msgid_gen
func stripBOM(r io.Reader) io.Reader {
	buf := make([]byte, 3)
	n, err := r.Read(buf)
	if err != nil {
		return r
	}
	if n >= 3 && buf[0] == 0xEF && buf[1] == 0xBB && buf[2] == 0xBF {
		return r
	}
	if n > 0 && utf8.Valid(buf[:n]) {
		return io.MultiReader(strings.NewReader(string(buf[:n])), r)
	}
	return io.MultiReader(strings.NewReader(string(buf[:n])), r)
}
