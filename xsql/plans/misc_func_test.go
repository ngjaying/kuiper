package plans

import (
	"encoding/json"
	"engine/xsql"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func TestHashFunc_Apply1(t *testing.T) {
	var tests = []struct {
		sql  string
		data *xsql.Tuple
		result []map[string]interface{}
	}{
		{
			sql: "SELECT md5(a) AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a" : "The quick brown fox jumps over the lazy dog",
					"b" : "myb",
					"c" : "myc",
				},
			},
			result: []map[string]interface{}{{
				"a": strings.ToLower("9E107D9D372BB6826BD81D3542A419D6"),
			}},
		},
		{
			sql: "SELECT sha1(a) AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a" : "The quick brown fox jumps over the lazy dog",
					"b" : "myb",
					"c" : "myc",
				},
			},
			result: []map[string]interface{}{{
				"a": strings.ToLower("2FD4E1C67A2D28FCED849EE1BB76E7391B93EB12"),
			}},
		},
		{
			sql: "SELECT sha256(a) AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a" : "The quick brown fox jumps over the lazy dog",
					"b" : "myb",
					"c" : "myc",
				},
			},
			result: []map[string]interface{}{{
				"a": strings.ToLower("D7A8FBB307D7809469CA9ABCB0082E4F8D5651E46D3CDB762D02D0BF37C9E592"),
			}},
		},
		{
			sql: "SELECT sha384(a) AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a" : "The quick brown fox jumps over the lazy dog",
					"b" : "myb",
					"c" : "myc",
				},
			},
			result: []map[string]interface{}{{
				"a": strings.ToLower("CA737F1014A48F4C0B6DD43CB177B0AFD9E5169367544C494011E3317DBF9A509CB1E5DC1E85A941BBEE3D7F2AFBC9B1"),
			}},
		},
		{
			sql: "SELECT sha512(a) AS a FROM test",
			data: &xsql.Tuple{
				Emitter: "test",
				Message: xsql.Message{
					"a" : "The quick brown fox jumps over the lazy dog",
					"b" : "myb",
					"c" : "myc",
				},
			},
			result: []map[string]interface{}{{
				"a": strings.ToLower("07E547D9586F6A73F73FBAC0435ED76951218FB7D0C8D788A309D785436BBB642E93A252A954F23912547D1E8A3B5ED6E1BFD7097821233FA0538F3DB854FEE6"),
			}},
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	for i, tt := range tests {
		stmt, err := xsql.NewParser(strings.NewReader(tt.sql)).Parse()
		if err != nil || stmt == nil {
			t.Errorf("parse sql %s error %v", tt.sql, err)
		}
		pp := &ProjectPlan{Fields:stmt.Fields}
		result := pp.Apply(nil, tt.data)
		var mapRes []map[string]interface{}
		if v, ok := result.([]byte); ok {
			err := json.Unmarshal(v, &mapRes)
			if err != nil {
				t.Errorf("Failed to parse the input into map.\n")
				continue
			}
			//fmt.Printf("%t\n", mapRes["rengine_field_0"])

			if !reflect.DeepEqual(tt.result, mapRes) {
				t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.sql, tt.result, mapRes)
			}
		} else {
			t.Errorf("The returned result is not type of []byte\n")
		}
	}
}