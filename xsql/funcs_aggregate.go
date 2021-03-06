package xsql

import (
	"fmt"
	"strings"
)

type AggregateFunctionValuer struct{
	Data AggregateData
}

func (v AggregateFunctionValuer) Value(key string) (interface{}, bool) {
	return nil, false
}

func (v AggregateFunctionValuer) Call(name string, args []interface{}) (interface{}, bool) {
	lowerName := strings.ToLower(name)
	switch lowerName {
	case "avg":
		arg0 := args[0].([]interface{})
		if len(arg0) > 0{
			v := getFirstValidArg(arg0)
			switch v.(type){
			case int:
				return sliceIntTotal(arg0)/len(arg0), true
			case int64:
				return sliceIntTotal(arg0)/len(arg0), true
			case float64:
				return sliceFloatTotal(arg0)/float64(len(arg0)), true
			default:
				return fmt.Errorf("invalid data type for avg function"), false
			}
		}
		return 0, true
	case "count":
		arg0 := args[0].([]interface{})
		return len(arg0), true
	case "max":
		arg0 := args[0].([]interface{})
		if len(arg0) > 0{
			v := getFirstValidArg(arg0)
			switch t := v.(type){
			case int:
				return sliceIntMax(arg0, t), true
			case int64:
				return sliceIntMax(arg0, int(t)), true
			case float64:
				return sliceFloatMax(arg0, t), true
			case string:
				return sliceStringMax(arg0, t), true
			default:
				return fmt.Errorf("unsupported data type for avg function"), false
			}
		}
		return fmt.Errorf("empty data for max function"), false
	case "min":
		arg0 := args[0].([]interface{})
		if len(arg0) > 0{
			v := getFirstValidArg(arg0)
			switch t := v.(type){
			case int:
				return sliceIntMin(arg0, t), true
			case int64:
				return sliceIntMin(arg0, int(t)), true
			case float64:
				return sliceFloatMin(arg0, t), true
			case string:
				return sliceStringMin(arg0, t), true
			default:
				return fmt.Errorf("unsupported data type for avg function"), false
			}
		}
		return fmt.Errorf("empty data for max function"), false
	case "sum":
		arg0 := args[0].([]interface{})
		if len(arg0) > 0{
			v := getFirstValidArg(arg0)
			switch v.(type){
			case int:
				return sliceIntTotal(arg0), true
			case int64:
				return sliceIntTotal(arg0), true
			case float64:
				return sliceFloatTotal(arg0), true
			default:
				return fmt.Errorf("invalid data type for sum function"), false
			}
		}
		return 0, true
	default:
		return nil, false
	}
}

func (v *AggregateFunctionValuer) GetAllTuples() AggregateData {
	return v.Data
}

func getFirstValidArg(s []interface{}) interface{}{
	for _, v := range s{
		if v != nil{
			return v
		}
	}
	return nil
}

func sliceIntTotal(s []interface{}) int{
	var total int
	for _, v := range s{
		if v, ok := v.(int); ok {
			total += v
		}
	}
	return total
}

func sliceFloatTotal(s []interface{}) float64{
	var total float64
	for _, v := range s{
		if v, ok := v.(float64); ok {
			total += v
		}
	}
	return total
}
func sliceIntMax(s []interface{}, max int) int{
	for _, v := range s{
		if v, ok := v.(int); ok {
			if max < v {
				max = v
			}
		}
	}
	return max
}
func sliceFloatMax(s []interface{}, max float64) float64{
	for _, v := range s{
		if v, ok := v.(float64); ok {
			if max < v {
				max = v
			}
		}
	}
	return max
}

func sliceStringMax(s []interface{}, max string) string{
	for _, v := range s{
		if v, ok := v.(string); ok {
			if max < v {
				max = v
			}
		}
	}
	return max
}
func sliceIntMin(s []interface{}, min int) int{
	for _, v := range s{
		if v, ok := v.(int); ok {
			if min > v {
				min = v
			}
		}
	}
	return min
}
func sliceFloatMin(s []interface{}, min float64) float64{
	for _, v := range s{
		if v, ok := v.(float64); ok {
			if min > v {
				min = v
			}
		}
	}
	return min
}

func sliceStringMin(s []interface{}, min string) string{
	for _, v := range s{
		if v, ok := v.(string); ok {
			if min < v {
				min = v
			}
		}
	}
	return min
}
