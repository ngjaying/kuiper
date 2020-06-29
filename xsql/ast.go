package xsql

import (
	"engine/common"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)



type Node interface {
	node()
}

type Expr interface {
	Node
	expr()
}

type Field struct {
	Name  string
	AName string
	Expr
}

type Source interface {
	Node
	source()
}

type Sources []Source

func (ss Sources) node(){}

type Table struct {
	Name string
	Alias string
}

func (t *Table) source() {}
func (ss *Table) node(){}


type JoinType int
const (
	LEFT_JOIN JoinType = iota
	INNER_JOIN
	RIGHT_JOIN
	FULL_JOIN
	CROSS_JOIN
)

type Join struct {
	Name     string
	Alias    string
	JoinType JoinType
	Expr     Expr
}

func (j *Join) source() {}
func (ss *Join) node(){}

type Joins []Join
func (ss Joins) node(){}

type Statement interface{
	Stmt()
	Node
}

type SelectStatement struct {
	Fields     Fields
	Sources    Sources
	Joins      Joins
	Condition  Expr
	Dimensions Dimensions
	Having	   Expr
	SortFields SortFields
}

func (ss *SelectStatement) Stmt() {}
func (ss *SelectStatement) node(){}

type Literal interface {
	Expr
	literal()
}

type ParenExpr struct {
	Expr Expr
}

type ArrowExpr struct {
	Expr Expr
}

type BracketExpr struct {
	Expr Expr
}

type ColonExpr struct {
	Start int
	End int
}

type IndexExpr struct {
	Index int
}

type BooleanLiteral struct {
	Val bool
}

type TimeLiteral struct {
	Val Token
}

type IntegerLiteral struct {
	Val int
}

type StringLiteral struct {
	Val string
}

type NumberLiteral struct {
	Val float64
}

type Wildcard struct {
	Token Token
}

type Dimension struct {
	Expr Expr
}

type SortField struct {
	Name string
	Ascending bool
}

type SortFields []SortField

type Dimensions []Dimension

func (f *Field) expr() {}
func (f *Field) node(){}

func (pe *ParenExpr) expr() {}
func (pe *ParenExpr) node(){}

func (ae *ArrowExpr) expr() {}
func (ae *ArrowExpr) node(){}

func (be *BracketExpr) expr() {}
func (be *BracketExpr) node(){}

func (be *ColonExpr) expr() {}
func (be *ColonExpr) node(){}

func (be *IndexExpr) expr() {}
func (be *IndexExpr) node(){}

func (w *Wildcard) expr() {}
func (w *Wildcard) node(){}

func (bl *BooleanLiteral) expr()    {}
func (bl *BooleanLiteral) literal() {}
func (bl *BooleanLiteral) node(){}

func (tl *TimeLiteral) expr()    {}
func (tl *TimeLiteral) literal() {}
func (tl *TimeLiteral) node(){}

func (il *IntegerLiteral) expr()    {}
func (il *IntegerLiteral) literal() {}
func (il *IntegerLiteral) node(){}

func (nl *NumberLiteral) expr()    {}
func (nl *NumberLiteral) literal() {}
func (nl *NumberLiteral) node(){}

func (sl *StringLiteral) expr()    {}
func (sl *StringLiteral) literal() {}
func (sl *StringLiteral) node(){}

func (d *Dimension) expr() {}
func (d *Dimension) node(){}

func (d Dimensions) node(){}
func (d *Dimensions) GetWindow() *Window{
	for _, child := range *d {
		if w, ok := child.Expr.(*Window); ok{
			return w
		}
	}
	return nil
}
func (d *Dimensions) GetGroups() Dimensions{
	var nd Dimensions
	for _, child := range *d {
		if _, ok := child.Expr.(*Window); !ok{
			nd = append(nd, child)
		}
	}
	return nd
}

func (sf *SortField) expr() {}
func (sf *SortField) node(){}

func (sf SortFields) node(){}

type Call struct {
	Name string
	Args []Expr
}

func (c *Call) expr() {}
func (c *Call) literal() {}
func (c *Call) node(){}

type WindowType int

const (
	NOT_WINDOW WindowType = iota
	TUMBLING_WINDOW
	HOPPING_WINDOW
	SLIDING_WINDOW
	SESSION_WINDOW
)

type Window struct {
	WindowType WindowType
	Length	   *IntegerLiteral
	Interval   *IntegerLiteral
}

func (w *Window) expr()    {}
func (w *Window) literal() {}
func (w *Window) node()    {}

type  SelectStatements []SelectStatement

func (ss *SelectStatements) node(){}

type Fields []Field
func (fs Fields) node(){}

type BinaryExpr struct {
	OP Token
	LHS Expr
	RHS Expr
}

func (fe *BinaryExpr) expr() {}
func (be *BinaryExpr) node(){}

type FieldRef struct {
	StreamName StreamName
	Name  string
}

func (fr *FieldRef) expr() {}
func (fr *FieldRef) node(){}


// The stream AST tree
type Options map[string]string
func (o Options) node() {}

type StreamName string
func (sn *StreamName) node() {}

type StreamStmt struct {
	Name StreamName
	StreamFields StreamFields
	Options Options
}

func (ss *StreamStmt) node(){}
func (ss *StreamStmt) Stmt() {}


type FieldType interface {
	fieldType()
	Node
}

type StreamField struct {
	Name string
	FieldType
}

type StreamFields []StreamField

func (sf StreamFields) node(){}

type BasicType struct {
	Type DataType
}
func (bt *BasicType) fieldType() {}
func (bt *BasicType) node(){}

type ArrayType struct {
	Type DataType
	FieldType
}
func (at *ArrayType) fieldType() {}
func (at *ArrayType) node(){}

type RecType struct {
	StreamFields StreamFields
}
func (rt *RecType) fieldType() {}
func (rt *RecType) node(){}

type ShowStreamsStatement struct {

}

type DescribeStreamStatement struct {
	Name string
}

type ExplainStreamStatement struct {
	Name string
}

type DropStreamStatement struct {
	Name string
}

func (ss *ShowStreamsStatement) Stmt() {}
func (ss *ShowStreamsStatement) node(){}

func (dss *DescribeStreamStatement) Stmt() {}
func (dss *DescribeStreamStatement) node(){}

func (ess *ExplainStreamStatement) Stmt() {}
func (ess *ExplainStreamStatement) node(){}

func (dss *DropStreamStatement) Stmt() {}
func (dss *DropStreamStatement) node(){}


type Visitor interface {
	Visit(Node) Visitor
}

func Walk(v Visitor, node Node) {
	if node == nil {
		return
	}

	if v = v.Visit(node); v == nil {
		return
	}

	switch n := node.(type) {

	case *BinaryExpr:
		Walk(v, n.LHS)
		Walk(v, n.RHS)

	case *Call:
		for _, expr := range n.Args {
			Walk(v, expr)
		}

	case *Window:
		Walk(v, n.Length)
		Walk(v, n.Interval)

	case *Field:
		Walk(v, n.Expr)

	case Fields:
		for _, c := range n {
			Walk(v, &c)
		}

	case *ParenExpr:
		Walk(v, n.Expr)

	case *SelectStatement:
		Walk(v, n.Fields)
		Walk(v, n.Dimensions)
		Walk(v, n.Sources)
		Walk(v, n.Joins)
		Walk(v, n.Condition)
		Walk(v, n.SortFields)

	case SortFields:
		for _, sf := range n {
			Walk(v, &sf)
		}

	case Sources:
		for _, s := range n {
			Walk(v, s)
		}

	case Joins:
		for _, s := range n {
			Walk(v, &s)
		}
	case *Join:
		Walk(v, n.Expr)

	case *StreamStmt:
		Walk(v, &n.Name)
		Walk(v, n.StreamFields)
		Walk(v, n.Options)

	case *BasicType:
		Walk(v, n)

	case *ArrayType:
		Walk(v, n)

	case *RecType:
		Walk(v, n)

	case *ShowStreamsStatement:
		Walk(v, n)

	case *DescribeStreamStatement:
		Walk(v, n)

	case *ExplainStreamStatement:
		Walk(v, n)

	case *DropStreamStatement:
		Walk(v, n)
	}
}


// WalkFunc traverses a node hierarchy in depth-first order.
func WalkFunc(node Node, fn func(Node)) {
	Walk(walkFuncVisitor(fn), node)
}

type walkFuncVisitor func(Node)

func (fn walkFuncVisitor) Visit(n Node) Visitor { fn(n); return fn }


// Valuer is the interface that wraps the Value() method.
type Valuer interface {
	// Value returns the value and existence flag for a given key.
	Value(key string) (interface{}, bool)
}


// CallValuer implements the Call method for evaluating function calls.
type CallValuer interface {
	Valuer

	// Call is invoked to evaluate a function call (if possible).
	Call(name string, args []interface{}) (interface{}, bool)
}

type AggregateCallValuer interface {
	CallValuer
	GetAllTuples() AggregateData
}

type Wildcarder interface {
	// Value returns the value and existence flag for a given key.
	All(stream string) (interface{}, bool)
}

type DataValuer interface {
	Valuer
	Wildcarder
}

type WildcardValuer struct {
	Data Wildcarder
}

//TODO deal with wildcard of a stream, e.g. SELECT Table.* from Table inner join Table1
func (wv *WildcardValuer) Value(key string) (interface{}, bool) {
	if key == ""{
		return wv.Data.All(key)
	}else{
		a := strings.Index(key, ".*")
		if a <= 0{
			return nil, false
		}else{
			return wv.Data.All(key[:a])
		}
	}
}

/**********************************
**	Various Data Types for SQL transformation
 */
type BufferOrEvent struct{
	Data interface{}
	Channel   string
	Processed bool
}

type AggregateData interface {
	AggregateEval(expr Expr) []interface{}
}

// Message is a valuer that substitutes values for the mapped interface.
type Message map[string]interface{}

// Value returns the value for a key in the Message.
func (m Message) Value(key string) (interface{}, bool) {
	key = strings.ToLower(key)
	if keys := strings.Split(key, "."); len(keys) == 1 {
		v, ok := m[key]
		return v, ok
	} else if len(keys) == 2 {
		v, ok := m[keys[1]]
		return v, ok
	}
	common.Log.Println("Invalid key: " + key + ", expect source.field or field.")
	return nil, false
}

type Event interface {
	GetTimestamp() int64
	IsWatermark() bool
}

type Tuple struct {
	Emitter   string
	Message   Message
	Timestamp int64
}

func (t *Tuple) Value(key string) (interface{}, bool) {
	return t.Message.Value(key)
}

func (t *Tuple) All(stream string) (interface{}, bool) {
	return t.Message, true
}

func (t *Tuple) AggregateEval(expr Expr) []interface{} {
	return []interface{}{Eval(expr, t)}
}

func (t *Tuple) GetTimestamp() int64 {
	return t.Timestamp
}

func (t *Tuple) IsWatermark() bool {
	return false
}

type WindowTuples struct {
	Emitter string
	Tuples []Tuple
}

type WindowTuplesSet []WindowTuples

func (w WindowTuplesSet) GetBySrc(src string) []Tuple {
	for _, me := range w {
		if me.Emitter == src {
			return me.Tuples
		}
	}
	return nil
}

func (w WindowTuplesSet) Len() int {
	if len(w) > 0{
		return len(w[0].Tuples)
	}
	return 0
}
func (w WindowTuplesSet) Swap(i, j int) {
	if len(w) > 0{
		s := w[0].Tuples
		s[i], s[j] = s[j], s[i]
	}
}
func (w WindowTuplesSet) Index(i int) Valuer {
	if len(w) > 0{
		s := w[0].Tuples
		return &(s[i])
	}
	return nil
}

func (w WindowTuplesSet) AddTuple(tuple *Tuple) WindowTuplesSet{
	found := false
	for i, t := range w {
		if t.Emitter == tuple.Emitter {
			t.Tuples = append(t.Tuples, *tuple)
			found = true
			w[i] = t
			break
		}
	}

	if !found {
		ets := &WindowTuples{Emitter: tuple.Emitter}
		ets.Tuples = append(ets.Tuples, *tuple)
		w = append(w, *ets)
	}
	return w
}

//Sort by tuple timestamp
func (w WindowTuplesSet) Sort() {
	for _, t := range w {
		tuples := t.Tuples
		sort.SliceStable(tuples, func(i, j int) bool {
			return tuples[i].Timestamp < tuples[j].Timestamp
		})
		t.Tuples = tuples
	}
}

func (w WindowTuplesSet) AggregateEval(expr Expr) []interface{} {
	var result []interface{}
	if len(w) != 1 { //should never happen
		return nil
	}
	for _, t := range w[0].Tuples {
		result = append(result, Eval(expr, &t))
	}
	return result
}

type JoinTuple struct {
	Tuples []Tuple
}

func (jt *JoinTuple) AddTuple(tuple Tuple) {
	jt.Tuples = append(jt.Tuples, tuple)
}

func (jt *JoinTuple) AddTuples(tuples []Tuple) {
	for _, t := range tuples {
		jt.Tuples = append(jt.Tuples, t)
	}
}

func (jt *JoinTuple) Value(key string) (interface{}, bool) {
	keys := strings.Split(key, ".")
	tuples := jt.Tuples
	switch len(keys) {
	case 1:
		if len(tuples) > 1 {
			for _, tuple := range tuples {	//TODO support key without modifier?
				v, ok := tuple.Message[key]
				if ok{
					return v, ok
				}
			}
			common.Log.Infoln("Wrong key: ", key, ", not found")
			return nil, false
		} else{
			v, ok := tuples[0].Message[key]
			return v, ok
		}
	case 2:
		emitter, key := keys[0], keys[1]
		//TODO should use hash here
		for _, tuple := range tuples {
			if tuple.Emitter == emitter {
				v, ok := tuple.Message[key]
				return v, ok
			}
		}
		return nil, false
	default:
		common.Log.Infoln("Wrong key: ", key, ", expect dot in the expression.")
		return nil, false
	}
}

func (jt *JoinTuple) All(stream string) (interface{}, bool) {
	if stream != ""{
		for _, t := range jt.Tuples{
			if t.Emitter == stream{
				return t.Message, true
			}
		}
	}else{
		var r Message = make(map[string]interface{})
		for _, t := range jt.Tuples{
			for k, v := range t.Message{
				if _, ok := r[k]; !ok{
					r[k] = v
				}
			}
		}
		return r, true
	}
	return nil, false
}

type JoinTupleSets []JoinTuple
func (s JoinTupleSets) Len() int      { return len(s) }
func (s JoinTupleSets) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s JoinTupleSets) Index(i int) Valuer { return &(s[i]) }

func (s JoinTupleSets) AggregateEval(expr Expr) []interface{} {
	var result []interface{}
	for _, t := range s {
		result = append(result, Eval(expr, &t))
	}
	return result
}

type GroupedTuples []DataValuer
func (s GroupedTuples) AggregateEval(expr Expr) []interface{} {
	var result []interface{}
	for _, t := range s {
		result = append(result, Eval(expr, t))
	}
	return result
}

type GroupedTuplesSet []GroupedTuples
func (s GroupedTuplesSet) Len() int           { return len(s) }
func (s GroupedTuplesSet) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s GroupedTuplesSet) Index(i int) Valuer { return s[i][0] }

type SortingData interface {
	Len() int
	Swap(i, j int)
	Index(i int) Valuer
}

// multiSorter implements the Sort interface, sorting the changes within.Hi
type MultiSorter struct {
	SortingData
	fields SortFields
}

// OrderedBy returns a Sorter that sorts using the less functions, in order.
// Call its Sort method to sort the data.
func OrderedBy(fields SortFields) *MultiSorter {
	return &MultiSorter{
		fields: fields,
	}
}

// Less is part of sort.Interface. It is implemented by looping along the
// less functions until it finds a comparison that discriminates between
// the two items (one is less than the other). Note that it can call the
// less functions twice per call. We could change the functions to return
// -1, 0, 1 and reduce the number of calls for greater efficiency: an
// exercise for the reader.
func (ms *MultiSorter) Less(i, j int) bool {
	p, q := ms.SortingData.Index(i), ms.SortingData.Index(j)
	vep, veq := &ValuerEval{Valuer: MultiValuer(p, &FunctionValuer{})}, &ValuerEval{Valuer: MultiValuer(q, &FunctionValuer{})}
	for _, field := range ms.fields {
		vp, ok := vep.Valuer.Value(field.Name)
		if !ok {
			return !field.Ascending
		}
		vq, ok := veq.Valuer.Value(field.Name)
		if !ok {
			return !field.Ascending
		}
		switch {
		case vep.simpleDataEval(vp, vq, LT):
			return field.Ascending
		case veq.simpleDataEval(vq, vp, LT):
			return !field.Ascending
		}
	}
	return false
}

// Sort sorts the argument slice according to the less functions passed to OrderedBy.
func (ms *MultiSorter) Sort(data SortingData) {
	ms.SortingData = data
	sort.Sort(ms)
}

type EvalResultMessage struct {
	Emitter string
	Result  interface{}
	Message Message
}

type ResultsAndMessages []EvalResultMessage

// Eval evaluates expr against a map.
func Eval(expr Expr, m Valuer) interface{} {
	eval := ValuerEval{Valuer: MultiValuer(m, &FunctionValuer{})}
	return eval.Eval(expr)
}

// ValuerEval will evaluate an expression using the Valuer.
type ValuerEval struct {
	Valuer Valuer

	// IntegerFloatDivision will set the eval system to treat
	// a division between two integers as a floating point division.
	IntegerFloatDivision bool
}

// MultiValuer returns a Valuer that iterates over multiple Valuer instances
// to find a match.
func MultiValuer(valuers ...Valuer) Valuer {
	return multiValuer(valuers)
}

type multiValuer []Valuer

func (a multiValuer) Value(key string) (interface{}, bool) {
	for _, valuer := range a {
		if v, ok := valuer.Value(key); ok {
			return v, true
		}
	}
	return nil, false
}

func (a multiValuer) Call(name string, args []interface{}) (interface{}, bool) {
	for _, valuer := range a {
		if valuer, ok := valuer.(CallValuer); ok {
			if v, ok := valuer.Call(name, args); ok {
				return v, true
			} else {
				common.Log.Println(fmt.Sprintf("Found error \"%s\" when call func %s.\n", v, name))
			}
		}
	}
	return nil, false
}

type multiAggregateValuer struct{
	data AggregateData
	valuers []Valuer
}

func MultiAggregateValuer(data AggregateData, valuers ...Valuer) Valuer {
	return &multiAggregateValuer{
		data: data,
		valuers: valuers,
	}
}

func (a *multiAggregateValuer) Value(key string) (interface{}, bool) {
	for _, valuer := range a.valuers {
		if v, ok := valuer.Value(key); ok {
			return v, true
		}
	}
	return nil, false
}

//The args is [][] for aggregation
func (a *multiAggregateValuer) Call(name string, args []interface{}) (interface{}, bool) {
	var singleArgs []interface{} = nil
	for _, valuer := range a.valuers {
		if a, ok := valuer.(AggregateCallValuer); ok {
			if v, ok := a.Call(name, args); ok {
				return v, true
			}
		}else if c, ok := valuer.(CallValuer); ok{
			if singleArgs == nil{
				for _, arg := range args{
					if arg, ok := arg.([]interface{}); ok{
						singleArgs = append(singleArgs, arg[0])
					}else{
						common.Log.Infof("multiAggregateValuer does not get [][] args but get %v", args)
						return nil, false
					}
				}
			}
			if v, ok := c.Call(name, singleArgs); ok {
				return v, true
			}
		}
	}
	return nil, false
}

func (a *multiAggregateValuer) GetAllTuples() AggregateData {
	return a.data
}

type BracketEvalResult struct {
	Start, End int
}

func (ber *BracketEvalResult) isIndex() bool {
	return ber.Start == ber.End
}

// Eval evaluates an expression and returns a value.
func (v *ValuerEval) Eval(expr Expr) interface{} {
	if expr == nil {
		return nil
	}
	switch expr := expr.(type) {
	case *BinaryExpr:
		return v.evalBinaryExpr(expr)
	//case *BooleanLiteral:
	//	return expr.Val
	case *IntegerLiteral:
		return expr.Val
	case *NumberLiteral:
		return expr.Val
	case *ParenExpr:
		return v.Eval(expr.Expr)
	case *StringLiteral:
		return expr.Val
	case *BooleanLiteral:
		return expr.Val
	case *ColonExpr:
		return &BracketEvalResult{Start:expr.Start, End:expr.End}
	case *IndexExpr:
		return &BracketEvalResult{Start:expr.Index, End:expr.Index}
	case *Call:
		if valuer, ok := v.Valuer.(CallValuer); ok {
			var args []interface{}

			if len(expr.Args) > 0 {
				args = make([]interface{}, len(expr.Args))
				if aggreValuer, ok := valuer.(AggregateCallValuer); ok{
					for i := range expr.Args {
						args[i] = aggreValuer.GetAllTuples().AggregateEval(expr.Args[i])
					}
				}else{
					for i := range expr.Args {
						args[i] = v.Eval(expr.Args[i])
					}
				}
			}
			val, _ := valuer.Call(expr.Name, args)
			return val
		}
		return nil
	case *FieldRef:
		if expr.StreamName == "" {
			val, _ := v.Valuer.Value(expr.Name)
			return val
		} else {
			//The field specified with stream source
			val, _ := v.Valuer.Value(string(expr.StreamName) + "." + expr.Name)
			return val
		}
	case *Wildcard:
		val, _ := v.Valuer.Value("")
		return val
	default:
		return nil
	}
	return nil
}


func (v *ValuerEval) evalBinaryExpr(expr *BinaryExpr) interface{} {
	lhs := v.Eval(expr.LHS)
	switch val := lhs.(type) {
	case map[string]interface{}:
		return v.evalJsonExpr(val, expr.OP, expr.RHS)
	case []interface{}:
		return v.evalJsonExpr(val, expr.OP, expr.RHS)
	}

	rhs := v.Eval(expr.RHS)
	if lhs == nil && rhs != nil {
		// When the LHS is nil and the RHS is a boolean, implicitly cast the
		// nil to false.
		if _, ok := rhs.(bool); ok {
			lhs = false
		}
	} else if lhs != nil && rhs == nil {
		// Implicit cast of the RHS nil to false when the LHS is a boolean.
		if _, ok := lhs.(bool); ok {
			rhs = false
		}
	}
	return v.simpleDataEval(lhs, rhs, expr.OP)
}


func (v *ValuerEval) evalJsonExpr(result interface{}, op Token,  expr Expr) interface{} {
	if val, ok := result.(map[string]interface{}); ok {
		switch op {
		case ARROW:
			if exp, ok := expr.(*FieldRef); ok {
				ve := &ValuerEval{Valuer: Message(val)}
				return ve.Eval(exp)
			} else {
				fmt.Printf("The right expression is not a field reference node.\n")
				return nil
			}
		default:
			fmt.Printf("%v is an invalid operation.\n", op)
			return nil
		}
	}

	if val, ok := result.([]interface{}); ok {
		switch op {
		case SUBSET:
			ber := v.Eval(expr)
			if berVal, ok1 := ber.(*BracketEvalResult); ok1 {
				if berVal.isIndex() {
					if berVal.Start >= len(val) {
						fmt.Printf("Out of index: %d of %d.\n", berVal.Start, len(val))
						return nil
					}
					return val[berVal.Start]
				} else {
					if berVal.Start >= len(val) {
						fmt.Printf("Start value is out of index: %d of %d.\n", berVal.Start, len(val))
						return nil
					}

					if berVal.End >= len(val) {
						fmt.Printf("End value is out of index: %d of %d.\n", berVal.End, len(val))
						return nil
					}
					return val[berVal.Start : berVal.End]
				}
			} else {
				fmt.Printf("Invalid evaluation result - %v.\n", berVal)
				return nil
			}
		default:
			fmt.Printf("%v is an invalid operation.\n", op)
			return nil
		}
	}
	return nil
}

func (v *ValuerEval) simpleDataEval(lhs, rhs interface{}, op Token) interface{} {
	lhs = convertNum(lhs)
	rhs = convertNum(rhs)
	// Evaluate if both sides are simple types.
	switch lhs := lhs.(type) {
	case bool:
		rhs, ok := rhs.(bool)
		switch op {
		case AND:
			return ok && (lhs && rhs)
		case OR:
			return ok && (lhs || rhs)
		case BITWISE_AND:
			return ok && (lhs && rhs)
		case BITWISE_OR:
			return ok && (lhs || rhs)
		case BITWISE_XOR:
			return ok && (lhs != rhs)
		case EQ:
			return ok && (lhs == rhs)
		case NEQ:
			return ok && (lhs != rhs)
		}
	case float64:
		// Try the rhs as a float64, int64, or uint64
		rhsf, ok := rhs.(float64)
		if !ok {
			switch val := rhs.(type) {
			case int64:
				rhsf, ok = float64(val), true
			case uint64:
				rhsf, ok = float64(val), true
			}
		}

		rhs := rhsf
		switch op {
		case EQ:
			return ok && (lhs == rhs)
		case NEQ:
			return ok && (lhs != rhs)
		case LT:
			return ok && (lhs < rhs)
		case LTE:
			return ok && (lhs <= rhs)
		case GT:
			return ok && (lhs > rhs)
		case GTE:
			return ok && (lhs >= rhs)
		case ADD:
			if !ok {
				return nil
			}
			return lhs + rhs
		case SUB:
			if !ok {
				return nil
			}
			return lhs - rhs
		case MUL:
			if !ok {
				return nil
			}
			return lhs * rhs
		case DIV:
			if !ok {
				return nil
			} else if rhs == 0 {
				return float64(0)
			}
			return lhs / rhs
		case MOD:
			if !ok {
				return nil
			}
			return math.Mod(lhs, rhs)
		}
	case int64:
		// Try as a float64 to see if a float cast is required.
		switch rhs := rhs.(type) {
		case float64:
			lhs := float64(lhs)
			switch op {
			case EQ:
				return lhs == rhs
			case NEQ:
				return lhs != rhs
			case LT:
				return lhs < rhs
			case LTE:
				return lhs <= rhs
			case GT:
				return lhs > rhs
			case GTE:
				return lhs >= rhs
			case ADD:
				return lhs + rhs
			case SUB:
				return lhs - rhs
			case MUL:
				return lhs * rhs
			case DIV:
				if rhs == 0 {
					return float64(0)
				}
				return lhs / rhs
			case MOD:
				return math.Mod(lhs, rhs)
			}
		case int64:
			switch op {
			case EQ:
				return lhs == rhs
			case NEQ:
				return lhs != rhs
			case LT:
				return lhs < rhs
			case LTE:
				return lhs <= rhs
			case GT:
				return lhs > rhs
			case GTE:
				return lhs >= rhs
			case ADD:
				return lhs + rhs
			case SUB:
				return lhs - rhs
			case MUL:
				return lhs * rhs
			case DIV:
				if v.IntegerFloatDivision {
					if rhs == 0 {
						return float64(0)
					}
					return float64(lhs) / float64(rhs)
				}

				if rhs == 0 {
					return int64(0)
				}
				return lhs / rhs
			case MOD:
				if rhs == 0 {
					return int64(0)
				}
				return lhs % rhs
			case BITWISE_AND:
				return lhs & rhs
			case BITWISE_OR:
				return lhs | rhs
			case BITWISE_XOR:
				return lhs ^ rhs
			}
		case uint64:
			switch op {
			case EQ:
				return uint64(lhs) == rhs
			case NEQ:
				return uint64(lhs) != rhs
			case LT:
				if lhs < 0 {
					return true
				}
				return uint64(lhs) < rhs
			case LTE:
				if lhs < 0 {
					return true
				}
				return uint64(lhs) <= rhs
			case GT:
				if lhs < 0 {
					return false
				}
				return uint64(lhs) > rhs
			case GTE:
				if lhs < 0 {
					return false
				}
				return uint64(lhs) >= rhs
			case ADD:
				return uint64(lhs) + rhs
			case SUB:
				return uint64(lhs) - rhs
			case MUL:
				return uint64(lhs) * rhs
			case DIV:
				if rhs == 0 {
					return uint64(0)
				}
				return uint64(lhs) / rhs
			case MOD:
				if rhs == 0 {
					return uint64(0)
				}
				return uint64(lhs) % rhs
			case BITWISE_AND:
				return uint64(lhs) & rhs
			case BITWISE_OR:
				return uint64(lhs) | rhs
			case BITWISE_XOR:
				return uint64(lhs) ^ rhs
			}
		}
	case uint64:
		// Try as a float64 to see if a float cast is required.
		switch rhs := rhs.(type) {
		case float64:
			lhs := float64(lhs)
			switch op {
			case EQ:
				return lhs == rhs
			case NEQ:
				return lhs != rhs
			case LT:
				return lhs < rhs
			case LTE:
				return lhs <= rhs
			case GT:
				return lhs > rhs
			case GTE:
				return lhs >= rhs
			case ADD:
				return lhs + rhs
			case SUB:
				return lhs - rhs
			case MUL:
				return lhs * rhs
			case DIV:
				if rhs == 0 {
					return float64(0)
				}
				return lhs / rhs
			case MOD:
				return math.Mod(lhs, rhs)
			}
		case int64:
			switch op {
			case EQ:
				return lhs == uint64(rhs)
			case NEQ:
				return lhs != uint64(rhs)
			case LT:
				if rhs < 0 {
					return false
				}
				return lhs < uint64(rhs)
			case LTE:
				if rhs < 0 {
					return false
				}
				return lhs <= uint64(rhs)
			case GT:
				if rhs < 0 {
					return true
				}
				return lhs > uint64(rhs)
			case GTE:
				if rhs < 0 {
					return true
				}
				return lhs >= uint64(rhs)
			case ADD:
				return lhs + uint64(rhs)
			case SUB:
				return lhs - uint64(rhs)
			case MUL:
				return lhs * uint64(rhs)
			case DIV:
				if rhs == 0 {
					return uint64(0)
				}
				return lhs / uint64(rhs)
			case MOD:
				if rhs == 0 {
					return uint64(0)
				}
				return lhs % uint64(rhs)
			case BITWISE_AND:
				return lhs & uint64(rhs)
			case BITWISE_OR:
				return lhs | uint64(rhs)
			case BITWISE_XOR:
				return lhs ^ uint64(rhs)
			}
		case uint64:
			switch op {
			case EQ:
				return lhs == rhs
			case NEQ:
				return lhs != rhs
			case LT:
				return lhs < rhs
			case LTE:
				return lhs <= rhs
			case GT:
				return lhs > rhs
			case GTE:
				return lhs >= rhs
			case ADD:
				return lhs + rhs
			case SUB:
				return lhs - rhs
			case MUL:
				return lhs * rhs
			case DIV:
				if rhs == 0 {
					return uint64(0)
				}
				return lhs / rhs
			case MOD:
				if rhs == 0 {
					return uint64(0)
				}
				return lhs % rhs
			case BITWISE_AND:
				return lhs & rhs
			case BITWISE_OR:
				return lhs | rhs
			case BITWISE_XOR:
				return lhs ^ rhs
			}
		}
	case string:
		switch op {
		case EQ:
			rhs, ok := rhs.(string)
			if !ok {
				return false
			}
			return lhs == rhs
		case NEQ:
			rhs, ok := rhs.(string)
			if !ok {
				return false
			}
			return lhs != rhs
		case LT:
			rhs, ok := rhs.(string)
			if !ok {
				return false
			}
			return lhs < rhs
		case LTE:
			rhs, ok := rhs.(string)
			if !ok {
				return false
			}
			return lhs <= rhs
		case GT:
			rhs, ok := rhs.(string)
			if !ok {
				return false
			}
			return lhs > rhs
		case GTE:
			rhs, ok := rhs.(string)
			if !ok {
				return false
			}
			return lhs >= rhs
		}
	case time.Time:
		rt, err := common.InterfaceToTime(rhs, "")
		if err != nil{
			return false
		}
		switch op {
		case EQ:
			return lhs.Equal(rt)
		case NEQ:
			return !lhs.Equal(rt)
		case LT:
			return lhs.Before(rt)
		case LTE:
			return lhs.Before(rt) || lhs.Equal(rt)
		case GT:
			return lhs.After(rt)
		case GTE:
			return lhs.After(rt) || lhs.Equal(rt)
		}
	}

	// The types were not comparable. If our operation was an equality operation,
	// return false instead of true.
	switch op {
	case EQ, NEQ, LT, LTE, GT, GTE:
		return false
	}
	return nil
}

func convertNum(para interface{}) interface{} {
	if isInt(para) {
		para = toInt64(para)
	} else if isFloat(para) {
		para = toFloat64(para)
	}
	return para
}

func isInt(para interface{}) bool {
	switch para.(type) {
	case int:
		return true
	case int8:
		return true
	case int16:
		return true
	case int32:
		return true
	case int64:
		return true
	}
	return false
}

func toInt64(para interface{}) int64 {
	if v, ok := para.(int); ok {
		return int64(v)
	} else if v, ok := para.(int8); ok {
		return int64(v)
	} else if v, ok := para.(int16); ok {
		return int64(v)
	} else if v, ok := para.(int32); ok {
		return int64(v)
	} else if v, ok := para.(int64); ok {
		return v
	}
	return 0
}

func isFloat(para interface{}) bool {
	switch para.(type) {
	case float32:
		return true
	case float64:
		return true
	}
	return false
}

func toFloat64(para interface{}) float64 {
	if v, ok := para.(float32); ok {
		return float64(v)
	} else if v, ok := para.(float64); ok {
		return v
	}
	return 0
}

func IsAggStatement(node Node) (bool) {
	var r bool = false
	WalkFunc(node, func(n Node) {
		if f, ok := n.(*Call); ok {
			fn := strings.ToLower(f.Name)
			if _, ok1 := aggFuncMap[fn]; ok1 {
				r = true
				return
			}
		} else if d, ok := n.(Dimensions); ok {
			ds := d.GetGroups()
			if ds != nil && len(ds) > 0 {
				r = true
				return
			}
		}
	});
	return r
}