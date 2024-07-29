package ast

import (
	"SequelGo/internal/lexer"
	"fmt"
	"strings"
)

func expressionListToString[N Expression](list []N, separator string) string {
	var str strings.Builder
	var items []string
	for _, s := range list {
		items = append(items, s.TokenLiteral())
	}
	str.WriteString(strings.Join(items, separator))
	return str.String()
}

var BuiltinFunctionsTokenType = []lexer.TokenType{
	lexer.TDenseRank, lexer.TRank, lexer.TRowNumber, lexer.TAbs, lexer.TAcos, lexer.TAsin,
	lexer.TAtan, lexer.TCeiling, lexer.TCos, lexer.TCot, lexer.TDegrees, lexer.TExp, lexer.TFloor,
	lexer.TLog, lexer.TLog10, lexer.TPi, lexer.TPower, lexer.TRadians, lexer.TRands, lexer.TRound,
	lexer.TSign, lexer.TSin, lexer.TSqrt, lexer.TSquare, lexer.TTan, lexer.TFirstValue,
	lexer.TLastValue, lexer.TLag, lexer.TLead, lexer.TAvg, lexer.TCount, lexer.TMax, lexer.TMin,
	lexer.TStdev, lexer.TStdevp, lexer.TSum, lexer.TVar, lexer.TVarp, lexer.TGetdate,
	lexer.TChecksum, lexer.TNewId,
}

type ExprStringLiteral struct {
	Span
	Value string
}

type ExprNumberLiteral struct {
	Span
	Value string
}

type ExprLocalVariable struct {
	Span
	Value string
}

type ExprIdentifier struct {
	Span
	Value string
}

type ExprQuotedIdentifier struct {
	Span
	Value string
}

type ExprStar struct {
	Span
}

type ExprWithAlias struct {
	Span
	Expression     Expression
	AsTokenPresent bool
	Alias          Expression
}

type ExprCompoundIdentifier struct {
	Span
	Identifiers []Expression
}

type SelectItems struct {
	Span
	Items []Expression
}

type WhereClause struct {
	Span
	Clause Expression
}

type HavingClause struct {
	Span
	Clause Expression
}

type GroupByClause struct {
	Span
	Items []Expression
}

type ExprSubquery struct {
	SelectBody
}

type TopArg struct {
	Span
	TopKeyword     Keyword
	WithKeyword    *Keyword
	TiesKeyword    *Keyword
	PercentKeyword *Keyword
	Quantity       Expression
}

type TableArg struct {
	Span
	FromKeyword Keyword
	Table       *TableSource
	Joins       []Join
}

type TableSource struct {
	Span
	Type   TableSourceType
	Source Expression
}

type Join struct {
	Span
	JoinKeyword1 Keyword
	JoinKeyword2 *Keyword
	JoinKeyword3 *Keyword
	Type         JoinType
	Table        *TableSource
	Condition    Expression
}

type OrderByClause struct {
	Span
	Expressions []OrderByArg
	OffsetFetch *OffsetFetchClause
}

type OffsetFetchClause struct {
	Span
	Offset OffsetArg
	Fetch  *FetchArg
}

type OrderByArg struct {
	Span
	Column Expression
	Type   OrderByType
}

type OffsetArg struct {
	Span
	Value     Expression
	RowOrRows RowOrRows
}

type FetchArg struct {
	Span
	Value       Expression
	NextOrFirst NextOrFirst
	RowOrRows   RowOrRows
}

type ExprExpressionList struct {
	Span
	List []Expression
}

type FunctionOverClause struct {
	Span
	PartitionByClause []Expression
	OrderByClause     []OrderByArg
	WindowFrameClause *WindowFrameClause
}

type WindowFrameClause struct {
	Span
	RowsOrRange RowsOrRangeType
	Start       *WindowFrameBound
	End         *WindowFrameBound
}

type WindowFrameBound struct {
	Span
	Type       WindowFrameBoundType
	Expression Expression
}

type ExprFunction struct {
	Span
	Type FuncType
	Name Expression
}

type ExprFunctionCall struct {
	Span
	Name       *ExprFunction
	Args       []Expression
	OverClause *FunctionOverClause
}

type ExprCast struct {
	Span
	Expression Expression
	DataType   DataType
}

type CommonTableExpression struct {
	Span
	Name    string
	Columns *ExprExpressionList
	Query   SelectBody
}

func (e ExprStringLiteral) expressionNode()       {}
func (e ExprNumberLiteral) expressionNode()       {}
func (e ExprLocalVariable) expressionNode()       {}
func (e ExprIdentifier) expressionNode()          {}
func (e ExprQuotedIdentifier) expressionNode()    {}
func (e ExprStar) expressionNode()                {}
func (e ExprWithAlias) expressionNode()           {}
func (e ExprCompoundIdentifier) expressionNode()  {}
func (si SelectItems) expressionNode()            {}
func (w WhereClause) expressionNode()             {}
func (h HavingClause) expressionNode()            {}
func (gb GroupByClause) expressionNode()          {}
func (ta TableArg) expressionNode()               {}
func (ts TableSource) expressionNode()            {}
func (j Join) expressionNode()                    {}
func (ta TopArg) expressionNode()                 {}
func (o OrderByArg) expressionNode()              {}
func (o OrderByClause) expressionNode()           {}
func (o OffsetArg) expressionNode()               {}
func (f FetchArg) expressionNode()                {}
func (o OffsetFetchClause) expressionNode()       {}
func (e ExprSubquery) expressionNode()            {}
func (e ExprExpressionList) expressionNode()      {}
func (e ExprFunction) expressionNode()            {}
func (w WindowFrameBound) expressionNode()        {}
func (w WindowFrameClause) expressionNode()       {}
func (e FunctionOverClause) expressionNode()      {}
func (e ExprFunctionCall) expressionNode()        {}
func (e ExprCast) expressionNode()                {}
func (cte CommonTableExpression) expressionNode() {}

func (e ExprStringLiteral) TokenLiteral() string {
	return fmt.Sprintf("'%s'", e.Value)
}
func (e ExprNumberLiteral) TokenLiteral() string {
	return e.Value
}
func (e ExprLocalVariable) TokenLiteral() string {
	return fmt.Sprintf("@%s", e.Value)
}
func (e ExprIdentifier) TokenLiteral() string {
	return e.Value
}
func (e ExprQuotedIdentifier) TokenLiteral() string {
	return fmt.Sprintf("[%s]", e.Value)
}
func (e ExprStar) TokenLiteral() string {
	return "*"
}
func (e ExprWithAlias) TokenLiteral() string {
	var str strings.Builder
	str.WriteString(e.Expression.TokenLiteral())
	if e.AsTokenPresent {
		str.WriteString(" AS ")
	} else {
		str.WriteString(" ")
	}
	str.WriteString(e.Alias.TokenLiteral())
	return str.String()
}
func (e ExprCompoundIdentifier) TokenLiteral() string {
	var str strings.Builder
	for i, item := range e.Identifiers {
		if i > 0 {
			str.WriteString(".")
		}
		str.WriteString(item.TokenLiteral())
	}
	return str.String()
}
func (si SelectItems) TokenLiteral() string {
	return expressionListToString(si.Items, ", ")
}
func (w WhereClause) TokenLiteral() string {
	return w.Clause.TokenLiteral()
}
func (h HavingClause) TokenLiteral() string {
	return h.Clause.TokenLiteral()
}
func (gb GroupByClause) TokenLiteral() string {
	return expressionListToString(gb.Items, ", ")
}
func (ta TableArg) TokenLiteral() string {
	var str strings.Builder

	str.WriteString(fmt.Sprintf(" %s ", ta.FromKeyword.TokenLiteral()))

	str.WriteString(ta.Table.TokenLiteral())

	if len(ta.Joins) == 0 {
		return str.String()
	}

	var joins []string
	for _, j := range ta.Joins {
		joins = append(joins, j.TokenLiteral())
	}

	str.WriteString(strings.Join(joins, " "))

	return str.String()
}
func (ts TableSource) TokenLiteral() string {
	var str strings.Builder

	str.WriteString(ts.Source.TokenLiteral())

	return str.String()
}
func (j Join) TokenLiteral() string {
	var str strings.Builder

	str.WriteString(fmt.Sprintf(" %s", j.JoinKeyword1.TokenLiteral()))
	if j.JoinKeyword2 != nil {
		str.WriteString(fmt.Sprintf(" %s ", j.JoinKeyword2.TokenLiteral()))
	}
	if j.JoinKeyword3 != nil {
		str.WriteString(fmt.Sprintf(" %s ", j.JoinKeyword3.TokenLiteral()))
	}

	str.WriteString(j.Table.TokenLiteral())

	if j.Condition != nil {
		str.WriteString(" ON ")
		str.WriteString(j.Condition.TokenLiteral())
	}

	return str.String()
}
func (ta TopArg) TokenLiteral() string {
	var str strings.Builder
	str.WriteString(fmt.Sprintf("%s %s", ta.TopKeyword.TokenLiteral(), ta.Quantity.TokenLiteral()))

	if ta.PercentKeyword != nil {
		str.WriteString(fmt.Sprintf(" %s", ta.PercentKeyword.TokenLiteral()))
	}

	if ta.WithKeyword != nil {
		str.WriteString(fmt.Sprintf(" %s", ta.WithKeyword.TokenLiteral()))
	}

	if ta.TiesKeyword != nil {
		str.WriteString(fmt.Sprintf(" %s", ta.TiesKeyword.TokenLiteral()))
	}

	return str.String()
}
func (o OrderByClause) TokenLiteral() string {
	var str strings.Builder

	if len(o.Expressions) == 0 {
		return ""
	}

	var orderByArgs []string
	for _, o := range o.Expressions {
		orderByArgs = append(orderByArgs, o.TokenLiteral())
	}

	str.WriteString(" ORDER BY ")
	str.WriteString(strings.Join(orderByArgs, ", "))

	if o.OffsetFetch == nil {
		return str.String()
	}

	str.WriteString(o.OffsetFetch.TokenLiteral())

	return str.String()
}
func (o OffsetFetchClause) TokenLiteral() string {
	var str strings.Builder

	str.WriteString(o.Offset.TokenLiteral())

	if o.Fetch == nil {
		return str.String()
	}

	str.WriteString(o.Fetch.TokenLiteral())
	return str.String()
}
func (o OrderByArg) TokenLiteral() string {
	var str strings.Builder
	str.WriteString(o.Column.TokenLiteral())
	switch o.Type {
	case OBNone:
		break
	case OBAsc:
		str.WriteString(" ASC")
		break
	case OBDesc:
		str.WriteString(" DESC")
		break
	}

	return str.String()
}
func (o OffsetArg) TokenLiteral() string {
	var str strings.Builder
	str.WriteString(" OFFSET ")
	str.WriteString(o.Value.TokenLiteral())
	switch o.RowOrRows {
	case RRRow:
		str.WriteString(" ROW")
		break
	case RRRows:
		str.WriteString(" ROWS")
		break
	}
	return str.String()
}
func (f FetchArg) TokenLiteral() string {
	var str strings.Builder
	str.WriteString(" FETCH ")
	switch f.NextOrFirst {
	case NFNext:
		str.WriteString(" NEXT ")
		break
	case NFFirst:
		str.WriteString(" FIRST ")
		break
	}

	str.WriteString(f.Value.TokenLiteral())

	switch f.RowOrRows {
	case RRRow:
		str.WriteString(" ROW")
		break
	case RRRows:
		str.WriteString(" ROWS")
		break
	}

	str.WriteString(" ONLY")
	return str.String()
}
func (e ExprSubquery) TokenLiteral() string {
	var str strings.Builder
	str.WriteString("(")

	str.WriteString(e.SelectBody.TokenLiteral())

	str.WriteString(")")
	return str.String()
}
func (e ExprExpressionList) TokenLiteral() string {
	var str strings.Builder
	for i, item := range e.List {
		if i > 0 {
			str.WriteString(", ")
		}

		str.WriteString(item.TokenLiteral())
	}
	return str.String()
}
func (e ExprFunction) TokenLiteral() string {
	switch e.Type {
	case FuncDenseRank:
		return "DENSE_RANK"
	case FuncRank:
		return "RANK"
	case FuncRowNumber:
		return "ROW_NUMBER"
	case FuncAbs:
		return "ABS"
	case FuncAcos:
		return "ACOS"
	case FuncAsin:
		return "ASIN"
	case FuncAtan:
		return "ATAN"
	case FuncCeiling:
		return "CEILING"
	case FuncCos:
		return "COS"
	case FuncCot:
		return "COT"
	case FuncDegrees:
		return "DEGREES"
	case FuncExp:
		return "EXP"
	case FuncFloor:
		return "FLOOR"
	case FuncLog:
		return "LOG"
	case FuncLog10:
		return "LOG10"
	case FuncPi:
		return "PI"
	case FuncPower:
		return "POWER"
	case FuncRadians:
		return "RADIANS"
	case FuncRands:
		return "RANDS"
	case FuncRound:
		return "ROUND"
	case FuncSign:
		return "SIGN"
	case FuncSin:
		return "SIN"
	case FuncSqrt:
		return "SQRT"
	case FuncSquare:
		return "SQUARE"
	case FuncTan:
		return "TAN"
	case FuncFirstValue:
		return "FIRST_VALUE"
	case FuncLastValue:
		return "LAST_VALUE"
	case FuncLag:
		return "LAG"
	case FuncLead:
		return "LEAD"
	case FuncAvg:
		return "AVG"
	case FuncCount:
		return "COUNT"
	case FuncMax:
		return "MAX"
	case FuncMin:
		return "MIN"
	case FuncStdev:
		return "STDEV"
	case FuncStdevp:
		return "STDEVP"
	case FuncSum:
		return "SUM"
	case FuncVar:
		return "VAR"
	case FuncVarp:
		return "VARP"
	case FuncGetdate:
		return "GETDATE"
	case FuncChecksum:
		return "CHECKSUM"
	case FuncNewId:
		return "NEWID"
	case FuncUserDefined:
		return e.Name.TokenLiteral()
	default:
		return "unimplemented function type"
	}
}
func (w WindowFrameBound) TokenLiteral() string {
	var str strings.Builder

	switch w.Type {
	case WFBTFollowing:
		str.WriteString(w.Expression.TokenLiteral())
		str.WriteString(" FOLLOWING")
		break
	case WFBTCurrentRow:
		str.WriteString("CURRENT ROW")
		break
	case WFBTPreceding:
		str.WriteString(w.Expression.TokenLiteral())
		str.WriteString(" PRECEDING")
		break
	case WFBTUnboundedPreceding:
		str.WriteString("UNBOUNDED PRECEDING")
		break
	case WFBTUnboundedFollowing:
		str.WriteString("UNBOUNDED FOLLOWING")
		break
	}

	return str.String()
}
func (w WindowFrameClause) TokenLiteral() string {
	var str strings.Builder

	switch w.RowsOrRange {
	case RRTRows:
		str.WriteString(" ROWS ")
		break
	case RRTRange:
		str.WriteString(" RANGE ")
		break
	}

	if w.End != nil {
		str.WriteString("BETWEEN ")
	}

	str.WriteString(w.Start.TokenLiteral())

	if w.End != nil {
		str.WriteString(" AND ")
		str.WriteString(w.End.TokenLiteral())
	}

	return str.String()
}
func (e FunctionOverClause) TokenLiteral() string {
	var str strings.Builder

	str.WriteString("(")

	if len(e.PartitionByClause) > 0 {
		str.WriteString("PARTITION BY ")
		var expressions []string
		for _, p := range e.PartitionByClause {
			expressions = append(expressions, p.TokenLiteral())
		}
		str.WriteString(strings.Join(expressions, ", "))
	}

	if len(e.OrderByClause) > 0 {
		str.WriteString(" ORDER BY ")
		var args []string
		for _, o := range e.OrderByClause {
			args = append(args, o.TokenLiteral())
		}
		str.WriteString(strings.Join(args, ", "))
	}

	if e.WindowFrameClause != nil {
		str.WriteString(e.WindowFrameClause.TokenLiteral())
	}

	str.WriteString(")")

	return str.String()
}
func (e ExprFunctionCall) TokenLiteral() string {
	var str strings.Builder
	str.WriteString(e.Name.TokenLiteral())
	str.WriteString("(")
	for i, item := range e.Args {
		if i > 0 {
			str.WriteString(", ")
		}
		str.WriteString(item.TokenLiteral())
	}
	str.WriteString(")")

	if e.OverClause != nil {
		str.WriteString(e.OverClause.TokenLiteral())
	}

	return str.String()
}
func (e ExprCast) TokenLiteral() string {
	var str strings.Builder

	str.WriteString("CAST(")
	str.WriteString(e.Expression.TokenLiteral())
	str.WriteString(" AS ")
	str.WriteString(e.DataType.TokenLiteral())
	str.WriteString(")")

	return str.String()
}
func (cte *CommonTableExpression) TokenLiteral() string {
	var str strings.Builder
	str.WriteString(fmt.Sprintf("WITH AS %s", cte.Name))
	if cte.Columns != nil {
		str.WriteString(cte.Columns.TokenLiteral())
	}
	str.WriteString(" ( ")
	str.WriteString(cte.Query.TokenLiteral())
	str.WriteString(" )")
	return str.String()
}

func (e *ExprStringLiteral) SetSpan(span Span)       { e.Span = span }
func (e *ExprNumberLiteral) SetSpan(span Span)       { e.Span = span }
func (e *ExprLocalVariable) SetSpan(span Span)       { e.Span = span }
func (e *ExprIdentifier) SetSpan(span Span)          { e.Span = span }
func (e *ExprQuotedIdentifier) SetSpan(span Span)    { e.Span = span }
func (e *ExprStar) SetSpan(span Span)                { e.Span = span }
func (e *ExprWithAlias) SetSpan(span Span)           { e.Span = span }
func (e *ExprCompoundIdentifier) SetSpan(span Span)  { e.Span = span }
func (si *SelectItems) SetSpan(span Span)            { si.Span = span }
func (w *WhereClause) SetSpan(span Span)             { w.Span = span }
func (h *HavingClause) SetSpan(span Span)            { h.Span = span }
func (gb *GroupByClause) SetSpan(span Span)          { gb.Span = span }
func (ta *TableArg) SetSpan(span Span)               { ta.Span = span }
func (ts *TableSource) SetSpan(span Span)            { ts.Span = span }
func (j *Join) SetSpan(span Span)                    { j.Span = span }
func (ta *TopArg) SetSpan(span Span)                 { ta.Span = span }
func (o *OrderByClause) SetSpan(span Span)           { o.Span = span }
func (o *OffsetFetchClause) SetSpan(span Span)       { o.Span = span }
func (o *OrderByArg) SetSpan(span Span)              { o.Span = span }
func (o *OffsetArg) SetSpan(span Span)               { o.Span = span }
func (f *FetchArg) SetSpan(span Span)                { f.Span = span }
func (e *ExprSubquery) SetSpan(span Span)            { e.Span = span }
func (e *ExprExpressionList) SetSpan(span Span)      { e.Span = span }
func (e *ExprFunction) SetSpan(span Span)            { e.Span = span }
func (w *WindowFrameBound) SetSpan(span Span)        { w.Span = span }
func (w *WindowFrameClause) SetSpan(span Span)       { w.Span = span }
func (e *FunctionOverClause) SetSpan(span Span)      { e.Span = span }
func (e *ExprFunctionCall) SetSpan(span Span)        { e.Span = span }
func (e *ExprCast) SetSpan(span Span)                { e.Span = span }
func (cte *CommonTableExpression) SetSpan(span Span) { cte.Span = span }

func (e ExprStringLiteral) GetSpan() Span        { return e.Span }
func (e ExprNumberLiteral) GetSpan() Span        { return e.Span }
func (e ExprLocalVariable) GetSpan() Span        { return e.Span }
func (e ExprIdentifier) GetSpan() Span           { return e.Span }
func (e ExprQuotedIdentifier) GetSpan() Span     { return e.Span }
func (e ExprStar) GetSpan() Span                 { return e.Span }
func (e ExprWithAlias) GetSpan() Span            { return e.Span }
func (e ExprCompoundIdentifier) GetSpan() Span   { return e.Span }
func (si SelectItems) GetSpan() Span             { return si.Span }
func (w WhereClause) GetSpan() Span              { return w.Span }
func (h HavingClause) GetSpan() Span             { return h.Span }
func (gb GroupByClause) GetSpan() Span           { return gb.Span }
func (ta TableArg) GetSpan() Span                { return ta.Span }
func (ts TableSource) GetSpan() Span             { return ts.Span }
func (j Join) GetSpan() Span                     { return j.Span }
func (ta TopArg) GetSpan() Span                  { return ta.Span }
func (o OrderByClause) GetSpan() Span            { return o.Span }
func (o OffsetFetchClause) GetSpan() Span        { return o.Span }
func (o OrderByArg) GetSpan() Span               { return o.Span }
func (o OffsetArg) GetSpan() Span                { return o.Span }
func (f FetchArg) GetSpan() Span                 { return f.Span }
func (e ExprSubquery) GetSpan() Span             { return e.Span }
func (e ExprExpressionList) GetSpan() Span       { return e.Span }
func (e ExprFunction) GetSpan() Span             { return e.Span }
func (w WindowFrameBound) GetSpan() Span         { return w.Span }
func (w WindowFrameClause) GetSpan() Span        { return w.Span }
func (e FunctionOverClause) GetSpan() Span       { return e.Span }
func (e ExprFunctionCall) GetSpan() Span         { return e.Span }
func (e ExprCast) GetSpan() Span                 { return e.Span }
func (cte *CommonTableExpression) GetSpan() Span { return cte.Span }

type TableSourceType uint8

const (
	TSTTable TableSourceType = iota
	TSTDerived
	TSTTableValuedFunction
)

type JoinType uint8

const (
	JTInner JoinType = iota
	JTLeft
	JTLeftOuter
	JTRight
	JTRightOuter
	JTFull
	JTFullOuter
)

type OrderByType uint8

const (
	OBNone OrderByType = iota
	OBAsc
	OBDesc
)

type RowOrRows uint8

const (
	RRRow RowOrRows = iota
	RRRows
)

type NextOrFirst uint8

const (
	NFNext NextOrFirst = iota
	NFFirst
)

type FuncType uint8

const (
	FuncDenseRank FuncType = iota
	FuncRank
	FuncRowNumber
	FuncAbs
	FuncAcos
	FuncAsin
	FuncAtan
	FuncCeiling
	FuncCos
	FuncCot
	FuncDegrees
	FuncExp
	FuncFloor
	FuncLog
	FuncLog10
	FuncPi
	FuncPower
	FuncRadians
	FuncRands
	FuncRound
	FuncSign
	FuncSin
	FuncSqrt
	FuncSquare
	FuncTan
	FuncFirstValue
	FuncLastValue
	FuncLag
	FuncLead
	FuncAvg
	FuncCount
	FuncMax
	FuncMin
	FuncStdev
	FuncStdevp
	FuncSum
	FuncVar
	FuncVarp
	FuncGetdate
	FuncChecksum
	FuncNewId
	FuncUserDefined
)

type WindowFrameBoundType uint8

const (
	WFBTCurrentRow WindowFrameBoundType = iota
	WFBTPreceding
	WFBTFollowing
	WFBTUnboundedPreceding
	WFBTUnboundedFollowing
)

type RowsOrRangeType uint8

const (
	RRTRows RowsOrRangeType = iota
	RRTRange
)
