package ast

import (
	"SequelGo/internal/lexer"
	"fmt"
	"strings"
)

type ExprStringLiteral struct {
	BaseNode
	Value string
}

type ExprNumberLiteral struct {
	BaseNode
	Value string
}

type ExprLocalVariable struct {
	BaseNode
	Value string
}

type ExprIdentifier struct {
	BaseNode
	Value string
}

type ExprQuotedIdentifier struct {
	BaseNode
	Value string
}

type ExprStar struct{
	BaseNode
}

type ExprWithAlias struct {
	BaseNode
	Expression     Expression
	AsTokenPresent bool
	Alias          Expression
}

type ExprCompoundIdentifier struct {
	BaseNode
	Identifiers []Expression
}

type ExprSubquery struct {
	BaseNode
	Distinct      bool
	Top           *TopArg
	SelectItems   []Expression
	Table         *TableArg
	WhereClause   Expression
	GroupByClause []Expression
	HavingClause  Expression
	OrderByClause *OrderByClause
}

type ExprExpressionList struct {
	BaseNode
	List []Expression
}

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

var BuiltinFunctionsTokenType = []lexer.TokenType{
	lexer.TDenseRank, lexer.TRank, lexer.TRowNumber, lexer.TAbs, lexer.TAcos, lexer.TAsin,
	lexer.TAtan, lexer.TCeiling, lexer.TCos, lexer.TCot, lexer.TDegrees, lexer.TExp, lexer.TFloor,
	lexer.TLog, lexer.TLog10, lexer.TPi, lexer.TPower, lexer.TRadians, lexer.TRands, lexer.TRound,
	lexer.TSign, lexer.TSin, lexer.TSqrt, lexer.TSquare, lexer.TTan, lexer.TFirstValue,
	lexer.TLastValue, lexer.TLag, lexer.TLead, lexer.TAvg, lexer.TCount, lexer.TMax, lexer.TMin,
	lexer.TStdev, lexer.TStdevp, lexer.TSum, lexer.TVar, lexer.TVarp, lexer.TGetdate,
	lexer.TChecksum, lexer.TNewId,
}

type FunctionOverClause struct {
	BaseNode
	PartitionByClause []Expression
	OrderByClause     []OrderByArg
	WindowFrameClause *WindowFrameClause
}

type WindowFrameClause struct {
	BaseNode
	RowsOrRange RowsOrRangeType
	Start       *WindowFrameBound
	End         *WindowFrameBound
}

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

type WindowFrameBound struct {
	BaseNode
	Type       WindowFrameBoundType
	Expression Expression
}

type ExprFunction struct {
	BaseNode
	Type FuncType
	Name Expression
}

type ExprFunctionCall struct {
	BaseNode
	Name       *ExprFunction
	Args       []Expression
	OverClause *FunctionOverClause
}

type ExprCast struct {
	BaseNode
	Expression Expression
	DataType   DataType
}

func (e ExprStringLiteral) expressionNode() {}
func (e ExprStringLiteral) TokenLiteral() string {
	return fmt.Sprintf("'%s'", e.Value)
}
func (e ExprStringLiteral) SetBaseNode(baseNode BaseNode) {
    e.BaseNode = baseNode
}

func (e ExprNumberLiteral) expressionNode() {}
func (e ExprNumberLiteral) TokenLiteral() string {
	return e.Value
}
func (e ExprNumberLiteral) SetBaseNode(baseNode BaseNode) {
    e.BaseNode = baseNode
}

func (e ExprLocalVariable) expressionNode() {}
func (e ExprLocalVariable) TokenLiteral() string {
	return fmt.Sprintf("@%s", e.Value)
}
func (e ExprLocalVariable) SetBaseNode(baseNode BaseNode) {
    e.BaseNode = baseNode
}

func (e ExprIdentifier) expressionNode() {}
func (e ExprIdentifier) TokenLiteral() string {
	return e.Value
}
func (e ExprIdentifier) SetBaseNode(baseNode BaseNode) {
    e.BaseNode = baseNode
}

func (e ExprQuotedIdentifier) expressionNode() {}
func (e ExprQuotedIdentifier) TokenLiteral() string {
	return fmt.Sprintf("[%s]", e.Value)
}
func (e ExprQuotedIdentifier) SetBaseNode(baseNode BaseNode) {
    e.BaseNode = baseNode
}

func (e ExprStar) expressionNode() {}
func (e ExprStar) TokenLiteral() string {
	return "*"
}
func (e ExprStar) SetBaseNode(baseNode BaseNode) {
    e.BaseNode = baseNode
}

func (e ExprWithAlias) expressionNode() {}
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
func (e ExprWithAlias) SetBaseNode(baseNode BaseNode) {
    e.BaseNode = baseNode
}

func (e ExprCompoundIdentifier) expressionNode() {}
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
func (e ExprCompoundIdentifier) SetBaseNode(baseNode BaseNode) {
    e.BaseNode = baseNode
}

func (e ExprSubquery) expressionNode() {}
func (e ExprSubquery) TokenLiteral() string {
	var str strings.Builder
	str.WriteString("(")
	str.WriteString("SELECT ")

	if e.Distinct {
		str.WriteString("DISTINCT ")
	}

	if e.Top != nil {
		str.WriteString(fmt.Sprintf("%s ", e.Top.TokenLiteral()))
	}

	var selectItems []string
	for _, s := range e.SelectItems {
		selectItems = append(selectItems, s.TokenLiteral())
	}
	str.WriteString(strings.Join(selectItems, ", "))

	if e.Table != nil {
		str.WriteString(" FROM ")
		str.WriteString(e.Table.TokenLiteral())
	}

	if e.WhereClause != nil {
		str.WriteString(" WHERE ")
		str.WriteString(e.WhereClause.TokenLiteral())
	}

	var groupByArgs []string
	for _, g := range e.GroupByClause {
		groupByArgs = append(groupByArgs, g.TokenLiteral())
	}
	if len(groupByArgs) > 1 {
		str.WriteString(strings.Join(groupByArgs, ", "))
	}

	if e.HavingClause != nil {
		str.WriteString(" HAVING ")
		str.WriteString(e.HavingClause.TokenLiteral())
	}

	if e.OrderByClause != nil {
		str.WriteString(e.OrderByClause.TokenLiteral())
	}

	str.WriteString(")")
	return str.String()
}
func (e ExprSubquery) SetBaseNode(baseNode BaseNode) {
    e.BaseNode = baseNode
}

func (e ExprExpressionList) expressionNode() {}
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
func (e ExprExpressionList) SetBaseNode(baseNode BaseNode) {
    e.BaseNode = baseNode
}

func (e ExprFunction) expressionNode() {}
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
func (e ExprFunction) SetBaseNode(baseNode BaseNode) {
    e.BaseNode = baseNode
}

func (w WindowFrameBound) expressionNode() {}
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
func (w WindowFrameBound) SetBaseNode(baseNode BaseNode) {
    w.BaseNode = baseNode
}

func (w WindowFrameClause) expressionNode() {}
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
func (w WindowFrameClause) SetBaseNode(baseNode BaseNode) {
    w.BaseNode = baseNode
}

func (e FunctionOverClause) expressionNode() {}
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
func (e FunctionOverClause) SetBaseNode(baseNode BaseNode) {
    e.BaseNode = baseNode
}

func (e ExprFunctionCall) expressionNode() {}
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
func (e ExprFunctionCall) SetBaseNode(baseNode BaseNode) {
    e.BaseNode = baseNode
}

func (e ExprCast) expressionNode() {}
func (e ExprCast) TokenLiteral() string {
	var str strings.Builder

	str.WriteString("CAST(")
	str.WriteString(e.Expression.TokenLiteral())
	str.WriteString(" AS ")
	str.WriteString(e.DataType.TokenLiteral())
	str.WriteString(")")

	return str.String()
}
func (e ExprCast) SetBaseNode(baseNode BaseNode) {
    e.BaseNode = baseNode
}
