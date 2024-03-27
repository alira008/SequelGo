package ast

import (
	"fmt"
	"strings"
)

type OperatorType uint8

const (
	OpNone OperatorType = iota

	OpCase
	OpCaseItem

	// binary ops
	OpPlus
	OpMinus
	OpMult
	OpDiv
	OpMod
	OpEqual
	OpNotEqual
	OpGreater
	OpGreaterEqual
	OpLess
	OpLessEqual
	OpAnd
	OpOr
	OpLike
	OpIn
	OpBetween
	OpAny
	OpAll
	OpSome

	// unary ops
	OpNot
	OpUnaryMinus
	OpIsTrue
	OpIsNotTrue
	OpIsNull
	OpIsNotNull
	OpExists
)

type ExprStringLiteral struct {
	Value string
}

func (e *ExprStringLiteral) expressionNode() {}
func (e *ExprStringLiteral) TokenLiteral() string {
	return e.Value
}

type ExprNumberLiteral struct {
	Value string
}

func (e *ExprNumberLiteral) expressionNode() {}
func (e *ExprNumberLiteral) TokenLiteral() string {
	return e.Value
}

type ExprLocalVariable struct {
	Value string
}

func (e *ExprLocalVariable) expressionNode() {}
func (e *ExprLocalVariable) TokenLiteral() string {
	return fmt.Sprintf("@%s", e.Value)
}

type ExprIdentifier struct {
	Value string
}

func (e *ExprIdentifier) expressionNode() {}
func (e *ExprIdentifier) TokenLiteral() string {
	return e.Value
}

type ExprQuotedIdentifier struct {
	Value string
}

func (e *ExprQuotedIdentifier) expressionNode() {}
func (e *ExprQuotedIdentifier) TokenLiteral() string {
	return fmt.Sprintf("[%s]", e.Value)
}

type ExprStar struct{}

func (e *ExprStar) expressionNode() {}
func (e *ExprStar) TokenLiteral() string {
	return "*"
}

type ExprCompoundIdentifier struct {
	Identifiers []Expression
}

func (e *ExprCompoundIdentifier) expressionNode() {}
func (e *ExprCompoundIdentifier) TokenLiteral() string {
	var str strings.Builder
	for i, item := range e.Identifiers {
		if i > 0 {
			str.WriteString(".")
		}
		str.WriteString(item.TokenLiteral())
	}
	return str.String()
}

type ExprExpressionList struct {
	List []*Expression
}

func (e *ExprExpressionList) expressionNode() {}
func (e *ExprExpressionList) TokenLiteral() string {
	var str strings.Builder
	for i, item := range e.List {
		if i > 0 {
			str.WriteString(", ")
		}

		str.WriteString((*item).TokenLiteral())
	}
	return str.String()
}

type ExprBinary struct {
	Left     Expression
	Right    Expression
	Operator OperatorType
}

func (e *ExprBinary) expressionNode() {}
func (e *ExprBinary) TokenLiteral() string {
	var str strings.Builder
	str.WriteString(e.Left.TokenLiteral())
	switch e.Operator {
	case OpPlus:
		str.WriteString(" + ")
	case OpMinus:
		str.WriteString(" - ")
	case OpMult:
		str.WriteString(" * ")
	case OpDiv:
		str.WriteString(" / ")
	case OpMod:
		str.WriteString(" % ")
	case OpEqual:
		str.WriteString(" = ")
	case OpGreater:
		str.WriteString(" > ")
	case OpGreaterEqual:
		str.WriteString(" >= ")
	case OpLess:
		str.WriteString(" < ")
	case OpLessEqual:
		str.WriteString(" <= ")
	case OpAnd:
		str.WriteString(" AND ")
	case OpOr:
		str.WriteString(" OR ")
	case OpLike:
		str.WriteString(" LIKE ")
	case OpIn:
		str.WriteString(" IN ")
	// case OpBetween:
	//     str.WriteString(" BETWEEN ")
	// case OpAny:
	//     str.WriteString(" ANY ")
	// case OpAll:
	//     str.WriteString(" ALL ")
	// case OpSome:
	//     str.WriteString(" SOME ")
	default:
		str.WriteString(" ??? ")
	}
	str.WriteString(e.Right.TokenLiteral())
	return str.String()
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
	FuncUserDefined
)

type ExprFunction struct {
	Type FuncType
	Name Expression
}

func (e *ExprFunction) expressionNode() {}
func (e *ExprFunction) TokenLiteral() string {
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
	case FuncUserDefined:
		return e.Name.TokenLiteral()
	default:
		return "unimplemented function type"
	}
}

type ExprFunctionCall struct {
	Name *ExprFunction
	Args []Expression
}

func (e *ExprFunctionCall) expressionNode() {}
func (e *ExprFunctionCall) TokenLiteral() string {
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
	return str.String()
}
