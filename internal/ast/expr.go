package ast

import (
	"fmt"
	"strings"
)

type ExprStringLiteral struct {
	Value string
}

type ExprNumberLiteral struct {
	Value string
}

type ExprLocalVariable struct {
	Value string
}

type ExprIdentifier struct {
	Value string
}

type ExprQuotedIdentifier struct {
	Value string
}

type ExprStar struct{}

type ExprWithAlias struct {
	Expression     Expression
	AsTokenPresent bool
	Alias          Expression
}

type ExprCompoundIdentifier struct {
	Identifiers []Expression
}

type ExprSubquery struct {
	Distinct    bool
	Top         *TopArg
	SelectItems  []Expression
	TableObject Expression
	WhereClause Expression
}

type ExprExpressionList struct {
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
	FuncUserDefined
)

type ExprFunction struct {
	Type FuncType
	Name Expression
}

type ExprFunctionCall struct {
	Name *ExprFunction
	Args []Expression
}

func (e ExprStringLiteral) expressionNode() {}
func (e ExprStringLiteral) TokenLiteral() string {
	return fmt.Sprintf("'%s'", e.Value)
}

func (e ExprNumberLiteral) expressionNode() {}
func (e ExprNumberLiteral) TokenLiteral() string {
	return e.Value
}

func (e ExprLocalVariable) expressionNode() {}
func (e ExprLocalVariable) TokenLiteral() string {
	return fmt.Sprintf("@%s", e.Value)
}

func (e ExprIdentifier) expressionNode() {}
func (e ExprIdentifier) TokenLiteral() string {
	return e.Value
}

func (e ExprQuotedIdentifier) expressionNode() {}
func (e ExprQuotedIdentifier) TokenLiteral() string {
	return fmt.Sprintf("[%s]", e.Value)
}

func (e ExprStar) expressionNode() {}
func (e ExprStar) TokenLiteral() string {
	return "*"
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

func (e ExprSubquery) expressionNode() {}
func (e ExprSubquery) TokenLiteral() string {
	var str strings.Builder
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

	if e.TableObject != nil {
		str.WriteString(" FROM ")
		str.WriteString(e.TableObject.TokenLiteral())
	}

	if e.WhereClause != nil {
		str.WriteString(" WHERE ")
		str.WriteString(e.WhereClause.TokenLiteral())
	}

	fmt.Printf("subquery statement %s\n", str.String())
	return str.String()
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
	case FuncUserDefined:
		return e.Name.TokenLiteral()
	default:
		return "unimplemented function type"
	}
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
	return str.String()
}
