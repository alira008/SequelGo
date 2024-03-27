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
    Left Expression
    Right Expression
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
