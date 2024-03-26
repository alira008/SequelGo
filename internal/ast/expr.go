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

type ExprStar struct {
}

func (e *ExprStar) expressionNode() {}
func (e *ExprStar) TokenLiteral() string {
	return "*"
}

type ExprExpressionList struct {
	list []*Expression
}

func (e *ExprExpressionList) expressionNode() {}
func (e *ExprExpressionList) TokenLiteral() string {
	var str strings.Builder
	for i, item := range e.list {
		if i > 0 {
			str.WriteString(", ")
		}

		str.WriteString((*item).TokenLiteral())
	}
	return str.String()
}
