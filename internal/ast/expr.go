package ast

import (
	"fmt"
	"strings"
)

type ExprType uint8

const (
	ExprLiteralNumber ExprType = iota
	ExprLiteralBool
	ExprLiteralString
	ExprLiteralNull
	ExprLiteralQuotedString
	ExprLocalVar
	ExprIdentifier
	ExprQuotedIdentifier
	ExprSelect
	ExprStar
	ExprParameter
	ExprColumn
	ExprColumnRef
	ExprGrouping
	ExprExpressionList
	ExprFunction
	ExprCast
	ExprOperator
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

type Expr struct {
	ExprType     ExprType
	OperatorType OperatorType

	Expr                *Expr
	Expr2               *Expr
	ExprList            []*Expr
	NumberLiteral       float64
	StringLiteral       string
	QuotedStringLiteral string
	BoolLiteral         bool
	Identifier          string
	QuotedIdentifier    string
}

func (e *Expr) expressionNode() {}
func (e *Expr) TokenLiteral() string {
	switch e.ExprType {
	case ExprLiteralNumber:
		return fmt.Sprintf("%f", e.NumberLiteral)
	case ExprLiteralBool:
		return fmt.Sprintf("%t", e.BoolLiteral)
	case ExprLiteralString:
		return e.StringLiteral
	case ExprLiteralNull:
		return "NULL"
	case ExprLiteralQuotedString:
		return fmt.Sprintf("'%s'", e.QuotedStringLiteral)
	case ExprLocalVar:
		return fmt.Sprintf("@%s", e.Identifier)
	case ExprIdentifier:
		return e.Identifier
	case ExprQuotedIdentifier:
		return e.QuotedIdentifier
	case ExprStar:
		return "*"
	case ExprParameter:
		return "?"
	case ExprColumn:
		return e.Identifier
	case ExprColumnRef:
		return fmt.Sprintf("%s.%s", e.Expr.TokenLiteral(), e.Identifier)
	case ExprGrouping:
		return fmt.Sprintf("(%s)", e.Expr.TokenLiteral())
	case ExprExpressionList:
		var str strings.Builder
		for i, e := range e.ExprList {
			str.WriteString(e.TokenLiteral())
			if i < len(e.ExprList)-1 {

				str.WriteString(", ")
			}
		}
		return str.String()
	default:
		return "unknown"
	}
}
func (e *Expr) IsType(et ExprType) bool { return e.ExprType == et }
