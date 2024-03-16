package ast

import "fmt"

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

func (e *Expr) expressionNode()         {}
func (e *Expr) TokenLiteral() string    { return fmt.Sprintf("'%s'", e.StringLiteral) }
func (e *Expr) IsType(et ExprType) bool { return e.ExprType == et }
