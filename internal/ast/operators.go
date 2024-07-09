package ast

import "strings"

type LogicalOperatorType uint8
type UnaryOperatorType uint8
type ComparisonOperatorType uint8
type ArithmeticOperatorType uint8

const (
	ComparisonOpEqual ComparisonOperatorType = iota
	ComparisonOpNotEqualBang
	ComparisonOpNotEqualArrow
	ComparisonOpGreater
	ComparisonOpGreaterEqual
	ComparisonOpLess
	ComparisonOpLessEqual
)
const (
	ArithmeticOpPlus ArithmeticOperatorType = iota
	ArithmeticOpMinus
	ArithmeticOpMult
	ArithmeticOpDiv
	ArithmeticOpMod
)
const (
	UnaryOpPlus UnaryOperatorType = iota
	UnaryOpMinus
)

type ExprUnaryOperator struct {
	Right     Expression
	Operator UnaryOperatorType
}

func (e *ExprUnaryOperator) expressionNode() {}
func (e *ExprUnaryOperator) TokenLiteral() string {
	var str strings.Builder
	str.WriteString(e.Right.TokenLiteral())

	switch e.Operator {
	case UnaryOpPlus:
		str.WriteString(" + ")
	case UnaryOpMinus:
		str.WriteString(" - ")
	}

	return str.String()
}

type ExprComparisonOperator struct {
	Left     Expression
	Right    Expression
	Operator ComparisonOperatorType
}

func (e *ExprComparisonOperator) expressionNode() {}
func (e *ExprComparisonOperator) TokenLiteral() string {
	var str strings.Builder
	str.WriteString(e.Left.TokenLiteral())
	switch e.Operator {
	case ComparisonOpEqual:
		str.WriteString(" = ")
	case ComparisonOpGreater:
		str.WriteString(" > ")
	case ComparisonOpGreaterEqual:
		str.WriteString(" >= ")
	case ComparisonOpLess:
		str.WriteString(" < ")
	case ComparisonOpLessEqual:
		str.WriteString(" <= ")
	case ComparisonOpNotEqualArrow:
		str.WriteString(" <> ")
	case ComparisonOpNotEqualBang:
		str.WriteString(" != ")
	}
	str.WriteString(e.Right.TokenLiteral())
	return str.String()
}

type ExprArithmeticOperator struct {
	Left     Expression
	Right    Expression
	Operator ArithmeticOperatorType
}

func (e *ExprArithmeticOperator) expressionNode() {}
func (e *ExprArithmeticOperator) TokenLiteral() string {
	var str strings.Builder
	str.WriteString(e.Left.TokenLiteral())
	switch e.Operator {
	case ArithmeticOpPlus:
		str.WriteString(" + ")
	case ArithmeticOpMinus:
		str.WriteString(" - ")
	case ArithmeticOpMult:
		str.WriteString(" * ")
	case ArithmeticOpDiv:
		str.WriteString(" / ")
	case ArithmeticOpMod:
		str.WriteString(" % ")
	}
	str.WriteString(e.Right.TokenLiteral())
	return str.String()
}
	// OpAnd
	// OpOr
	// OpLike
	// OpIn
	// OpBetween
	// OpAny
	// OpAll
	// OpSome

	// unary ops
	// OpNot
	// OpUnaryMinus
	// OpIsTrue
	// OpIsNotTrue
	// OpIsNull
	// OpIsNotNull
	// OpExists
