package ast

import (
	"fmt"
	"strings"
)

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
	Right    Expression
	Operator UnaryOperatorType
}

type ExprComparisonOperator struct {
	Left     Expression
	Right    Expression
	Operator ComparisonOperatorType
}

func (o ComparisonOperatorType) ToString() string {
	var str string
	switch o {
	case ComparisonOpEqual:
		str = "="
	case ComparisonOpGreater:
		str = ">"
	case ComparisonOpGreaterEqual:
		str = ">="
	case ComparisonOpLess:
		str = "<"
	case ComparisonOpLessEqual:
		str = "<="
	case ComparisonOpNotEqualArrow:
		str = "<>"
	case ComparisonOpNotEqualBang:
		str = "!="
	}
	return str
}

type ExprArithmeticOperator struct {
	Left     Expression
	Right    Expression
	Operator ArithmeticOperatorType
}

type ExprAndLogicalOperator struct {
	Left  Expression
	Right Expression
}

type ExprAllLogicalOperator struct {
	ScalarExpression   Expression
	ComparisonOperator ComparisonOperatorType
	Subquery           *ExprSubquery
}

type ExprBetweenLogicalOperator struct {
	TestExpression Expression
	Not            bool
	Begin          Expression
	End            Expression
}

type ExprExistsLogicalOperator struct {
	Subquery *ExprSubquery
}

type ExprInSubqueryLogicalOperator struct {
	TestExpression Expression
	Not            bool
	Subquery       *ExprSubquery
}

type ExprInLogicalOperator struct {
	TestExpression Expression
	Not            bool
	Expressions    []Expression
}

type ExprLikeLogicalOperator struct {
	MatchExpression Expression
	Not             bool
	Pattern         Expression
}

type ExprNotLogicalOperator struct {
	Expression Expression
}

type ExprOrLogicalOperator struct {
	Left  Expression
	Right Expression
}

type ExprSomeLogicalOperator struct {
	ScalarExpression   Expression
	ComparisonOperator ComparisonOperatorType
	Subquery           *ExprSubquery
}

type ExprAnyLogicalOperator struct {
	ScalarExpression   Expression
	ComparisonOperator ComparisonOperatorType
	Subquery           *ExprSubquery
}

func (e ExprUnaryOperator) expressionNode() {}
func (e ExprUnaryOperator) TokenLiteral() string {
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

func (e ExprComparisonOperator) expressionNode() {}
func (e ExprComparisonOperator) TokenLiteral() string {
	var str strings.Builder
	str.WriteString(e.Left.TokenLiteral())
	str.WriteString(fmt.Sprintf(" %s ", e.Operator.ToString()))
	str.WriteString(e.Right.TokenLiteral())
	return str.String()
}

func (e ExprArithmeticOperator) expressionNode() {}
func (e ExprArithmeticOperator) TokenLiteral() string {
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

func (e ExprAndLogicalOperator) expressionNode() {}
func (e ExprAndLogicalOperator) TokenLiteral() string {
	var str strings.Builder
	str.WriteString(e.Left.TokenLiteral())
	str.WriteString(" AND ")
	str.WriteString(e.Right.TokenLiteral())
	return str.String()
}

func (e ExprAllLogicalOperator) expressionNode() {}
func (e ExprAllLogicalOperator) TokenLiteral() string {
	var str strings.Builder
	str.WriteString(e.ScalarExpression.TokenLiteral())
	str.WriteString(fmt.Sprintf(" %s ", e.ComparisonOperator.ToString()))
	str.WriteString(" AND ")
	str.WriteString(e.Subquery.TokenLiteral())
	return str.String()
}

func (e ExprBetweenLogicalOperator) expressionNode() {}
func (e ExprBetweenLogicalOperator) TokenLiteral() string {
	var str strings.Builder
	str.WriteString(e.TestExpression.TokenLiteral())
	if e.Not {
		str.WriteString(" NOT")
	}
	str.WriteString(" BETWEEN ")
	str.WriteString(e.Begin.TokenLiteral())
	str.WriteString(" AND ")
	str.WriteString(e.End.TokenLiteral())
	return str.String()
}

func (e ExprExistsLogicalOperator) expressionNode() {}
func (e ExprExistsLogicalOperator) TokenLiteral() string {
	var str strings.Builder
	str.WriteString("EXISTS (")
	str.WriteString(e.Subquery.TokenLiteral())
	str.WriteString(")")
	return str.String()
}

func (e ExprInSubqueryLogicalOperator) expressionNode() {}
func (e ExprInSubqueryLogicalOperator) TokenLiteral() string {
	var str strings.Builder
	str.WriteString(e.TestExpression.TokenLiteral())
	if e.Not {
		str.WriteString(" NOT")
	}
	str.WriteString(" IN (")
	str.WriteString(e.Subquery.TokenLiteral())
	str.WriteString(")")
	return str.String()
}

func (e ExprInLogicalOperator) expressionNode() {}
func (e ExprInLogicalOperator) TokenLiteral() string {
	var str strings.Builder
	str.WriteString(e.TestExpression.TokenLiteral())
	if e.Not {
		str.WriteString(" NOT")
	}
	str.WriteString(" IN (")

	var strs []string
	for _, expr := range e.Expressions {
		strs = append(strs, expr.TokenLiteral())
	}
	str.WriteString(strings.Join(strs, ", "))

	str.WriteString(")")

	return str.String()
}

func (e ExprLikeLogicalOperator) expressionNode() {}
func (e ExprLikeLogicalOperator) TokenLiteral() string {
	var str strings.Builder
	str.WriteString(e.MatchExpression.TokenLiteral())
	if e.Not {
		str.WriteString(" NOT")
	}
	str.WriteString(" LIKE ")
	str.WriteString(e.Pattern.TokenLiteral())
	return str.String()
}

func (e ExprNotLogicalOperator) expressionNode() {}
func (e ExprNotLogicalOperator) TokenLiteral() string {
	var str strings.Builder
	str.WriteString("NOT ")
	str.WriteString(e.Expression.TokenLiteral())
	return str.String()
}

func (e ExprOrLogicalOperator) expressionNode() {}
func (e ExprOrLogicalOperator) TokenLiteral() string {
	var str strings.Builder
	str.WriteString(e.Left.TokenLiteral())
	str.WriteString(" OR ")
	str.WriteString(e.Right.TokenLiteral())
	return str.String()
}

func (e ExprSomeLogicalOperator) expressionNode() {}
func (e ExprSomeLogicalOperator) TokenLiteral() string {
	var str strings.Builder
	str.WriteString(e.ScalarExpression.TokenLiteral())
	str.WriteString(fmt.Sprintf(" %s ", e.ComparisonOperator.ToString()))
	str.WriteString(" SOME ")
	str.WriteString(e.Subquery.TokenLiteral())
	return str.String()
}

func (e ExprAnyLogicalOperator) expressionNode() {}
func (e ExprAnyLogicalOperator) TokenLiteral() string {
	var str strings.Builder
	str.WriteString(e.ScalarExpression.TokenLiteral())
	str.WriteString(fmt.Sprintf(" %s ", e.ComparisonOperator.ToString()))
	str.WriteString(" ANY ")
	str.WriteString(e.Subquery.TokenLiteral())
	return str.String()
}
