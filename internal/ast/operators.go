package ast

import (
	"fmt"
	"strings"
)

type ExprUnaryOperator struct {
	Span
	Right    Expression
	Operator UnaryOperatorType
}

type ExprComparisonOperator struct {
	Span
	Left     Expression
	Right    Expression
	Operator ComparisonOperatorType
}

type ExprArithmeticOperator struct {
	Span
	Left     Expression
	Right    Expression
	Operator ArithmeticOperatorType
}

type ExprAndLogicalOperator struct {
	Span
	Left  Expression
	Right Expression
}

type ExprAllLogicalOperator struct {
	Span
	ScalarExpression   Expression
	ComparisonOperator ComparisonOperatorType
	Subquery           *ExprSubquery
}

type ExprBetweenLogicalOperator struct {
	Span
	TestExpression Expression
	Not            bool
	Begin          Expression
	End            Expression
}

type ExprExistsLogicalOperator struct {
	Span
	Subquery *ExprSubquery
}

type ExprInSubqueryLogicalOperator struct {
	Span
	TestExpression Expression
	Not            bool
	Subquery       *ExprSubquery
}

type ExprInLogicalOperator struct {
	Span
	TestExpression Expression
	Not            bool
	Expressions    []Expression
}

type ExprLikeLogicalOperator struct {
	Span
	MatchExpression Expression
	Not             bool
	Pattern         Expression
}

type ExprNotLogicalOperator struct {
	Span
	Expression Expression
}

type ExprOrLogicalOperator struct {
	Span
	Left  Expression
	Right Expression
}

type ExprSomeLogicalOperator struct {
	Span
	ScalarExpression   Expression
	ComparisonOperator ComparisonOperatorType
	Subquery           *ExprSubquery
}

type ExprAnyLogicalOperator struct {
	Span
	ScalarExpression   Expression
	ComparisonOperator ComparisonOperatorType
	Subquery           *ExprSubquery
}

func (e ExprUnaryOperator) expressionNode()             {}
func (e ExprComparisonOperator) expressionNode()        {}
func (e ExprArithmeticOperator) expressionNode()        {}
func (e ExprAndLogicalOperator) expressionNode()        {}
func (e ExprAllLogicalOperator) expressionNode()        {}
func (e ExprBetweenLogicalOperator) expressionNode()    {}
func (e ExprExistsLogicalOperator) expressionNode()     {}
func (e ExprInSubqueryLogicalOperator) expressionNode() {}
func (e ExprInLogicalOperator) expressionNode()         {}
func (e ExprLikeLogicalOperator) expressionNode()       {}
func (e ExprNotLogicalOperator) expressionNode()        {}
func (e ExprOrLogicalOperator) expressionNode()         {}
func (e ExprSomeLogicalOperator) expressionNode()       {}
func (e ExprAnyLogicalOperator) expressionNode()        {}

func (e ExprUnaryOperator) TokenLiteral() string {
	var str strings.Builder

	switch e.Operator {
	case UnaryOpPlus:
		str.WriteString(" + ")
	case UnaryOpMinus:
		str.WriteString(" - ")
	}

	str.WriteString(e.Right.TokenLiteral())
	return str.String()
}
func (e ExprComparisonOperator) TokenLiteral() string {
	var str strings.Builder
	str.WriteString(e.Left.TokenLiteral())
	str.WriteString(fmt.Sprintf(" %s ", e.Operator.TokenLiteral()))
	str.WriteString(e.Right.TokenLiteral())
	return str.String()
}
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
func (e ExprAndLogicalOperator) TokenLiteral() string {
	var str strings.Builder
	str.WriteString(e.Left.TokenLiteral())
	str.WriteString(" AND ")
	str.WriteString(e.Right.TokenLiteral())
	return str.String()
}
func (e ExprAllLogicalOperator) TokenLiteral() string {
	var str strings.Builder
	str.WriteString(e.ScalarExpression.TokenLiteral())
	str.WriteString(fmt.Sprintf(" %s ", e.ComparisonOperator.TokenLiteral()))
	str.WriteString(" AND ")
	str.WriteString(e.Subquery.TokenLiteral())
	return str.String()
}
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
func (e ExprExistsLogicalOperator) TokenLiteral() string {
	var str strings.Builder
	str.WriteString("EXISTS (")
	str.WriteString(e.Subquery.TokenLiteral())
	str.WriteString(")")
	return str.String()
}
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
func (e ExprNotLogicalOperator) TokenLiteral() string {
	var str strings.Builder
	str.WriteString("NOT ")
	str.WriteString(e.Expression.TokenLiteral())
	return str.String()
}
func (e ExprOrLogicalOperator) TokenLiteral() string {
	var str strings.Builder
	str.WriteString(e.Left.TokenLiteral())
	str.WriteString(" OR ")
	str.WriteString(e.Right.TokenLiteral())
	return str.String()
}
func (e ExprSomeLogicalOperator) TokenLiteral() string {
	var str strings.Builder
	str.WriteString(e.ScalarExpression.TokenLiteral())
	str.WriteString(fmt.Sprintf(" %s ", e.ComparisonOperator.TokenLiteral()))
	str.WriteString(" SOME ")
	str.WriteString(e.Subquery.TokenLiteral())
	return str.String()
}
func (e ExprAnyLogicalOperator) TokenLiteral() string {
	var str strings.Builder
	str.WriteString(e.ScalarExpression.TokenLiteral())
	str.WriteString(fmt.Sprintf(" %s ", e.ComparisonOperator.TokenLiteral()))
	str.WriteString(" ANY ")
	str.WriteString(e.Subquery.TokenLiteral())
	return str.String()
}
func (o ComparisonOperatorType) TokenLiteral() string {
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

func (e *ExprUnaryOperator) SetSpan(span Span)             { e.Span = span }
func (e *ExprComparisonOperator) SetSpan(span Span)        { e.Span = span }
func (e *ExprArithmeticOperator) SetSpan(span Span)        { e.Span = span }
func (e *ExprAndLogicalOperator) SetSpan(span Span)        { e.Span = span }
func (e *ExprAllLogicalOperator) SetSpan(span Span)        { e.Span = span }
func (e *ExprBetweenLogicalOperator) SetSpan(span Span)    { e.Span = span }
func (e *ExprExistsLogicalOperator) SetSpan(span Span)     { e.Span = span }
func (e *ExprInSubqueryLogicalOperator) SetSpan(span Span) { e.Span = span }
func (e *ExprInLogicalOperator) SetSpan(span Span)         { e.Span = span }
func (e *ExprLikeLogicalOperator) SetSpan(span Span)       { e.Span = span }
func (e *ExprNotLogicalOperator) SetSpan(span Span)        { e.Span = span }
func (e *ExprOrLogicalOperator) SetSpan(span Span)         { e.Span = span }
func (e *ExprSomeLogicalOperator) SetSpan(span Span)       { e.Span = span }
func (e *ExprAnyLogicalOperator) SetSpan(span Span)        { e.Span = span }

func (e ExprUnaryOperator) GetSpan() Span             { return e.Span }
func (e ExprComparisonOperator) GetSpan() Span        { return e.Span }
func (e ExprArithmeticOperator) GetSpan() Span        { return e.Span }
func (e ExprAndLogicalOperator) GetSpan() Span        { return e.Span }
func (e ExprAllLogicalOperator) GetSpan() Span        { return e.Span }
func (e ExprBetweenLogicalOperator) GetSpan() Span    { return e.Span }
func (e ExprExistsLogicalOperator) GetSpan() Span     { return e.Span }
func (e ExprInSubqueryLogicalOperator) GetSpan() Span { return e.Span }
func (e ExprInLogicalOperator) GetSpan() Span         { return e.Span }
func (e ExprLikeLogicalOperator) GetSpan() Span       { return e.Span }
func (e ExprNotLogicalOperator) GetSpan() Span        { return e.Span }
func (e ExprOrLogicalOperator) GetSpan() Span         { return e.Span }
func (e ExprSomeLogicalOperator) GetSpan() Span       { return e.Span }
func (e ExprAnyLogicalOperator) GetSpan() Span        { return e.Span }

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
