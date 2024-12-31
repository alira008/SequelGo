package ast

import (
	"fmt"
	"strings"
)

type ExprUnaryOperator struct {
	Span
	Right            Expression
	Operator         UnaryOperatorType
}

type ExprComparisonOperator struct {
	Span
	Left             Expression
	Right            Expression
	Operator         ComparisonOperatorType
}

type ExprArithmeticOperator struct {
	Span
	Left             Expression
	Right            Expression
	Operator         ArithmeticOperatorType
}

type ExprAndLogicalOperator struct {
	Span
	AndKeyword       Keyword
	Left             Expression
	Right            Expression
}

type ExprAllLogicalOperator struct {
	Span
	AllKeyword         Keyword
	ScalarExpression   Expression
	ComparisonOperator ComparisonOperatorType
	Subquery           *ExprSubquery
}

type ExprBetweenLogicalOperator struct {
	Span
	BetweenKeyword   Keyword
	TestExpression   Expression
	NotKeyword       *Keyword
	Begin            Expression
	AndKeyword       Keyword
	End              Expression
}

type ExprExistsLogicalOperator struct {
	Span
	ExistsKeyword    Keyword
	Subquery         *ExprSubquery
}

type ExprInSubqueryLogicalOperator struct {
	Span
	InKeyword        Keyword
	TestExpression   Expression
	NotKeyword       *Keyword
	Subquery         *ExprSubquery
}

type ExprInLogicalOperator struct {
	Span
	InKeyword        Keyword
	TestExpression   Expression
	NotKeyword       *Keyword
	Expressions      []Expression
}

type ExprLikeLogicalOperator struct {
	Span
	LikeKeyword      Keyword
	MatchExpression  Expression
	NotKeyword       *Keyword
	Pattern          Expression
}

type ExprNotLogicalOperator struct {
	Span
	NotKeyword       Keyword
	Expression       Expression
}

type ExprOrLogicalOperator struct {
	Span
	OrKeyword        Keyword
	Left             Expression
	Right            Expression
}

type ExprSomeLogicalOperator struct {
	Span
	SomeKeyword        Keyword
	ScalarExpression   Expression
	ComparisonOperator ComparisonOperatorType
	Subquery           *ExprSubquery
}

type ExprAnyLogicalOperator struct {
	Span
	AnyKeyword         Keyword
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
	str.WriteString(fmt.Sprintf(" %s ", e.AndKeyword.TokenLiteral()))
	str.WriteString(e.Right.TokenLiteral())
	return str.String()
}
func (e ExprAllLogicalOperator) TokenLiteral() string {
	var str strings.Builder
	str.WriteString(e.ScalarExpression.TokenLiteral())
	str.WriteString(fmt.Sprintf(" %s ", e.ComparisonOperator.TokenLiteral()))
	str.WriteString(fmt.Sprintf(" %s ", e.AllKeyword.TokenLiteral()))
	str.WriteString(e.Subquery.TokenLiteral())
	return str.String()
}
func (e ExprBetweenLogicalOperator) TokenLiteral() string {
	var str strings.Builder
	str.WriteString(e.TestExpression.TokenLiteral())
	if e.NotKeyword != nil {
		str.WriteString(fmt.Sprintf(" %s", e.NotKeyword.TokenLiteral()))
	}
	str.WriteString(fmt.Sprintf(" %s ", e.BetweenKeyword.TokenLiteral()))
	str.WriteString(e.Begin.TokenLiteral())
	str.WriteString(fmt.Sprintf(" %s ", e.AndKeyword.TokenLiteral()))
	str.WriteString(e.End.TokenLiteral())
	return str.String()
}
func (e ExprExistsLogicalOperator) TokenLiteral() string {
	var str strings.Builder
	str.WriteString(fmt.Sprintf("%s (", e.ExistsKeyword.TokenLiteral()))
	str.WriteString(e.Subquery.TokenLiteral())
	str.WriteString(")")
	return str.String()
}
func (e ExprInSubqueryLogicalOperator) TokenLiteral() string {
	var str strings.Builder
	str.WriteString(e.TestExpression.TokenLiteral())
	if e.NotKeyword != nil {
		str.WriteString(fmt.Sprintf(" %s", e.NotKeyword.TokenLiteral()))
	}
	str.WriteString(fmt.Sprintf(" %s (", e.InKeyword.TokenLiteral()))
	str.WriteString(e.Subquery.TokenLiteral())
	str.WriteString(")")
	return str.String()
}
func (e ExprInLogicalOperator) TokenLiteral() string {
	var str strings.Builder
	str.WriteString(e.TestExpression.TokenLiteral())
	if e.NotKeyword != nil {
		str.WriteString(fmt.Sprintf(" %s", e.NotKeyword.TokenLiteral()))
	}
	str.WriteString(fmt.Sprintf(" %s (", e.InKeyword.TokenLiteral()))

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
	if e.NotKeyword != nil {
		str.WriteString(fmt.Sprintf(" %s", e.NotKeyword.TokenLiteral()))
	}
	str.WriteString(fmt.Sprintf(" %s ", e.LikeKeyword.TokenLiteral()))
	str.WriteString(e.Pattern.TokenLiteral())
	return str.String()
}
func (e ExprNotLogicalOperator) TokenLiteral() string {
	var str strings.Builder
	str.WriteString(fmt.Sprintf("%s ", e.NotKeyword.TokenLiteral()))
	str.WriteString(e.Expression.TokenLiteral())
	return str.String()
}
func (e ExprOrLogicalOperator) TokenLiteral() string {
	var str strings.Builder
	str.WriteString(e.Left.TokenLiteral())
	str.WriteString(fmt.Sprintf(" %s ", e.OrKeyword.TokenLiteral()))
	str.WriteString(e.Right.TokenLiteral())
	return str.String()
}
func (e ExprSomeLogicalOperator) TokenLiteral() string {
	var str strings.Builder
	str.WriteString(e.ScalarExpression.TokenLiteral())
	str.WriteString(fmt.Sprintf(" %s ", e.ComparisonOperator.TokenLiteral()))
	str.WriteString(fmt.Sprintf(" %s ", e.SomeKeyword.TokenLiteral()))
	str.WriteString(e.Subquery.TokenLiteral())
	return str.String()
}
func (e ExprAnyLogicalOperator) TokenLiteral() string {
	var str strings.Builder
	str.WriteString(e.ScalarExpression.TokenLiteral())
	str.WriteString(fmt.Sprintf(" %s ", e.ComparisonOperator.TokenLiteral()))
	str.WriteString(fmt.Sprintf(" %s ", e.AnyKeyword.TokenLiteral()))
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
