package ast

import (
	"SequelGo/internal/lexer"
	"fmt"
	"strings"
)

type Node interface {
	TokenLiteral() string
	SetSpan(span Span)
	GetSpan() Span
}

type Span struct {
	StartPosition, EndPosition lexer.Position
}

type Statement interface {
	Node
	statementNode()
}

type Expression interface {
	Node
	expressionNode()
}

type Query struct {
	Span
	Statements       []Statement
}

type Comment struct {
	Span
	Value            string
}

func NewSpanFromToken(token lexer.Token) Span {
	return Span{
		StartPosition: token.Start,
		EndPosition: token.End,
	}
}
func NewSpanFromLexerPosition(Start, End lexer.Position) Span {
	return Span{
		StartPosition: Start,
		EndPosition: End,
	}
}
func NewSpan(Start, End lexer.Position) Span {
	return Span{
		StartPosition: Start,
		EndPosition:   End,
	}
}
func NewComment(token lexer.Token) Comment {
	return Comment{
		Span:  NewSpanFromLexerPosition(token.Start, token.End),
		Value: token.Value,
	}
}

func (q *Query) SetSpan(span Span)   { q.Span = span }
func (c *Comment) SetSpan(span Span) { c.Span = span }

func (q Query) GetSpan() Span   { return q.Span }
func (c Comment) GetSpan() Span { return c.Span }

func (q *Query) TokenLiteral() string {
	str := strings.Builder{}

	for _, s := range q.Statements {
		str.WriteString(s.TokenLiteral())
	}

	return str.String()
}
func (c *Comment) TokenLiteral() string {
	return fmt.Sprintf("-- %s", c.Value)
}
