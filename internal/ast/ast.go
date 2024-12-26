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

type Position struct {
	Line, Col uint64
}

type Span struct {
	StartPosition, EndPosition Position
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
	LeadingComments  *[]Comment
	TrailingComments *[]Comment
	Statements       []Statement
}

type Comment struct {
	Span
	LeadingComments  *[]Comment
	TrailingComments *[]Comment
	Value            string
}

func NewSpanFromLexerPosition(Start, End lexer.Position) Span {
	return Span{
		StartPosition: Position{
			Line: uint64(Start.Line),
			Col:  uint64(Start.Col),
		},
		EndPosition: Position{
			Line: uint64(End.Line),
			Col:  uint64(End.Col),
		},
	}
}
func NewSpan(Start, End Position) Span {
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

func (q *Query) SetLeadingComments(comments []Comment)   { q.LeadingComments = &comments }
func (c *Comment) SetLeadingComments(comments []Comment) { c.LeadingComments = &comments }

func (c *Comment) SetTrailingComments(comments []Comment) { c.TrailingComments = &comments }
func (q *Query) SetTrailingComments(comments []Comment)   { q.TrailingComments = &comments }

func (q *Query) GetLeadingComments() *[]Comment   { return q.LeadingComments }
func (c *Comment) GetLeadingComments() *[]Comment { return c.LeadingComments }

func (c *Comment) GetTrailingComments() *[]Comment { return c.TrailingComments }
func (q *Query) GetTrailingComments() *[]Comment   { return q.TrailingComments }

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
