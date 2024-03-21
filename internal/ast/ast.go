package ast

import (
	"fmt"
	"strings"
)

type Node interface {
	TokenLiteral() string
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
	Statements []Statement
}

func (q *Query) TokenLiteral() string {
	str := strings.Builder{}

	for _, s := range q.Statements {
        if s != nil {
            fmt.Printf("yessir")
        }
		str.WriteString(s.TokenLiteral())
	}

	return str.String()
}

type DeclareStatement struct {
}

func (ds *DeclareStatement) statementNode() {}
func (ds *DeclareStatement) TokenLiteral() string {
	return ""
}

type ExecuteStatement struct {
}

type SetLocalVariableStatement struct {
}

type SelectStatement struct {
	SelectItems *[]*Expr
	TableObject *Expr
	whereClause *Expr
}

func (ss *SelectStatement) statementNode() {}
func (ss *SelectStatement) TokenLiteral() string {
	var str strings.Builder
	str.WriteString("SELECT ")

	if ss.SelectItems == nil {
		return ""
	}
	for i, s := range *ss.SelectItems {
		if i > 0 {
			str.WriteString(", ")
		}

		str.WriteString(s.TokenLiteral())
	}
	return str.String()
}

type InsertleteStatement struct {
}

type UpdateStatement struct {
}

type DeleteStatement struct {
}

type CTESelectStatement struct {
}

type CTEInsertleteStatement struct {
}

type CTEUpdateStatement struct {
}

type CTEDeleteStatement struct {
}
