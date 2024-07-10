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

type DeclareStatement struct{}
type ExecuteStatement struct{}
type SetLocalVariableStatement struct{}
type InsertStatement struct{}
type UpdateStatement struct{}
type DeleteStatement struct{}
type CommmonTableExpression struct{}

type SelectStatement struct {
	CTE        *CommmonTableExpression
	SelectBody *SelectBody
}

type SelectBody struct {
	Distinct      bool
	Top           *TopArg
	SelectItems   []Expression
	TableObject   Expression
	WhereClause   Expression
	OrderByClause []OrderByArg
}

type TopArg struct {
	WithTies bool
	Percent  bool
	Quantity Expression
}

type OrderByArg struct {
	Column Expression
	Type   OrderByType
}

type OrderByType uint8

const (
	OBNone OrderByType = iota
	OBAsc
	OBDesc
)

type RowOrRows uint8

const (
	Row RowOrRows = iota
	Rows
)

func (q Query) TokenLiteral() string {
	str := strings.Builder{}

	for _, s := range q.Statements {
		if s != nil {
			fmt.Printf("yessir")
		}
		str.WriteString(s.TokenLiteral())
	}

	return str.String()
}

func (ds DeclareStatement) statementNode() {}
func (ds DeclareStatement) TokenLiteral() string {
	return ""
}

func (ss SelectStatement) statementNode() {}
func (ss SelectStatement) TokenLiteral() string {
	fmt.Printf("select statement %s\n", ss.SelectBody.TokenLiteral())
	return ss.SelectBody.TokenLiteral()
}

func (sb SelectBody) statementNode() {}
func (sb SelectBody) TokenLiteral() string {
	var str strings.Builder
	str.WriteString("SELECT ")

	if sb.Distinct {
		str.WriteString("DISTINCT ")
	}

	if sb.Top != nil {
		str.WriteString(fmt.Sprintf("%s ", sb.Top.TokenLiteral()))
	}

	var selectItems []string
	for _, s := range sb.SelectItems {
		selectItems = append(selectItems, s.TokenLiteral())
	}
	str.WriteString(strings.Join(selectItems, ", "))

	if sb.TableObject != nil {
		str.WriteString(" FROM ")
		str.WriteString(sb.TableObject.TokenLiteral())
	}

	if sb.WhereClause != nil {
		str.WriteString(" WHERE ")
		str.WriteString(sb.WhereClause.TokenLiteral())
	}

	var orderByArgs []string
	for _, o := range sb.OrderByClause {
		orderByArgs = append(orderByArgs, o.TokenLiteral())
	}
    if len(orderByArgs) > 1 {
        str.WriteString(strings.Join(orderByArgs, ", "))
    }

	return str.String()
}

func (ta TopArg) TokenLiteral() string {
	var str strings.Builder
	str.WriteString(fmt.Sprintf("TOP %s", ta.Quantity.TokenLiteral()))

	if ta.Percent {
		str.WriteString(" PERCENT")
	}

	if ta.WithTies {
		str.WriteString(" WITH TIES")
	}

	return str.String()
}

func (o OrderByArg) TokenLiteral() string {
	var str strings.Builder
	str.WriteString(o.Column.TokenLiteral())
	switch o.Type {
	case OBNone:
		break
	case OBAsc:
		str.WriteString(" ASC")
		break
	case OBDesc:
		str.WriteString(" DESC")
		break
	}
	return str.String()
}
