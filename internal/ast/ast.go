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
	CTE        *CommmonTableExpression
	SelectBody *SelectBody
}

func (ss *SelectStatement) statementNode() {}
func (ss *SelectStatement) TokenLiteral() string {
	fmt.Printf("select statement %s\n", ss.SelectBody.TokenLiteral())
	return ss.SelectBody.TokenLiteral()
}

type CommmonTableExpression struct{}

type TopArg struct {
	WithTies bool
	Percent  bool
	Quantity Expression
}
func (ta *TopArg) TokenLiteral() string {
	var str strings.Builder
	str.WriteString(fmt.Sprintf("TOP %s ", ta.Quantity.TokenLiteral()))

    if ta.Percent {
        str.WriteString("PERCENT ")
    }

    if ta.WithTies {
        str.WriteString("WITH TIES ")
    }

    return str.String()
}

type SelectBody struct {
	Distinct    bool
	Top         *TopArg
	SelectItems []Expression
	TableObject Expression
	WhereClause Expression
}

func (sb *SelectBody) TokenLiteral() string {
	var str strings.Builder
	str.WriteString("SELECT ")

    if sb.Distinct {
        str.WriteString("DISTINCT ")
    }

    if sb.Top != nil {
        str.WriteString(sb.Top.TokenLiteral())
    }

	if sb.SelectItems == nil {
		return ""
	}
	for i, s := range sb.SelectItems {
		if i > 0 {
			str.WriteString(", ")
		}

		str.WriteString(s.TokenLiteral())
	}
	if sb.TableObject != nil {
		str.WriteString(" FROM ")
		str.WriteString(sb.TableObject.TokenLiteral())
	}

	if sb.WhereClause != nil {
		str.WriteString(" WHERE ")
		str.WriteString(sb.WhereClause.TokenLiteral())
	}

	return str.String()
}

type InsertStatement struct {
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
