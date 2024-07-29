package ast

import (
	"fmt"
	"strings"
)

type DeclareStatement struct{}
type ExecuteStatement struct{}
type SetLocalVariableStatement struct{}
type InsertStatement struct{}
type UpdateStatement struct{}
type DeleteStatement struct{}

type SelectStatement struct {
	Span
	WithKeyword *Keyword
	CTE         *[]CommonTableExpression
	SelectBody  *SelectBody
}

type SelectBody struct {
	Span
	SelectKeyword Keyword
	Distinct      *Keyword
	AllKeyword    *Keyword
	Top           *TopArg
	SelectItems   SelectItems
	Table         *TableArg
	WhereClause   *WhereClause
	HavingClause  *HavingClause
	GroupByClause *GroupByClause
	OrderByClause *OrderByClause
}

func (ds DeclareStatement) statementNode() {}
func (ss SelectStatement) statementNode()  {}
func (sb SelectBody) statementNode()       {}

func (ds DeclareStatement) TokenLiteral() string {
	return ""
}
func (ss SelectStatement) TokenLiteral() string {
	var str strings.Builder
	if ss.CTE != nil {
		str.WriteString(fmt.Sprintf("%s ", ss.WithKeyword.TokenLiteral()))
		ctes := []string{}

		for _, cte := range *ss.CTE {
			ctes = append(ctes, cte.TokenLiteral())
		}

		str.WriteString(strings.Join(ctes, ", "))
	}
	str.WriteString(" ")
	str.WriteString(ss.SelectBody.TokenLiteral())
	return str.String()
}
func (sb SelectBody) TokenLiteral() string {
	var str strings.Builder
	str.WriteString(fmt.Sprintf("%s ", sb.SelectKeyword.TokenLiteral()))

	if sb.Distinct != nil {
		str.WriteString(fmt.Sprintf("%s ", sb.Distinct.TokenLiteral()))
	}

	if sb.AllKeyword != nil {
		str.WriteString(fmt.Sprintf("%s ", sb.AllKeyword.TokenLiteral()))
	}

	if sb.Top != nil {
		str.WriteString(fmt.Sprintf("%s ", sb.Top.TokenLiteral()))
	}

	str.WriteString(expressionListToString(sb.SelectItems.Items, ", "))

	if sb.Table != nil {
		str.WriteString(sb.Table.TokenLiteral())
	}

	if sb.WhereClause != nil {
		str.WriteString(sb.WhereClause.TokenLiteral())
	}

	if sb.GroupByClause != nil {
		str.WriteString(sb.GroupByClause.TokenLiteral())
	}
	if sb.HavingClause != nil {
		str.WriteString(sb.HavingClause.TokenLiteral())
	}

	if sb.OrderByClause != nil {
		str.WriteString(sb.OrderByClause.TokenLiteral())
	}

	return str.String()
}

func (ss *SelectStatement) GetSpan() Span { return ss.Span }
func (sb *SelectBody) SetSpan(span Span)  { sb.Span = span }

func (ss *SelectStatement) SetSpan(span Span) { ss.Span = span }
func (sb *SelectBody) GetSpan() Span          { return sb.Span }
