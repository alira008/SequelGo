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
	HavingClause  Expression
	GroupByClause []Expression
	OrderByClause *OrderByClause
}

type TopArg struct {
	WithTies bool
	Percent  bool
	Quantity Expression
}

type OrderByClause struct {
	Expressions []OrderByArg
	OffsetFetch *OffsetFetchClause
}

type OffsetFetchClause struct {
	Offset OffsetArg
	Fetch  *FetchArg
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
	RRRow RowOrRows = iota
	RRRows
)

type NextOrFirst uint8

const (
	NFNext NextOrFirst = iota
	NFFirst
)

type OffsetArg struct {
	Value     Expression
	RowOrRows RowOrRows
}

type FetchArg struct {
	Value       Expression
	NextOrFirst NextOrFirst
	RowOrRows   RowOrRows
}

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

	var groupByArgs []string
	for _, g := range sb.GroupByClause {
		groupByArgs = append(groupByArgs, g.TokenLiteral())
	}
	if len(groupByArgs) > 1 {
		str.WriteString(strings.Join(groupByArgs, ", "))
	}

	if sb.HavingClause != nil {
		str.WriteString(" HAVING ")
		str.WriteString(sb.HavingClause.TokenLiteral())
	}

	if sb.OrderByClause != nil {
		str.WriteString(sb.OrderByClause.TokenLiteral())
	}

	return str.String()
}

func (ta TopArg) expressionNode() {}
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

func (o OrderByClause) expressionNode() {}
func (o OrderByClause) TokenLiteral() string {
	var str strings.Builder

	if len(o.Expressions) == 0 {
		return ""
	}

	var orderByArgs []string
	for _, o := range o.Expressions {
		orderByArgs = append(orderByArgs, o.TokenLiteral())
	}

	str.WriteString(" ORDER BY ")
	str.WriteString(strings.Join(orderByArgs, ", "))

	if o.OffsetFetch == nil {
		return str.String()
	}

	str.WriteString(o.OffsetFetch.TokenLiteral())

	return str.String()
}

func (o OffsetFetchClause) expressionNode() {}
func (o OffsetFetchClause) TokenLiteral() string {
	var str strings.Builder

	str.WriteString(o.Offset.TokenLiteral())

	if o.Fetch == nil {
		return str.String()
	}

	str.WriteString(o.Fetch.TokenLiteral())
	return str.String()
}

func (o OrderByArg) expressionNode() {}
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

func (o OffsetArg) expressionNode() {}
func (o OffsetArg) TokenLiteral() string {
	var str strings.Builder
	str.WriteString(" OFFSET ")
	str.WriteString(o.Value.TokenLiteral())
	switch o.RowOrRows {
	case RRRow:
		str.WriteString(" ROW")
		break
	case RRRows:
		str.WriteString(" ROWS")
		break
	}
	return str.String()
}

func (f FetchArg) expressionNode() {}
func (f FetchArg) TokenLiteral() string {
	var str strings.Builder
	str.WriteString(" FETCH ")
	switch f.NextOrFirst {
	case NFNext:
		str.WriteString(" NEXT ")
		break
	case NFFirst:
		str.WriteString(" FIRST ")
		break
	}

	str.WriteString(f.Value.TokenLiteral())

	switch f.RowOrRows {
	case RRRow:
		str.WriteString(" ROW")
		break
	case RRRows:
		str.WriteString(" ROWS")
		break
	}

	str.WriteString(" ONLY")
	return str.String()
}
