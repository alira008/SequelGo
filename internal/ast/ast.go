package ast

import (
	"SequelGo/internal/lexer"
	"fmt"
	"strings"
)

type Node interface {
	TokenLiteral() string
	SetBaseNode(baseNode BaseNode)
	GetBaseNode() BaseNode
}

type Position struct {
	Line, Col uint64
}

type BaseNode struct {
	StartPosition, EndPosition Position
}

func NewBaseNodeFromLexerPosition(Start, End lexer.Position) BaseNode {
	return BaseNode{
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

func NewBaseNode(Start, End Position) BaseNode {
	return BaseNode{
		StartPosition: Start,
		EndPosition:   End,
	}
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
	BaseNode
	Statements []Statement
	Comments   []Comment
}

type Comment struct {
	BaseNode
	Value string
}

func (c *Comment) TokenLiteral() string {
	return fmt.Sprintf("--%s", c.Value)
}
func (c *Comment) SetBaseNode(baseNode BaseNode) {
    c.BaseNode = baseNode
}
func (c *Comment) GetBaseNode() BaseNode {
    return c.BaseNode
}

func NewComment(token lexer.Token) Comment {
	return Comment{
		BaseNode: NewBaseNodeFromLexerPosition(token.Start, token.End),
		Value:    token.Value,
	}
}

type DeclareStatement struct{}
type ExecuteStatement struct{}
type SetLocalVariableStatement struct{}
type InsertStatement struct{}
type UpdateStatement struct{}
type DeleteStatement struct{}
type CommmonTableExpression struct {
	BaseNode
	Name    string
	Columns *ExprExpressionList
	Query   SelectBody
}

type SelectStatement struct {
	BaseNode
	CTE        *[]CommmonTableExpression
	SelectBody *SelectBody
}

type SelectBody struct {
	BaseNode
	Distinct      bool
	Top           *TopArg
	SelectItems   []Expression
	Table         *TableArg
	WhereClause   Expression
	HavingClause  Expression
	GroupByClause []Expression
	OrderByClause *OrderByClause
}

type TopArg struct {
	BaseNode
	WithTies bool
	Percent  bool
	Quantity Expression
}

type TableArg struct {
	BaseNode
	Table *TableSource
	Joins []Join
}

type TableSource struct {
	BaseNode
	Type   TableSourceType
	Source Expression
}

type TableSourceType uint8

const (
	TSTTable TableSourceType = iota
	TSTDerived
	TSTTableValuedFunction
)

type Join struct {
	BaseNode
	Type      JoinType
	Table     *TableSource
	Condition Expression
}

type JoinType uint8

const (
	JTInner JoinType = iota
	JTLeft
	JTLeftOuter
	JTRight
	JTRightOuter
	JTFull
	JTFullOuter
)

type OrderByClause struct {
	BaseNode
	Expressions []OrderByArg
	OffsetFetch *OffsetFetchClause
}

type OffsetFetchClause struct {
	BaseNode
	Offset OffsetArg
	Fetch  *FetchArg
}

type OrderByArg struct {
	BaseNode
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
	BaseNode
	Value     Expression
	RowOrRows RowOrRows
}

type FetchArg struct {
	BaseNode
	Value       Expression
	NextOrFirst NextOrFirst
	RowOrRows   RowOrRows
}

func (q Query) TokenLiteral() string {
	str := strings.Builder{}

	for _, s := range q.Statements {
		str.WriteString(s.TokenLiteral())
	}

	return str.String()
}

func (ds DeclareStatement) statementNode() {}
func (ds DeclareStatement) TokenLiteral() string {
	return ""
}

func (cte CommmonTableExpression) expressionNode() {}
func (cte CommmonTableExpression) TokenLiteral() string {
	var str strings.Builder
	str.WriteString(fmt.Sprintf("WITH AS %s", cte.Name))
	if cte.Columns != nil {
		str.WriteString(cte.Columns.TokenLiteral())
	}
	str.WriteString(" ( ")
	str.WriteString(cte.Query.TokenLiteral())
	str.WriteString(" )")
	return str.String()
}
func (cte *CommmonTableExpression) SetBaseNode(baseNode BaseNode) {
    cte.BaseNode = baseNode
}
func (cte *CommmonTableExpression) GetBaseNode() BaseNode {
    return cte.BaseNode
}

func (ss SelectStatement) statementNode() {}
func (ss SelectStatement) TokenLiteral() string {
	var str strings.Builder
	if ss.CTE != nil {
		ctes := []string{}

		for _, cte := range *ss.CTE {
			ctes = append(ctes, cte.TokenLiteral())
		}

		str.WriteString(strings.Join(ctes, ", "))
	}
	return ss.SelectBody.TokenLiteral()
}
func (ss *SelectStatement) SetBaseNode(baseNode BaseNode) {
    ss.BaseNode = baseNode
}
func (ss *SelectStatement) GetBaseNode() BaseNode {
    return ss.BaseNode
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

	if sb.Table != nil {
		str.WriteString(" FROM ")
		str.WriteString(sb.Table.TokenLiteral())
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
func (sb *SelectBody) SetBaseNode(baseNode BaseNode) {
    sb.BaseNode = baseNode
}
func (sb *SelectBody) GetBaseNode() BaseNode {
    return sb.BaseNode
}

func (ta TableArg) expressionNode() {}
func (ta TableArg) TokenLiteral() string {
	var str strings.Builder

	str.WriteString(ta.Table.TokenLiteral())

	if len(ta.Joins) == 0 {
		return str.String()
	}

	var joins []string
	for _, j := range ta.Joins {
		joins = append(joins, j.TokenLiteral())
	}

	str.WriteString(strings.Join(joins, " "))

	return str.String()
}
func (ta *TableArg) SetBaseNode(baseNode BaseNode) {
    ta.BaseNode = baseNode
}
func (ta *TableArg) GetBaseNode() BaseNode {
    return ta.BaseNode
}

func (ts TableSource) expressionNode() {}
func (ts TableSource) TokenLiteral() string {
	var str strings.Builder

	str.WriteString(ts.Source.TokenLiteral())

	return str.String()
}
func (ts *TableSource) SetBaseNode(baseNode BaseNode) {
    ts.BaseNode = baseNode
}
func (ts *TableSource) GetBaseNode() BaseNode {
    return ts.BaseNode
}

func (j Join) expressionNode() {}
func (j Join) TokenLiteral() string {
	var str strings.Builder

	switch j.Type {
	case JTInner:
		str.WriteString(" INNER JOIN ")
		break
	case JTLeft:
		str.WriteString(" LEFT JOIN ")
		break
	case JTLeftOuter:
		str.WriteString(" LEFT OUTER JOIN ")
		break
	case JTRight:
		str.WriteString(" RIGHT JOIN ")
		break
	case JTRightOuter:
		str.WriteString(" RIGHT OUTER JOIN ")
		break
	case JTFull:
		str.WriteString(" FULL JOIN ")
		break
	case JTFullOuter:
		str.WriteString(" RIGHT OUTER JOIN ")
		break
	}

	str.WriteString(j.Table.TokenLiteral())

	if j.Condition != nil {
		str.WriteString(" ON ")
		str.WriteString(j.Condition.TokenLiteral())
	}

	return str.String()
}
func (j *Join) SetBaseNode(baseNode BaseNode) {
    j.BaseNode = baseNode
}
func (j *Join) GetBaseNode() BaseNode {
    return j.BaseNode
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
func (ta *TopArg) SetBaseNode(baseNode BaseNode) {
    ta.BaseNode = baseNode
}
func (ta *TopArg) GetBaseNode() BaseNode {
    return ta.BaseNode
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
func (o *OrderByClause) SetBaseNode(baseNode BaseNode) {
    o.BaseNode = baseNode
}
func (o *OrderByClause) GetBaseNode() BaseNode {
    return o.BaseNode
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
func (o *OffsetFetchClause) SetBaseNode(baseNode BaseNode) {
    o.BaseNode = baseNode
}
func (o *OffsetFetchClause) GetBaseNode() BaseNode {
    return o.BaseNode
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
func (o *OrderByArg) SetBaseNode(baseNode BaseNode) {
    o.BaseNode = baseNode
}
func (o *OrderByArg) GetBaseNode() BaseNode {
    return o.BaseNode
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
func (o *OffsetArg) SetBaseNode(baseNode BaseNode) {
    o.BaseNode = baseNode
}
func (o *OffsetArg) GetBaseNode() BaseNode {
    return o.BaseNode
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
func (f *FetchArg) SetBaseNode(baseNode BaseNode) {
    f.BaseNode = baseNode
}
func (f *FetchArg) GetBaseNode() BaseNode {
    return f.BaseNode
}
