package ast

import "fmt"

type Visitor interface {
	Visit(node Node) (v Visitor)
}

func walkList[N Node](v Visitor, list []N) {
	for _, node := range list {
		Walk(v, node)
	}
}

func Walk(v Visitor, node Node) {
	if v := v.Visit(node); v == nil {
		return
	}

	switch n := node.(type) {
	case *Comment:
		break
	case *Query:
		walkList(v, n.Statements)
		break
	case *SelectStatement:
		if n.WithKeyword != nil {
			Walk(v, n.WithKeyword)
		}
		if n.CTE != nil {
			for _, cte := range *n.CTE {
				Walk(v, &cte)
			}
		}
		Walk(v, n.SelectBody)
		break
	case *SelectBody:
		Walk(v, &n.SelectKeyword)
		if n.DistinctKeyword != nil {
			Walk(v, n.DistinctKeyword)
		}
		if n.AllKeyword != nil {
			Walk(v, n.AllKeyword)
		}
		if n.Top != nil {
			Walk(v, n.Top)
		}
		Walk(v, &n.SelectItems)
		Walk(v, n.Table)
		if n.WhereClause != nil {
			Walk(v, n.WhereClause)
		}
		if n.HavingClause != nil {
			Walk(v, n.HavingClause)
		}
		if n.GroupByClause != nil {
			Walk(v, n.GroupByClause)
		}
		if n.OrderByClause != nil {
			Walk(v, n.OrderByClause)
		}
		break
	case *ExprStringLiteral:
		break
	case *ExprNumberLiteral:
		break
	case *ExprLocalVariable:
		break
	case *ExprIdentifier:
		break
	case *ExprQuotedIdentifier:
		break
	case *ExprStar:
		break
	case *ExprWithAlias:
		Walk(v, n.Expression)
		if n.AsKeyword != nil {
			Walk(v, n.AsKeyword)
		}
		Walk(v, n.Alias)
		break
	case *ExprCompoundIdentifier:
		walkList(v, n.Identifiers)
		break
	case *SelectItems:
		walkList(v, n.Items)
		break
	case *WhereClause:
		Walk(v, &n.WhereKeyword)
		Walk(v, n.Clause)
		break
	case *HavingClause:
		Walk(v, &n.HavingKeyword)
		Walk(v, n.Clause)
		break
	case *GroupByClause:
		for _, k := range n.GroupByKeyword {
			Walk(v, &k)
		}
		walkList(v, n.Items)
		break
	case *TableArg:
		Walk(v, &n.FromKeyword)
		Walk(v, n.Table)
		for _, j := range n.Joins {
			Walk(v, &j)
		}
		break
	case *TableSource:
		Walk(v, n.Source)
		break
	case *Join:
		for _, k := range n.JoinTypeKeyword {
			Walk(v, &k)
		}
		Walk(v, &n.JoinKeyword)
		Walk(v, n.Table)
		if n.OnKeyword != nil {
			Walk(v, n.OnKeyword)
		}
		Walk(v, n.Condition)
		break
	case *TopArg:
		Walk(v, &n.TopKeyword)
		Walk(v, n.Quantity)
		if n.PercentKeyword != nil {
			Walk(v, n.PercentKeyword)
		}
		if n.WithTiesKeyword != nil {
			for _, k := range n.WithTiesKeyword {
				Walk(v, &k)
			}
		}
		break
	case *OrderByArg:
		Walk(v, n.Column)
		if n.OrderKeyword != nil {
			Walk(v, n.OrderKeyword)
		}
		break
	case *OrderByClause:
		for _, k := range n.OrderByKeyword {
			Walk(v, &k)
		}
		for _, e := range n.Expressions {
			Walk(v, &e)
		}

		if n.OffsetFetch != nil {
			Walk(v, n.OffsetFetch)
		}
		break
	case *OffsetArg:
		Walk(v, &n.OffsetKeyword)
		Walk(v, &n.RowOrRowsKeyword)
		Walk(v, n.Value)
		break
	case *FetchArg:
		Walk(v, &n.FetchKeyword)
		Walk(v, n.Value)
		Walk(v, &n.NextOrFirstKeyword)
		Walk(v, &n.RowOrRowsKeyword)
		Walk(v, &n.OnlyKeyword)
		break
	case *OffsetFetchClause:
		Walk(v, &n.Offset)
		if n.Fetch != nil {
			Walk(v, n.Fetch)
		}
		break
	case *ExprSubquery:
		Walk(v, &n.SelectBody)
		break
	case *ExprExpressionList:
		walkList(v, n.List)
		break
	case *ExprFunction:
		Walk(v, n.Name)
		break
	case *WindowFrameBound:
		for _, k := range n.BoundKeyword {
			Walk(v, &k)
		}
		Walk(v, n.Expression)
		break
	case *WindowFrameClause:
		Walk(v, &n.RowsOrRangeKeyword)
		if n.BetweenKeyword != nil {
			Walk(v, n.BetweenKeyword)
		}
		Walk(v, n.Start)
		if n.AndKeyword != nil {
			Walk(v, n.AndKeyword)
		}
		if n.End != nil {
			Walk(v, n.End)
		}
		break
	case *FunctionOverClause:
		Walk(v, &n.OverKeyword)
		if n.PartitionByKeyword != nil {
			for _, k := range n.PartitionByKeyword {
				Walk(v, &k)
			}
		}
		walkList(v, n.PartitionByClause)
		if n.OrderByKeyword != nil {
			for _, k := range n.OrderByKeyword {
				Walk(v, &k)
			}
		}
		for _, o := range n.OrderByClause {
			Walk(v, &o)
		}
		if n.WindowFrameClause != nil {
			Walk(v, n.WindowFrameClause)
		}
		break
	case *ExprFunctionCall:
		Walk(v, n.Name)
		walkList(v, n.Args)
		if n.OverClause != nil {
			Walk(v, n.OverClause)
		}
		break
	case *ExprCast:
		Walk(v, &n.CastKeyword)
		Walk(v, n.Expression)
		Walk(v, &n.AsKeyword)
		break
	case *CommonTableExpression:
		Walk(v, n.Columns)
		Walk(v, &n.AsKeyword)
		Walk(v, &n.Query)
		break
	case *DataType:
		Walk(v, n.DecimalNumericSize)
		break
	case *NumericSize:
		break
	case *ExprUnaryOperator:
		Walk(v, n.Right)
		break
	case *ExprComparisonOperator:
		Walk(v, n.Left)
		Walk(v, n.Right)
		break
	case *ExprArithmeticOperator:
		Walk(v, n.Left)
		Walk(v, n.Right)
		break
	case *ExprAndLogicalOperator:
		Walk(v, n.Left)
		Walk(v, &n.AndKeyword)
		Walk(v, n.Right)
		break
	case *ExprAllLogicalOperator:
		Walk(v, n.ScalarExpression)
		Walk(v, &n.AllKeyword)
		Walk(v, n.Subquery)
		break
	case *ExprBetweenLogicalOperator:
		Walk(v, n.TestExpression)
		Walk(v, &n.BetweenKeyword)
		Walk(v, n.Begin)
		Walk(v, &n.AndKeyword)
		Walk(v, n.End)
		break
	case *ExprExistsLogicalOperator:
		Walk(v, &n.ExistsKeyword)
		Walk(v, n.Subquery)
		break
	case *ExprInSubqueryLogicalOperator:
		Walk(v, n.TestExpression)
		if n.NotKeyword != nil {
			Walk(v, n.NotKeyword)
		}
		Walk(v, &n.InKeyword)
		Walk(v, n.Subquery)
		break
	case *ExprInLogicalOperator:
		Walk(v, n.TestExpression)
		if n.NotKeyword != nil {
			Walk(v, n.NotKeyword)
		}
		Walk(v, &n.InKeyword)
		walkList(v, n.Expressions)
		break
	case *ExprLikeLogicalOperator:
		if n.NotKeyword != nil {
			Walk(v, n.NotKeyword)
		}
		Walk(v, &n.LikeKeyword)
		Walk(v, n.MatchExpression)
		Walk(v, n.Pattern)
		break
	case *ExprNotLogicalOperator:
		Walk(v, &n.NotKeyword)
		Walk(v, n.Expression)
		break
	case *ExprOrLogicalOperator:
		Walk(v, n.Left)
		Walk(v, &n.OrKeyword)
		Walk(v, n.Right)
		break
	case *ExprSomeLogicalOperator:
		Walk(v, n.ScalarExpression)
		Walk(v, &n.SomeKeyword)
		Walk(v, n.Subquery)
		break
	case *ExprAnyLogicalOperator:
		Walk(v, n.ScalarExpression)
		Walk(v, &n.AnyKeyword)
		Walk(v, n.Subquery)
		break
	case *Keyword:
		break
	default:
		panic(fmt.Sprintf("ast.Walk: unexpected node type %T", n))
	}

	v.Visit(nil)
}

type inspector func(Node) bool

func (f inspector) Visit(node Node) Visitor {
	if f(node) {
		return f
	}

	return nil
}

// traverse tree depth first
func Inspect(node Node, f func(Node) bool) {
	Walk(inspector(f), node)
}
