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
	case *SelectStatement:
		if n.CTE != nil {
			for _, cte := range *n.CTE {
				Walk(v, &cte)
			}
		}
		Walk(v, n.SelectBody)
		break
	case *SelectBody:
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
		Walk(v, n.Alias)
		break
	case *ExprCompoundIdentifier:
		walkList(v, n.Identifiers)
		break
	case *SelectItems:
		walkList(v, n.Items)
		break
	case *WhereClause:
		Walk(v, n.Clause)
		break
	case *HavingClause:
		Walk(v, n.Clause)
		break
	case *GroupByClause:
		walkList(v, n.Items)
		break
	case *TableArg:
		Walk(v, n.Table)
		for _, j := range n.Joins {
			Walk(v, &j)
		}
		break
	case *TableSource:
		Walk(v, n.Source)
		break
	case *Join:
		Walk(v, n.Table)
		Walk(v, n.Condition)
		break
	case *TopArg:
		Walk(v, n.Quantity)
		break
	case *OrderByArg:
		Walk(v, n.Column)
		break
	case *OrderByClause:
		for _, e := range n.Expressions {
			Walk(v, &e)
		}

		if n.OffsetFetch != nil {
			Walk(v, n.OffsetFetch)
		}
		break
	case *OffsetArg:
		Walk(v, n.Value)
		break
	case *FetchArg:
		Walk(v, n.Value)
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
		Walk(v, n.Expression)
		break
	case *WindowFrameClause:
		Walk(v, n.Start)
		if n.End != nil {
			Walk(v, n.End)
		}
		break
	case *FunctionOverClause:
		walkList(v, n.PartitionByClause)
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
		Walk(v, n.Expression)
		break
	case *CommonTableExpression:
		Walk(v, n.Columns)
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
		Walk(v, n.Right)
		break
	case *ExprAllLogicalOperator:
		Walk(v, n.ScalarExpression)
		Walk(v, n.Subquery)
		break
	case *ExprBetweenLogicalOperator:
		Walk(v, n.TestExpression)
		Walk(v, n.Begin)
		Walk(v, n.End)
		break
	case *ExprExistsLogicalOperator:
		Walk(v, n.Subquery)
		break
	case *ExprInSubqueryLogicalOperator:
		Walk(v, n.TestExpression)
		Walk(v, n.Subquery)
		break
	case *ExprInLogicalOperator:
		Walk(v, n.TestExpression)
		walkList(v, n.Expressions)
		break
	case *ExprLikeLogicalOperator:
		Walk(v, n.MatchExpression)
		Walk(v, n.Pattern)
		break
	case *ExprNotLogicalOperator:
		Walk(v, n.Expression)
		break
	case *ExprOrLogicalOperator:
		Walk(v, n.Left)
		Walk(v, n.Right)
		break
	case *ExprSomeLogicalOperator:
		Walk(v, n.ScalarExpression)
		Walk(v, n.Subquery)
		break
	case *ExprAnyLogicalOperator:
		Walk(v, n.ScalarExpression)
		Walk(v, n.Subquery)
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
