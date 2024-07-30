package formatter

import (
	"SequelGo/internal/ast"
	"SequelGo/internal/lexer"
	"SequelGo/internal/parser"
	"fmt"
	"strings"

	"go.uber.org/zap"
)

type Formatter struct {
	logger         *zap.SugaredLogger
	settings       Settings
	indentLevel    uint32
	formattedQuery string
	currentLine    uint64
	comments       []ast.Comment
	NodeToComments map[ast.Node][]ast.Comment
}

func NewFormatter(settings Settings, logger *zap.SugaredLogger) Formatter {
	return Formatter{
		settings:       settings,
		logger:         logger,
		currentLine:    1,
		NodeToComments: make(map[ast.Node][]ast.Comment),
	}
}

func (f *Formatter) Format(input string) (string, error) {
	l := lexer.NewLexer(input)
	p := parser.NewParser(f.logger, l)
	query := p.Parse()
	if len(p.Errors()) > 0 {
		return "", fmt.Errorf(strings.Join(p.Errors(), "\n"))
	}
	f.comments = query.Comments

	ast.Inspect(&query, f.visitNode)
	// for i, s := range query.Statements {
	// 	if i > 0 {
	// 		f.printNewLine()
	// 		f.printNewLine()
	// 	}
	// 	f.associateCommentsWithNodes(s)
	// 	f.walkQuery(s)
	// }

	return f.formattedQuery, nil
}

func (f *Formatter) visitNode(node ast.Node) bool {
	switch n := node.(type) {

	case *ast.Query:
		//walkList(v, n.Statements)
		break
	case *ast.SelectStatement:
		break
	case *ast.SelectBody:
		if n.Distinct != nil {
			// f.printSpace()
		}
		if n.AllKeyword != nil {
			// f.printSpace()
		}
		break
	case *ast.ExprStringLiteral:
		f.formattedQuery += fmt.Sprintf("'%s'", n.Value)
		break
	case *ast.ExprNumberLiteral:
		f.formattedQuery += n.Value
		break
	case *ast.ExprLocalVariable:
		f.formattedQuery += fmt.Sprintf("@%s", n.Value)
		break
	case *ast.ExprIdentifier:
		f.formattedQuery += n.Value
		break
	case *ast.ExprQuotedIdentifier:
		f.formattedQuery += fmt.Sprintf("[%s]", n.Value)
		break
	case *ast.ExprStar:
		f.formattedQuery += "*"
		break
	case *ast.ExprWithAlias:
		break
	case *ast.ExprCompoundIdentifier:
		idents := make([]string, 0, len(n.Identifiers))
		for _, e := range n.Identifiers {
			idents = append(idents, e.TokenLiteral())
		}
		f.formattedQuery += strings.Join(idents, ".")
		//walkList(v, n.Identifiers)
		break
	case *ast.SelectItems:
		f.printNewLine()
		items := make([]string, 0, len(n.Items))
		for _, e := range n.Items {
			items = append(items, e.TokenLiteral())
		}
		f.formattedQuery += strings.Join(items, ",\n")
		//walkList(v, n.Items)
		break
	case *ast.WhereClause:
		//Walk(v, n.Clause)
		break
	case *ast.HavingClause:
		//Walk(v, n.Clause)
		break
	case *ast.GroupByClause:
		//walkList(v, n.Items)
		break
	case *ast.TableArg:
		//Walk(v, n.Table)
		// for _, j := range n.Joins {
		//Walk(v, &j)
		// }
		break
	case *ast.TableSource:
		//Walk(v, n.Source)
		break
	case *ast.Join:
		//Walk(v, n.Table)
		//Walk(v, n.Condition)
		break
	case *ast.TopArg:
		//Walk(v, n.Quantity)
		break
	case *ast.OrderByArg:
		//Walk(v, n.Column)
		break
	case *ast.OrderByClause:
		// for _, e := range n.Expressions {
		//Walk(v, &e)
		// }

		if n.OffsetFetch != nil {
			//Walk(v, n.OffsetFetch)
		}
		break
	case *ast.OffsetArg:
		//Walk(v, n.Value)
		break
	case *ast.FetchArg:
		//Walk(v, n.Value)
		break
	case *ast.OffsetFetchClause:
		//Walk(v, &n.Offset)
		if n.Fetch != nil {
			//Walk(v, n.Fetch)
		}
		break
	case *ast.ExprSubquery:
		//Walk(v, &n.SelectBody)
		break
	case *ast.ExprExpressionList:
		//walkList(v, n.List)
		break
	case *ast.ExprFunction:
		//Walk(v, n.Name)
		break
	case *ast.WindowFrameBound:
		//Walk(v, n.Expression)
		break
	case *ast.WindowFrameClause:
		//Walk(v, n.Start)
		if n.End != nil {
			//Walk(v, n.End)
		}
		break
	case *ast.FunctionOverClause:
		//walkList(v, n.PartitionByClause)
		// for _, o := range n.OrderByClause {
		//Walk(v, &o)
		// }
		if n.WindowFrameClause != nil {
			//Walk(v, n.WindowFrameClause)
		}
		break
	case *ast.ExprFunctionCall:
		//Walk(v, n.Name)
		//walkList(v, n.Args)
		if n.OverClause != nil {
			//Walk(v, n.OverClause)
		}
		break
	case *ast.ExprCast:
		//Walk(v, n.Expression)
		break
	case *ast.CommonTableExpression:
		//Walk(v, n.Columns)
		//Walk(v, &n.Query)
		break
	case *ast.DataType:
		//Walk(v, n.DecimalNumericSize)
		break
	case *ast.NumericSize:
		break
	case *ast.ExprUnaryOperator:
		//Walk(v, n.Right)
		break
	case *ast.ExprComparisonOperator:
		//Walk(v, n.Left)
		//Walk(v, n.Right)
		break
	case *ast.ExprArithmeticOperator:
		//Walk(v, n.Left)
		//Walk(v, n.Right)
		break
	case *ast.ExprAndLogicalOperator:
		//Walk(v, n.Left)
		//Walk(v, n.Right)
		break
	case *ast.ExprAllLogicalOperator:
		//Walk(v, n.ScalarExpression)
		//Walk(v, n.Subquery)
		break
	case *ast.ExprBetweenLogicalOperator:
		//Walk(v, n.TestExpression)
		//Walk(v, n.Begin)
		//Walk(v, n.End)
		break
	case *ast.ExprExistsLogicalOperator:
		//Walk(v, n.Subquery)
		break
	case *ast.ExprInSubqueryLogicalOperator:
		//Walk(v, n.TestExpression)
		//Walk(v, n.Subquery)
		break
	case *ast.ExprInLogicalOperator:
		//Walk(v, n.TestExpression)
		//walkList(v, n.Expressions)
		break
	case *ast.ExprLikeLogicalOperator:
		//Walk(v, n.MatchExpression)
		//Walk(v, n.Pattern)
		break
	case *ast.ExprNotLogicalOperator:
		//Walk(v, n.Expression)
		break
	case *ast.ExprOrLogicalOperator:
		//Walk(v, n.Left)
		//Walk(v, n.Right)
		break
	case *ast.ExprSomeLogicalOperator:
		//Walk(v, n.ScalarExpression)
		//Walk(v, n.Subquery)
		break
	case *ast.ExprAnyLogicalOperator:
		//Walk(v, n.ScalarExpression)
		//Walk(v, n.Subquery)
		break
	case *ast.Keyword:
		f.printKeyword(n.TokenLiteral())
		break
	case nil:
		return false
	default:
		panic(fmt.Sprintf("ast.Walk: unexpected node type %T", n))
	}

	f.printSpace()
	return true
}

func nodeList(n ast.Node) []ast.Node {
	var list []ast.Node
	ast.Inspect(n, func(n ast.Node) bool {
		switch n.(type) {
		case nil:
			return false
		}
		list = append(list, n)
		return true
	})

	return list
}

// AssociateCommentsWithNodes associates comments with the nearest nodes in the AST.
func (f *Formatter) associateCommentsWithNodes(node ast.Node) {
	// Collect nodes and their positions.
	nodes := nodeList(node)

	// Associate each comment with the nearest node.
	for _, comment := range f.comments {
		closestNode := f.findClosestNode(comment.GetSpan().StartPosition, nodes)
		if closestNode != nil {
			f.NodeToComments[closestNode] = append(f.NodeToComments[closestNode], comment)
		}
	}
}
func (f *Formatter) findClosestNode(commentPos ast.Position, nodes []ast.Node) ast.Node {
	var closestNode ast.Node
	minDistance := int64(^uint(0) >> 1) // Initialize to max int value
	for _, node := range nodes {
		distance := f.positionDistance(commentPos, node.GetSpan().StartPosition)
		if distance < minDistance {
			minDistance = distance
			closestNode = node
		}
	}
	return closestNode
}

func (f *Formatter) positionDistance(pos1, pos2 ast.Position) int64 {
	// Simple distance measure considering line difference first, then column difference
	lineDiff := abs(int64(pos1.Line) - int64(pos2.Line))
	columnDiff := abs(int64(pos1.Col) - int64(pos2.Col))
	return lineDiff*1000 + columnDiff // Assuming line difference is more significant
}

func abs(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}

func (f *Formatter) increaseIndent() {
	f.indentLevel += 1
}

func (f *Formatter) decreaseIndent() {
	f.indentLevel -= 1
}

func (f *Formatter) printKeyword(keyword string) {
	if f.settings.KeywordCase == KCUpper {
		f.formattedQuery += strings.ToUpper(keyword)
	} else if f.settings.KeywordCase == KCLower {
		f.formattedQuery += strings.ToLower(keyword)
	}
}

func (f *Formatter) printIndent() {
	for i := uint32(0); i < f.indentLevel; i++ {
		for j := uint32(0); j < f.settings.IndentWidth; j++ {
			if f.settings.UseTab {
				f.formattedQuery += "\t"
			} else {
				f.formattedQuery += " "
			}
		}
	}
}

func (f *Formatter) printSpace() {
	f.formattedQuery += " "
}

func (f *Formatter) printNewLine() {
	f.formattedQuery += "\n"
	f.currentLine += 1
	f.printIndent()
}

func (f *Formatter) printSelectColumnComma() {
	f.increaseIndent()
	if f.settings.IndentCommaLists == ICLNoSpaceAfterComma {
		f.printNewLine()
		f.formattedQuery += ","
	} else if f.settings.IndentCommaLists == ICLSpaceAfterComma {
		f.printNewLine()
		f.formattedQuery += ", "
	} else if f.settings.IndentCommaLists == ICLTrailingComma {
		f.formattedQuery += ","
		f.printNewLine()
	}
	f.decreaseIndent()
}

func (f *Formatter) printExpressionListComma() {
	f.formattedQuery += ", "
}

func (f *Formatter) printInListComma() {
	if f.settings.IndentInLists {
		f.printSelectColumnComma()
	} else {
		f.formattedQuery += ", "
	}
}

func (f *Formatter) printInListNewLine() {
	if f.settings.IndentInLists {
		f.increaseIndent()
		f.printNewLine()
		f.decreaseIndent()
	}
}

func (f *Formatter) printColumnListOpenParen() {
	f.increaseIndent()
	f.formattedQuery += "("
	if f.settings.IndentCommaLists == ICLNoSpaceAfterComma {
		f.printNewLine()
	}
	f.decreaseIndent()
}

func (f *Formatter) printColumnListCloseParen() {
	f.increaseIndent()
	if f.settings.IndentCommaLists == ICLNoSpaceAfterComma {
		f.printNewLine()
	}
	f.formattedQuery += ")"
	f.decreaseIndent()
}

func (f *Formatter) walkQuery(statement ast.Statement) {
	switch stmt := statement.(type) {
	case *ast.SelectStatement:
		f.visitSelectQuery(stmt)
		break
	}
}

func (f *Formatter) printCommentsBeforeNode(node ast.Node) {
	if comments, ok := f.NodeToComments[node]; ok {
		for _, comment := range comments {
			if comment.GetSpan().StartPosition.Line < node.GetSpan().StartPosition.Line {
				f.formattedQuery += fmt.Sprintf("\n%s", comment.TokenLiteral())
			}
		}
	}
}

func (f *Formatter) printCommentsAfterNode(node ast.Node) {
	// Add comments before this node.
	if comments, ok := f.NodeToComments[node]; ok {
		for _, comment := range comments {
			if comment.GetSpan().StartPosition.Line == node.GetSpan().StartPosition.Line &&
				comment.GetSpan().StartPosition.Col > node.GetSpan().StartPosition.Col {
				f.formattedQuery += fmt.Sprintf(" %s\n", comment.TokenLiteral())
			}
			if comment.GetSpan().StartPosition.Line > node.GetSpan().StartPosition.Line {

				f.formattedQuery += fmt.Sprintf("\n%s", comment.TokenLiteral())
			}
		}
	}
}

func (f *Formatter) visitExpression(expression ast.Expression) {
	f.printCommentsBeforeNode(expression)
	switch e := expression.(type) {
	case *ast.ExprStringLiteral:
		f.visitStringLiteralExpression(e)
		break
	case *ast.ExprNumberLiteral:
		f.visitNumberLiteralExpression(e)
		break
	case *ast.ExprLocalVariable:
		f.visitLocalVariableExpression(e)
		break
	case *ast.ExprIdentifier:
		f.visitIdentifierExpression(e)
		break
	case *ast.ExprQuotedIdentifier:
		f.visitQuotedIdentifierExpression(e)
		break
	case *ast.ExprStar:
		f.visitStarExpression()
		break
	case *ast.ExprWithAlias:
		f.visitExpressionWithAlias(e)
		break
	case *ast.ExprCompoundIdentifier:
		f.visitExpressionCompoundIdentifier(e)
		break
	case *ast.ExprSubquery:
		f.visitExpressionSubquery(e)
		break
	case *ast.ExprExpressionList:
		f.visitExpressionExpresssionList(e)
		break
	// case *ast.ExprFunction:
	// 	f.visitExpressionFunction(e)
	// 	break
	case *ast.ExprFunctionCall:
		f.visitExpressionFunctionCall(e)
		break
	case *ast.ExprCast:
		f.visitExpressionCast(e)
		break
	case *ast.ExprUnaryOperator:
		f.visitUnaryOperatorExpression(e)
		break
	case *ast.ExprComparisonOperator:
		f.visitComparisonOperatorExpression(e)
		break
	case *ast.ExprArithmeticOperator:
		f.visitArithmeticOperatorExpression(e)
		break
	case *ast.ExprAndLogicalOperator:
		f.visitAndLogicalOperatorExpression(e)
		break
	case *ast.ExprAllLogicalOperator:
		f.visitAllLogicalOperatorExpression(e)
		break
	case *ast.ExprBetweenLogicalOperator:
		f.visitBetweenLogicalOperatorExpression(e)
		break
	case *ast.ExprExistsLogicalOperator:
		f.visitExistsLogicalOperatorExpression(e)
		break
	case *ast.ExprInSubqueryLogicalOperator:
		f.visitInSubqueryLogicalOperatorExpression(e)
		break
	case *ast.ExprInLogicalOperator:
		f.visitInLogicalOperatorExpression(e)
		break
	case *ast.ExprLikeLogicalOperator:
		f.visitLikeLogicalOperatorExpression(e)
		break
	case *ast.ExprNotLogicalOperator:
		f.visitNotLogicalOperatorExpression(e)
		break
	case *ast.ExprOrLogicalOperator:
		f.visitOrLogicalOperatorExpression(e)
		break
	case *ast.ExprSomeLogicalOperator:
		f.visitSomeLogicalOperatorExpression(e)
		break
	case *ast.ExprAnyLogicalOperator:
		f.visitAnyLogicalOperatorExpression(e)
		break
	case *ast.Keyword:
		f.visitKeyword(e)
		break
	}
	f.printCommentsAfterNode(expression)
}

func (f *Formatter) visitKeyword(keyword *ast.Keyword) {
	f.printKeyword(keyword.TokenLiteral())
}

func (f *Formatter) visitSelectQuery(selectStatement *ast.SelectStatement) {
	f.visitSelectBody(selectStatement.SelectBody)
}

func (f *Formatter) visitSelectBody(selectBody *ast.SelectBody) {
	f.visitKeyword(&selectBody.SelectKeyword)
	if selectBody.Distinct != nil {
		f.printKeyword(fmt.Sprintf(" %s", selectBody.Distinct.TokenLiteral()))
	}
	if selectBody.AllKeyword != nil {
		f.printKeyword(fmt.Sprintf(" %s", selectBody.AllKeyword.TokenLiteral()))
	}
	f.visitSelectTopArg(selectBody.Top)
	f.visitSelectItems(selectBody.SelectItems)
	f.visitSelectTableArg(selectBody.Table)
	f.visitSelectWhereClause(selectBody.WhereClause)
	f.visitSelectGroupByClause(selectBody.GroupByClause)
	f.visitSelectHavingClause(selectBody.HavingClause)
	f.visitSelectOrderByClause(selectBody.OrderByClause)
}

func (f *Formatter) visitSelectTopArg(selectTopArg *ast.TopArg) {
	if selectTopArg == nil {
		return
	}

	f.printKeyword(fmt.Sprintf(" %s ", selectTopArg.TopKeyword.TokenLiteral()))
	f.visitExpression(selectTopArg.Quantity)
	if selectTopArg.PercentKeyword != nil {
		f.printKeyword(fmt.Sprintf(" %s", selectTopArg.PercentKeyword.TokenLiteral()))
	}
	if selectTopArg.WithKeyword != nil {
		f.printKeyword(fmt.Sprintf(" %s", selectTopArg.WithKeyword.TokenLiteral()))
	}
	if selectTopArg.TiesKeyword != nil {
		f.printKeyword(fmt.Sprintf(" %s", selectTopArg.TiesKeyword.TokenLiteral()))
	}
}

func (f *Formatter) visitSelectItems(selectItems ast.SelectItems) {
	for i, e := range selectItems.Items {
		if i == 0 && len(selectItems.Items) > 1 {
			f.increaseIndent()
			f.printNewLine()
			f.decreaseIndent()
		} else if i == 0 && len(selectItems.Items) == 1 {
			f.printSpace()
		}
		if i > 0 {
			f.printSelectColumnComma()
		}
		f.visitExpression(e)
	}
}

func (f *Formatter) visitSelectTableArg(selectTableArg *ast.TableArg) {
	if selectTableArg == nil {
		return
	}

	f.printNewLine()
	f.printKeyword(selectTableArg.FromKeyword.TokenLiteral())
	f.printSpace()
	f.visitTableSource(selectTableArg.Table)
	if len(selectTableArg.Joins) > 0 {
		f.printNewLine()
	}
	for _, join := range selectTableArg.Joins {
		_ = join
		f.visitTableJoin(join)
	}
}

func (f *Formatter) visitTableSource(tableSource *ast.TableSource) {
	if tableSource == nil {
		return
	}

	// not needed for now
	/* switch tableSource.Type {
	case ast.TSTTable:
		break
	case ast.TSTDerived:
		break
	case ast.TSTTableValuedFunction:
		break
	} */

	f.visitExpression(tableSource.Source)
}

func (f *Formatter) visitTableJoin(tableJoin ast.Join) {
	f.printKeyword(fmt.Sprintf("%s", tableJoin.JoinTypeKeyword1.TokenLiteral()))
	if tableJoin.JoinTypeKeyword2 != nil {
		f.printKeyword(fmt.Sprintf(" %s", tableJoin.JoinTypeKeyword2.TokenLiteral()))
	}
	f.printKeyword(fmt.Sprintf(" %s ", tableJoin.JoinKeyword.TokenLiteral()))

	f.visitTableSource(tableJoin.Table)
	if tableJoin.Condition != nil {
		if tableJoin.Condition != nil {
			f.printKeyword(fmt.Sprintf(" %s ", tableJoin.OnKeyword.TokenLiteral()))
		}
		f.visitExpression(tableJoin.Condition)
	}
}

func (f *Formatter) visitSelectWhereClause(whereClause *ast.WhereClause) {
	if whereClause == nil {
		return
	}

	f.printNewLine()
	f.printKeyword(whereClause.WhereKeyword.TokenLiteral())
	f.printSpace()

	f.visitExpression(whereClause.Clause)
}

func (f *Formatter) visitSelectGroupByClause(groupByClause *ast.GroupByClause) {
	if groupByClause == nil {
		return
	}
	if len(groupByClause.Items) == 0 {
		return
	}

	f.printNewLine()
	f.printKeyword(groupByClause.GroupKeyword.TokenLiteral())
	f.printSpace()
	f.printKeyword(groupByClause.ByKeyword.TokenLiteral())
	f.printSpace()
	for i, e := range groupByClause.Items {
		if i > 0 {
			f.printSelectColumnComma()
		}
		f.visitExpression(e)
	}
}

func (f *Formatter) visitSelectHavingClause(havingClause *ast.HavingClause) {
	if havingClause == nil {
		return
	}

	f.printNewLine()
	f.printKeyword(havingClause.HavingKeyword.TokenLiteral())
	f.printSpace()
	f.visitExpression(havingClause.Clause)
}

func (f *Formatter) visitSelectOrderByClause(orderByClause *ast.OrderByClause) {
	if orderByClause == nil {
		return
	}

	f.printNewLine()
	f.printKeyword(orderByClause.OrderKeyword.TokenLiteral())
	f.printSpace()
	f.printKeyword(orderByClause.ByKeyword.TokenLiteral())
	f.printSpace()
	// f.printKeyword("order by ")
	for i, e := range orderByClause.Expressions {
		if i > 0 {
			f.printSelectColumnComma()
		}
		f.visitExpression(e.Column)
		if e.OrderKeyword != nil {
			f.printSpace()
			f.printKeyword(e.OrderKeyword.TokenLiteral())
		}
		// switch e.Type {
		// case ast.OBNone:
		// 	break
		// case ast.OBAsc:
		// 	f.printKeyword(" asc")
		// 	break
		// case ast.OBDesc:
		// 	f.printKeyword(" desc")
		// 	break
		// }
	}
	f.visitSelectOffsetFetchClause(orderByClause.OffsetFetch)
}

func (f *Formatter) visitSelectOffsetFetchClause(offsetFetchClause *ast.OffsetFetchClause) {
	if offsetFetchClause == nil {
		return
	}

	f.visitSelectOffsetClause(offsetFetchClause.Offset)
	f.visitSelectFetchClause(offsetFetchClause.Fetch)
}

func (f *Formatter) visitSelectOffsetClause(offsetArg ast.OffsetArg) {
	f.printNewLine()
	f.printSpace()
	f.printKeyword(offsetArg.OffsetKeyword.TokenLiteral())
	f.printSpace()
	f.visitExpression(offsetArg.Value)
	f.printSpace()
	f.printKeyword(offsetArg.RowOrRowsKeyword.TokenLiteral())
	f.printSpace()
	// switch offsetArg.RowOrRows {
	// case ast.RRRow:
	// 	f.printKeyword("row ")
	// 	break
	// case ast.RRRows:
	// 	f.printKeyword("rows ")
	// 	break
	// }
}

func (f *Formatter) visitSelectFetchClause(fetchArg *ast.FetchArg) {
	if fetchArg == nil {
		return
	}

	f.printNewLine()
	f.printKeyword(fetchArg.FetchKeyword.TokenLiteral())
	f.printSpace()
	f.printKeyword(fetchArg.NextOrFirstKeyword.TokenLiteral())
	f.printSpace()
	// switch fetchArg.NextOrFirst {
	// case ast.NFNext:
	// 	f.printKeyword("next ")
	// 	break
	// case ast.NFFirst:
	// 	f.printKeyword("first ")
	// 	break
	// }
	f.visitExpression(fetchArg.Value)
	f.printSpace()
	f.printKeyword(fetchArg.RowOrRowsKeyword.TokenLiteral())
	f.printSpace()
	f.printKeyword(fetchArg.OnlyKeyword.TokenLiteral())
	f.printSpace()
	// switch fetchArg.RowOrRows {
	// case ast.RRRow:
	// 	f.printKeyword("row ")
	// 	break
	// case ast.RRRows:
	// 	f.printKeyword("rows ")
	// 	break
	// }
	// f.printKeyword("only ")
}

func (f *Formatter) visitStringLiteralExpression(e *ast.ExprStringLiteral) {
	f.formattedQuery += fmt.Sprintf("'%s'", e.Value)
}

func (f *Formatter) visitNumberLiteralExpression(e *ast.ExprNumberLiteral) {
	f.formattedQuery += e.Value
}

func (f *Formatter) visitLocalVariableExpression(e *ast.ExprLocalVariable) {
	f.formattedQuery += fmt.Sprintf("@%s", e.Value)
}

func (f *Formatter) visitIdentifierExpression(e *ast.ExprIdentifier) {
	f.formattedQuery += e.Value
}

func (f *Formatter) visitQuotedIdentifierExpression(e *ast.ExprQuotedIdentifier) {
	f.formattedQuery += fmt.Sprintf("[%s]", e.Value)
}

func (f *Formatter) visitStarExpression() {
	f.formattedQuery += "*"
}

func (f *Formatter) visitExpressionWithAlias(e *ast.ExprWithAlias) {
	f.visitExpression(e.Expression)
	f.printSpace()
	if e.AsKeyword != nil {
		f.printKeyword(e.AsKeyword.TokenLiteral())
		f.printSpace()
	}
	f.visitExpression(e.Alias)
}

func (f *Formatter) visitComparisonOperatorExpression(e *ast.ExprComparisonOperator) {
	f.visitExpression(e.Left)
	f.printSpace()
	f.visitComparisonOperatorType(e.Operator)
	f.printSpace()
	f.visitExpression(e.Right)
}

func (f *Formatter) visitExpressionCompoundIdentifier(e *ast.ExprCompoundIdentifier) {
	for i, e := range e.Identifiers {
		if i > 0 {
			f.formattedQuery += "."
		}
		f.visitExpression(e)
	}
}

func (f *Formatter) visitExpressionSubquery(e *ast.ExprSubquery) {
	f.formattedQuery += "("
	f.increaseIndent()
	f.increaseIndent()
	f.printNewLine()

	f.visitSelectBody(&e.SelectBody)

	f.decreaseIndent()
	f.printNewLine()
	f.decreaseIndent()
	f.formattedQuery += ")"
}

func (f *Formatter) visitExpressionExpresssionList(e *ast.ExprExpressionList) {
	f.formattedQuery += "("
	for i, e := range e.List {
		if i > 0 {
			f.printExpressionListComma()
		}
		f.visitExpression(e)
	}
	f.formattedQuery += ")"
}

func (f *Formatter) visitExpressionFunction(e *ast.ExprFunction) {
	switch e.Type {
	case ast.FuncUserDefined:
		f.visitExpression(e.Name)
		break
	default:
		f.printKeyword(e.Name.TokenLiteral())
		break
	}
}

func (f *Formatter) visitExpressionFunctionCall(e *ast.ExprFunctionCall) {
	f.visitExpressionFunction(e.Name)
	f.formattedQuery += "("
	for i, a := range e.Args {
		if i > 0 {
			f.printExpressionListComma()
		}
		f.visitExpression(a)
	}
	f.formattedQuery += ")"
	if e.OverClause != nil {
		f.visitOverClause(e.OverClause)
	}
}

func (f *Formatter) visitOverClause(oc *ast.FunctionOverClause) {
	f.printSpace()
	f.printKeyword(oc.OverKeyword.TokenLiteral())
	f.printSpace()
	f.formattedQuery += "("
	if len(oc.PartitionByClause) > 0 {
		f.printKeyword(fmt.Sprintf("%s %s ", oc.PartitionKeyword.TokenLiteral(),
			oc.PByKeyword.TokenLiteral()))
	}
	for i, e := range oc.PartitionByClause {
		if i > 0 {
			f.printExpressionListComma()
		}
		f.visitExpression(e)
	}

	if len(oc.OrderByClause) > 0 {
		f.printSpace()
		f.printKeyword(oc.OrderKeyword.TokenLiteral())
		f.printSpace()
		f.printKeyword(oc.OByKeyword.TokenLiteral())
		f.printSpace()
	}
	for i, e := range oc.OrderByClause {
		if i > 0 {
			f.printExpressionListComma()
		}
		f.visitExpression(e.Column)
		if e.OrderKeyword != nil {
			f.printSpace()
			f.printKeyword(e.OrderKeyword.TokenLiteral())
		}
		// switch e.Type {
		// case ast.OBNone:
		// 	break
		// case ast.OBAsc:
		// 	f.printKeyword(" asc")
		// 	break
		// case ast.OBDesc:
		// 	f.printKeyword(" desc")
		// 	break
		// }
	}

	if oc.WindowFrameClause != nil {
		f.visitWindowFrameClause(oc.WindowFrameClause)
	}

	f.formattedQuery += ")"
}

func (f *Formatter) visitWindowFrameClause(wf *ast.WindowFrameClause) {
	f.visitRowsOrRange(wf.RowsOrRange)

	if wf.End != nil {
		f.printKeyword(wf.BetweenKeyword.TokenLiteral())
		f.printSpace()
	}

	if wf.Start.Expression != nil {
		f.visitExpression(wf.Start.Expression)
		f.printSpace()
	}
	f.printKeyword(fmt.Sprintf("%s", wf.Start.BoundKeyword1.TokenLiteral()))
	if wf.Start.BoundKeyword2 != nil {
		f.printKeyword(fmt.Sprintf(" %s", wf.Start.BoundKeyword2.TokenLiteral()))
	}

	if wf.End == nil {
		return
	}

	f.printSpace()
	f.printKeyword(wf.AndKeyword.TokenLiteral())
	f.printSpace()

	if wf.End.Expression != nil {
		f.visitExpression(wf.End.Expression)
		f.printSpace()
	}
	f.printKeyword(fmt.Sprintf("%s", wf.End.BoundKeyword1.TokenLiteral()))
	if wf.End.BoundKeyword2 != nil {
		f.printKeyword(fmt.Sprintf(" %s", wf.End.BoundKeyword2.TokenLiteral()))
	}
}

func (f *Formatter) visitComparisonOperatorType(op ast.ComparisonOperatorType) {
	switch op {
	case ast.ComparisonOpEqual:
		f.formattedQuery += "="
	case ast.ComparisonOpGreater:
		f.formattedQuery += ">"
	case ast.ComparisonOpGreaterEqual:
		f.formattedQuery += ">="
	case ast.ComparisonOpLess:
		f.formattedQuery += "<"
	case ast.ComparisonOpLessEqual:
		f.formattedQuery += "<="
	case ast.ComparisonOpNotEqualArrow:
		f.formattedQuery += "<>"
	case ast.ComparisonOpNotEqualBang:
		f.formattedQuery += "!="
	}
}

func (f *Formatter) visitWindowFrameBoundType(b ast.WindowFrameBoundType) {
	switch b {
	case ast.WFBTPreceding:
		f.printKeyword("preceding")
	case ast.WFBTFollowing:
		f.printKeyword("following")
	case ast.WFBTCurrentRow:
		f.printKeyword("current row")
	case ast.WFBTUnboundedPreceding:
		f.printKeyword("unbounded preceding")
	case ast.WFBTUnboundedFollowing:
		f.printKeyword("unbounded following")
	}
}

func (f *Formatter) visitRowsOrRange(r ast.RowsOrRangeType) {
	switch r {
	case ast.RRTRows:
		f.printKeyword(" rows ")
	case ast.RRTRange:
		f.printKeyword(" range ")
	}
}

func (f *Formatter) visitExpressionCast(e *ast.ExprCast) {
	f.printKeyword(fmt.Sprintf("%s(", e.CastKeyword.TokenLiteral()))
	f.visitExpression(e.Expression)
	f.printKeyword(fmt.Sprintf(" %s ", e.AsKeyword.TokenLiteral()))
	f.visitDataType(&e.DataType)
	f.formattedQuery += ")"
}

func (f *Formatter) visitDataType(dt *ast.DataType) {
	switch dt.Kind {
	case ast.DTInt:
		f.printKeyword("INT")
		break
	case ast.DTBigInt:
		f.printKeyword("BIGINT")
		break
	case ast.DTTinyInt:
		f.printKeyword("TINYINT")
		break
	case ast.DTSmallInt:
		f.printKeyword("SMALLINT")
		break
	case ast.DTBit:
		f.printKeyword("BIT")
		break
	case ast.DTFloat:
		f.printKeyword("FLOAT")
		if dt.FloatPrecision != nil {
			f.formattedQuery += fmt.Sprintf("(%d)", *dt.FloatPrecision)
		}
		break
	case ast.DTReal:
		f.printKeyword("REAL")
		break
	case ast.DTDate:
		f.printKeyword("DATE")
		break
	case ast.DTDatetime:
		f.printKeyword("DATETIME")
		break
	case ast.DTTime:
		f.printKeyword("TIME")
		break
	case ast.DTDecimal:
		f.printKeyword("DECIMAL")
		if dt.DecimalNumericSize != nil {
			f.visitNumericSize(dt.DecimalNumericSize)
		}
		break
	case ast.DTNumeric:
		f.printKeyword("NUMERIC")
		if dt.DecimalNumericSize != nil {
			f.visitNumericSize(dt.DecimalNumericSize)
		}
		break
	case ast.DTVarchar:
		f.printKeyword("VARCHAR")
		if dt.VarcharLength != nil {
			f.formattedQuery += fmt.Sprintf("(%d)", *dt.VarcharLength)
		}
		break
	}
}

func (f *Formatter) visitNumericSize(ns *ast.NumericSize) {
	f.formattedQuery += fmt.Sprintf("%d", ns.Precision)
	if ns.Scale != nil {
		f.formattedQuery += fmt.Sprintf(", %d", *ns.Scale)
	}
}

func (f *Formatter) visitUnaryOperatorExpression(e *ast.ExprUnaryOperator) {
	f.visitUnaryOperatorType(e.Operator)
	f.visitExpression(e.Right)
}

func (f *Formatter) visitUnaryOperatorType(o ast.UnaryOperatorType) {
	switch o {
	case ast.UnaryOpPlus:
		f.formattedQuery += "+"
	case ast.UnaryOpMinus:
		f.formattedQuery += "-"
	}
}

func (f *Formatter) visitArithmeticOperatorExpression(e *ast.ExprArithmeticOperator) {
	f.visitExpression(e.Left)
	f.visitArithmeticOperatorType(e.Operator)
	f.visitExpression(e.Right)
}

func (f *Formatter) visitArithmeticOperatorType(o ast.ArithmeticOperatorType) {
	switch o {
	case ast.ArithmeticOpPlus:
		f.formattedQuery += "+"
	case ast.ArithmeticOpMinus:
		f.formattedQuery += "-"
	case ast.ArithmeticOpMult:
		f.formattedQuery += "*"
	case ast.ArithmeticOpDiv:
		f.formattedQuery += "/"
	case ast.ArithmeticOpMod:
		f.formattedQuery += "%"
	}
}

func (f *Formatter) visitAndLogicalOperatorExpression(e *ast.ExprAndLogicalOperator) {
	f.visitExpression(e.Left)

	f.increaseIndent()
	f.printNewLine()

	f.printKeyword(fmt.Sprintf("%s ", e.AndKeyword.TokenLiteral()))
	f.visitExpression(e.Right)

	f.decreaseIndent()
}

func (f *Formatter) visitAllLogicalOperatorExpression(e *ast.ExprAllLogicalOperator) {
	f.visitExpression(e.ScalarExpression)
	f.visitComparisonOperatorType(e.ComparisonOperator)
	f.printKeyword(fmt.Sprintf(" %s ", e.AllKeyword.TokenLiteral()))
	f.visitExpressionSubquery(e.Subquery)
}

func (f *Formatter) visitBetweenLogicalOperatorExpression(e *ast.ExprBetweenLogicalOperator) {
	f.visitExpression(e.TestExpression)
	if e.NotKeyword != nil {
		f.printKeyword(fmt.Sprintf(" %s", e.NotKeyword.TokenLiteral()))
	}
	f.printKeyword(fmt.Sprintf(" %s ", e.BetweenKeyword.TokenLiteral()))
	f.visitExpression(e.Begin)
	f.printKeyword(fmt.Sprintf(" %s ", e.AndKeyword.TokenLiteral()))
	f.visitExpression(e.End)
}

func (f *Formatter) visitExistsLogicalOperatorExpression(e *ast.ExprExistsLogicalOperator) {
	f.printKeyword(fmt.Sprintf("%s", e.ExistsKeyword.TokenLiteral()))
	f.visitExpressionSubquery(e.Subquery)
}

func (f *Formatter) visitInSubqueryLogicalOperatorExpression(e *ast.ExprInSubqueryLogicalOperator) {
	f.visitExpression(e.TestExpression)
	if e.NotKeyword != nil {
		f.printKeyword(fmt.Sprintf(" %s", e.NotKeyword.TokenLiteral()))
	}
	f.printKeyword(fmt.Sprintf(" %s ", e.InKeyword.TokenLiteral()))
	f.visitExpressionSubquery(e.Subquery)
}

func (f *Formatter) visitInLogicalOperatorExpression(e *ast.ExprInLogicalOperator) {
	f.visitExpression(e.TestExpression)
	if e.NotKeyword != nil {
		f.printKeyword(fmt.Sprintf(" %s", e.NotKeyword.TokenLiteral()))
	}
	f.printKeyword(fmt.Sprintf(" %s ", e.InKeyword.TokenLiteral()))
	f.formattedQuery += "("
	for i, e := range e.Expressions {
		if i == 0 && f.settings.IndentInLists {
			f.increaseIndent()
			f.increaseIndent()
			f.printNewLine()
			f.decreaseIndent()
		}
		if i > 0 {
			f.printInListComma()
		}
		f.visitExpression(e)
	}
	if f.settings.IndentInLists {
		f.printNewLine()
		f.decreaseIndent()
	}
	f.formattedQuery += ")"
}

func (f *Formatter) visitLikeLogicalOperatorExpression(e *ast.ExprLikeLogicalOperator) {
	f.visitExpression(e.MatchExpression)
	if e.NotKeyword != nil {
		f.printKeyword(fmt.Sprintf(" %s", e.NotKeyword.TokenLiteral()))
	}
	f.printKeyword(fmt.Sprintf(" %s ", e.LikeKeyword.TokenLiteral()))
	f.visitExpression(e.Pattern)
}

func (f *Formatter) visitNotLogicalOperatorExpression(e *ast.ExprNotLogicalOperator) {
	f.printKeyword(fmt.Sprintf("%s ", e.NotKeyword.TokenLiteral()))
	f.visitExpression(e.Expression)
}

func (f *Formatter) visitOrLogicalOperatorExpression(e *ast.ExprOrLogicalOperator) {
	f.visitExpression(e.Left)
	f.printKeyword(fmt.Sprintf(" %s ", e.OrKeyword.TokenLiteral()))
	f.visitExpression(e.Right)
}

func (f *Formatter) visitSomeLogicalOperatorExpression(e *ast.ExprSomeLogicalOperator) {
	f.visitExpression(e.ScalarExpression)
	f.visitComparisonOperatorType(e.ComparisonOperator)
	f.printKeyword(fmt.Sprintf(" %s ", e.SomeKeyword.TokenLiteral()))
	f.visitExpressionSubquery(e.Subquery)
}

func (f *Formatter) visitAnyLogicalOperatorExpression(e *ast.ExprAnyLogicalOperator) {
	f.visitExpression(e.ScalarExpression)
	f.visitComparisonOperatorType(e.ComparisonOperator)
	f.printKeyword(fmt.Sprintf(" %s ", e.AnyKeyword.TokenLiteral()))
	f.visitExpressionSubquery(e.Subquery)
}
