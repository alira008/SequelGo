package formatter

import (
	"SequelGo/internal/ast"
	"SequelGo/internal/lexer"
	"SequelGo/internal/parser"
	"fmt"
	"math"
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

	f.associateCommentsWithNodes(&query)
	ast.Walk(f, &query)

	return f.formattedQuery, nil
}

func (f *Formatter) Visit(node ast.Node) ast.Visitor {
	// f.printCommentsBeforeNode(node)

	switch n := node.(type) {
	case *ast.Query:
		for i, s := range n.Statements {
			if i > 0 {
				f.printNewLine()
				f.printNewLine()
			}
			ast.Walk(f, s)
            // f.printCommentsAfterNode(s)
		}
		break
	case *ast.SelectStatement:
		return f
	case *ast.SelectBody:
		ast.Walk(f, &n.SelectKeyword)
		if n.AllKeyword != nil {
			f.printSpace()
			ast.Walk(f, n.AllKeyword)
		}
		if n.Distinct != nil {
			f.printSpace()
			ast.Walk(f, n.Distinct)
		}
		if n.Top != nil {
			ast.Walk(f, n.Top)
		}
		ast.Walk(f, &n.SelectItems)
		ast.Walk(f, n.Table)
		if n.WhereClause != nil {
			ast.Walk(f, n.WhereClause)
		}
		if n.HavingClause != nil {
			ast.Walk(f, n.HavingClause)
		}
		if n.GroupByClause != nil {
			ast.Walk(f, n.GroupByClause)
		}
		if n.OrderByClause != nil {
			ast.Walk(f, n.OrderByClause)
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
		ast.Walk(f, n.Expression)
		f.printSpace()
		if n.AsKeyword != nil {
			ast.Walk(f, n.AsKeyword)
			f.printSpace()
		}
		ast.Walk(f, n.Alias)
		break
	case *ast.ExprCompoundIdentifier:
		for i, e := range n.Identifiers {
			if i > 0 {
				f.formattedQuery += "."
			}
			ast.Walk(f, e)
		}
		break
	case *ast.SelectItems:
		if len(n.Items) > 1 {
			f.increaseIndent()
			f.printNewLine()
			f.decreaseIndent()
		} else {
			f.printSpace()
		}
		for i, e := range n.Items {
			if i > 0 {
				f.printSelectColumnComma()
			}
			ast.Walk(f, e)
		}
		break
	case *ast.WhereClause:
		f.printNewLine()
		ast.Walk(f, &n.WhereKeyword)
		f.printSpace()
		ast.Walk(f, n.Clause)
		break
	case *ast.HavingClause:
		f.printNewLine()
		ast.Walk(f, &n.HavingKeyword)
		f.printSpace()
		ast.Walk(f, n.Clause)
		break
	case *ast.GroupByClause:
		f.printNewLine()
		ast.Walk(f, &n.GroupKeyword)
		ast.Walk(f, &n.ByKeyword)
		f.printSpace()
		for _, e := range n.Items {
			ast.Walk(f, e)
		}
		break
	case *ast.TableArg:
		f.printNewLine()
		ast.Walk(f, &n.FromKeyword)
		f.printSpace()
		ast.Walk(f, n.Table)
		for _, j := range n.Joins {
			ast.Walk(f, &j)
		}
		break
	case *ast.TableSource:
		return f
	case *ast.Join:
		f.printNewLine()
		ast.Walk(f, &n.JoinTypeKeyword1)
		f.printSpace()
		if n.JoinTypeKeyword2 != nil {
			ast.Walk(f, n.JoinTypeKeyword2)
			f.printSpace()
		}
		ast.Walk(f, &n.JoinKeyword)
		f.printSpace()
		ast.Walk(f, n.Table)
		f.printSpace()
		if n.OnKeyword != nil {
			ast.Walk(f, n.OnKeyword)
			f.printSpace()
		}
		if n.Condition != nil {
			ast.Walk(f, n.Condition)
		}
		break
	case *ast.TopArg:
		f.printSpace()
		ast.Walk(f, &n.TopKeyword)
		f.printSpace()
		ast.Walk(f, n.Quantity)
		if n.PercentKeyword != nil {
			f.printSpace()
			ast.Walk(f, n.PercentKeyword)
		}
		if n.WithKeyword != nil {
			f.printSpace()
			ast.Walk(f, n.WithKeyword)

			if n.TiesKeyword != nil {
				f.printSpace()
				ast.Walk(f, n.TiesKeyword)
			}
		}
		break
	case *ast.OrderByArg:
		ast.Walk(f, n.Column)
		if n.OrderKeyword != nil {
			f.printSpace()
			ast.Walk(f, n.OrderKeyword)
		}
		break
	case *ast.OrderByClause:
		f.printNewLine()
		ast.Walk(f, &n.OrderKeyword)
		f.printSpace()
		ast.Walk(f, &n.ByKeyword)
		f.printSpace()
		for i, e := range n.Expressions {
			if i > 0 {
				f.printSpace()
			}
			ast.Walk(f, &e)
		}

		if n.OffsetFetch != nil {
			ast.Walk(f, n.OffsetFetch)
		}
		break
	case *ast.OffsetArg:
		f.printNewLine()
		ast.Walk(f, &n.OffsetKeyword)
		f.printSpace()
		ast.Walk(f, n.Value)
		f.printSpace()
		ast.Walk(f, &n.RowOrRowsKeyword)
		break
	case *ast.FetchArg:
		f.printNewLine()
		ast.Walk(f, &n.FetchKeyword)
		f.printSpace()
		ast.Walk(f, &n.NextOrFirstKeyword)
		f.printSpace()
		ast.Walk(f, n.Value)
		f.printSpace()
		ast.Walk(f, &n.RowOrRowsKeyword)
		f.printSpace()
		ast.Walk(f, &n.OnlyKeyword)
		break
	case *ast.OffsetFetchClause:
		ast.Walk(f, &n.Offset)
		if n.Fetch != nil {
			ast.Walk(f, n.Fetch)
		}
		break
	case *ast.ExprSubquery:
		f.formattedQuery += "("
		f.increaseIndent()
		f.increaseIndent()
		f.printNewLine()

		ast.Walk(f, &n.SelectBody)

		f.decreaseIndent()
		f.printNewLine()
		f.decreaseIndent()
		f.formattedQuery += ")"
		break
	case *ast.ExprExpressionList:
		for i, e := range n.List {
			if i > 0 {
				f.printExpressionListComma()
			}
			ast.Walk(f, e)
		}
		break
	case *ast.ExprFunction:
		if n.Type == ast.FuncUserDefined {
			ast.Walk(f, n.Name)
		} else {
			f.printKeyword(n.Name.TokenLiteral())
		}
		break
	case *ast.WindowFrameBound:
		ast.Walk(f, &n.BoundKeyword1)
		if n.BoundKeyword2 != nil {
			f.printSpace()
			ast.Walk(f, n.BoundKeyword2)
		}
		if n.Expression != nil {
			f.printSpace()
			ast.Walk(f, n.Expression)
		}
		break
	case *ast.WindowFrameClause:
		f.increaseIndent()
		f.increaseIndent()
		f.printNewLine()
		ast.Walk(f, &n.RowsOrRangeKeyword)
		f.printSpace()
		if n.BetweenKeyword != nil {
			ast.Walk(f, n.BetweenKeyword)
			f.printSpace()
		}
		ast.Walk(f, n.Start)
		if n.AndKeyword != nil {
			f.increaseIndent()
			f.printNewLine()
			ast.Walk(f, n.AndKeyword)
			f.printSpace()
		}
		if n.End != nil {
			ast.Walk(f, n.End)
			f.decreaseIndent()
		}
		f.decreaseIndent()
		f.decreaseIndent()
		break
	case *ast.FunctionOverClause:
		f.increaseIndent()
		f.increaseIndent()
		f.printNewLine()
		ast.Walk(f, &n.OverKeyword)
		f.printSpace()
		f.formattedQuery += "("
		if n.PartitionKeyword != nil {
			ast.Walk(f, n.PartitionKeyword)
			f.printSpace()
		}
		if n.PByKeyword != nil {
			ast.Walk(f, n.PByKeyword)
			f.printSpace()
		}
		for i, e := range n.PartitionByClause {
			if i > 0 {
				f.printSpace()
			}
			ast.Walk(f, e)
		}

		if n.PartitionKeyword != nil && n.OrderKeyword != nil {
			f.printSpace()
		}

		if n.OrderKeyword != nil {
			ast.Walk(f, n.OrderKeyword)
			f.printSpace()
		}
		if n.OByKeyword != nil {
			ast.Walk(f, n.OByKeyword)
			f.printSpace()
		}
		for i, e := range n.OrderByClause {
			if i > 0 {
				f.printSpace()
			}
			ast.Walk(f, &e)
		}
		if n.WindowFrameClause != nil {
			ast.Walk(f, n.WindowFrameClause)
		}
		f.formattedQuery += ")"
		f.decreaseIndent()
		f.decreaseIndent()
		break
	case *ast.ExprFunctionCall:
		ast.Walk(f, n.Name)
		f.formattedQuery += "("
		for i, a := range n.Args {
			if i > 0 {
				f.printExpressionListComma()
			}
			ast.Walk(f, a)
		}
		f.formattedQuery += ")"
		if n.OverClause != nil {
			ast.Walk(f, n.OverClause)
		}
		break
	case *ast.ExprCast:
		ast.Walk(f, &n.CastKeyword)
		f.formattedQuery += "("
		ast.Walk(f, n.Expression)
		f.printSpace()
		ast.Walk(f, &n.AsKeyword)
		f.printSpace()
		ast.Walk(f, &n.DataType)
		f.formattedQuery += ")"
		break
	case *ast.CommonTableExpression:
		f.formattedQuery += n.Name
		if n.Columns != nil {
			f.printSpace()
			ast.Walk(f, n.Columns)
		}
		f.printNewLine()
		ast.Walk(f, &n.AsKeyword)
		f.formattedQuery += "("
		f.increaseIndent()
		ast.Walk(f, &n.Query)
		f.formattedQuery += ")"
		f.decreaseIndent()
		break
	case *ast.DataType:
		switch n.Kind {
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
			if n.FloatPrecision != nil {
				f.formattedQuery += fmt.Sprintf("(%d)", *n.FloatPrecision)
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
			if n.DecimalNumericSize != nil {
				ast.Walk(f, n.DecimalNumericSize)
			}
			break
		case ast.DTNumeric:
			f.printKeyword("NUMERIC")
			if n.DecimalNumericSize != nil {
				ast.Walk(f, n.DecimalNumericSize)
			}
			break
		case ast.DTVarchar:
			f.printKeyword("VARCHAR")
			if n.VarcharLength != nil {
				f.formattedQuery += fmt.Sprintf("(%d)", *n.VarcharLength)
			}
			break
		}
		break
	case *ast.NumericSize:
		f.formattedQuery += fmt.Sprintf("%d", n.Precision)
		if n.Scale != nil {
			f.formattedQuery += fmt.Sprintf(", %d", *n.Scale)
		}
		break
	case *ast.ExprUnaryOperator:
		f.visitUnaryOperatorType(n.Operator)
		ast.Walk(f, n.Right)
		break
	case *ast.ExprComparisonOperator:
		ast.Walk(f, n.Left)
		f.printSpace()
		f.visitComparisonOperatorType(n.Operator)
		f.printSpace()
		ast.Walk(f, n.Right)
		break
	case *ast.ExprArithmeticOperator:
		ast.Walk(f, n.Left)
		f.printSpace()
		f.visitArithmeticOperatorType(n.Operator)
		f.printSpace()
		ast.Walk(f, n.Right)
		break
	case *ast.ExprAndLogicalOperator:
		ast.Walk(f, n.Left)
		f.increaseIndent()
		f.printNewLine()
		ast.Walk(f, &n.AndKeyword)
		f.printSpace()
		ast.Walk(f, n.Right)
		f.decreaseIndent()
		break
	case *ast.ExprAllLogicalOperator:
		ast.Walk(f, n.ScalarExpression)
		f.printSpace()
		f.visitComparisonOperatorType(n.ComparisonOperator)
		f.printSpace()
		ast.Walk(f, &n.AllKeyword)
		f.printSpace()
		ast.Walk(f, n.Subquery)
		break
	case *ast.ExprBetweenLogicalOperator:
		ast.Walk(f, n.TestExpression)
		f.printSpace()
		if n.NotKeyword != nil {
			ast.Walk(f, n.NotKeyword)
			f.printSpace()
		}
		ast.Walk(f, &n.BetweenKeyword)
		f.printSpace()
		ast.Walk(f, n.Begin)
		f.increaseIndent()
		f.increaseIndent()
		f.printNewLine()
		ast.Walk(f, &n.AndKeyword)
		f.printSpace()
		ast.Walk(f, n.End)
		f.decreaseIndent()
		f.decreaseIndent()
		break
	case *ast.ExprExistsLogicalOperator:
		return f
		// break
	case *ast.ExprInSubqueryLogicalOperator:
		ast.Walk(f, n.TestExpression)
		f.printSpace()

		if n.NotKeyword != nil {
			ast.Walk(f, n.NotKeyword)
			f.printSpace()
		}

		ast.Walk(f, &n.InKeyword)
		f.printSpace()
		ast.Walk(f, n.Subquery)
		break
	case *ast.ExprInLogicalOperator:
		ast.Walk(f, n.TestExpression)
		f.printSpace()

		ast.Walk(f, &n.InKeyword)
		f.printSpace()

		if n.NotKeyword != nil {
			ast.Walk(f, n.NotKeyword)
			f.printSpace()
		}

		f.formattedQuery += "("
		for i, e := range n.Expressions {
			if i == 0 && f.settings.IndentInLists {
				f.increaseIndent()
				f.increaseIndent()
				f.printNewLine()
				f.decreaseIndent()
			}
			if i > 0 {
				f.printInListComma()
			}
			ast.Walk(f, e)
		}
		if f.settings.IndentInLists {
			f.printNewLine()
			f.decreaseIndent()
		}
		f.formattedQuery += ")"
		break
	case *ast.ExprLikeLogicalOperator:
		ast.Walk(f, n.MatchExpression)
		f.printSpace()

		if n.NotKeyword != nil {
			ast.Walk(f, n.NotKeyword)
			f.printSpace()
		}

		ast.Walk(f, &n.LikeKeyword)
		f.printSpace()

		ast.Walk(f, n.Pattern)
		break
	case *ast.ExprNotLogicalOperator:
		ast.Walk(f, &n.NotKeyword)
		f.printSpace()
		ast.Walk(f, n.Expression)
		break
	case *ast.ExprOrLogicalOperator:
		ast.Walk(f, n.Left)
		f.increaseIndent()
		f.printNewLine()
		ast.Walk(f, &n.OrKeyword)
		f.printSpace()
		ast.Walk(f, n.Right)
		f.decreaseIndent()
		break
	case *ast.ExprSomeLogicalOperator:
		ast.Walk(f, n.ScalarExpression)
		f.printSpace()
		f.visitComparisonOperatorType(n.ComparisonOperator)
		f.printSpace()
		ast.Walk(f, &n.SomeKeyword)
		f.printSpace()
		ast.Walk(f, n.Subquery)
		break
	case *ast.ExprAnyLogicalOperator:
		ast.Walk(f, n.ScalarExpression)
		f.printSpace()
		f.visitComparisonOperatorType(n.ComparisonOperator)
		f.printSpace()
		ast.Walk(f, &n.AnyKeyword)
		f.printSpace()
		ast.Walk(f, n.Subquery)
		break
	case *ast.Keyword:
		f.printKeyword(n.TokenLiteral())
		break
	case nil:
		break
	default:
		return f
	}

	// f.printCommentsAfterNode(node)
	return nil
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
		// fmt.Println("\tclosest node: ", closestNode.TokenLiteral(), "\n\tcomment: ", comment.TokenLiteral())
		if closestNode != nil {
			f.NodeToComments[closestNode] = append(f.NodeToComments[closestNode], comment)
		}
	}
}
func (f *Formatter) findClosestNode(commentPos ast.Position, nodes []ast.Node) ast.Node {
	var closestNode ast.Node
	minDistance := int64(math.MaxInt64) // Initialize to max int value
	for _, node := range nodes {
		distance := f.positionDistance(commentPos, node.GetSpan().EndPosition)
		if distance < minDistance {
			minDistance = distance
			closestNode = node
		}
	}
	// fmt.Println("distance ", minDistance)
	return closestNode
}

func (f *Formatter) positionDistance(pos1, pos2 ast.Position) int64 {
	// Simple distance measure considering line difference first, then column difference
	lineDiff := abs(int64(pos1.Line) - int64(pos2.Line))
	if lineDiff == 0 {
		columnDiff := abs(int64(pos1.Col) - int64(pos2.Col))
		return columnDiff
	} else {
		columnDiff := abs(int64(pos1.Col)-int64(math.MaxInt32)) + abs(int64(pos2.Col)-int64(math.MaxInt32))
		return lineDiff*1000 + columnDiff
	}
	// return lineDiff*1000 + columnDiff // Assuming line difference is more significant
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

func (f *Formatter) printCommentsBeforeNode(node ast.Node) {
	if comments, ok := f.NodeToComments[node]; ok {
		for _, comment := range comments {
			if comment.GetSpan().StartPosition.Line < node.GetSpan().StartPosition.Line {
				f.formattedQuery += fmt.Sprintf("%s", comment.TokenLiteral())
				f.printNewLine()
			}
		}
	}
}

func (f *Formatter) printCommentsAfterNode(node ast.Node) {
	if comments, ok := f.NodeToComments[node]; ok {
		for _, comment := range comments {
			if comment.GetSpan().StartPosition.Line == node.GetSpan().StartPosition.Line &&
				comment.GetSpan().StartPosition.Col > node.GetSpan().StartPosition.Col {
				f.formattedQuery += fmt.Sprintf(" %s", comment.TokenLiteral())
				f.printNewLine()
			}
			if comment.GetSpan().StartPosition.Line > node.GetSpan().StartPosition.Line {
				f.printNewLine()
				f.formattedQuery += fmt.Sprintf("%s", comment.TokenLiteral())
			}
		}
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

func (f *Formatter) visitUnaryOperatorType(o ast.UnaryOperatorType) {
	switch o {
	case ast.UnaryOpPlus:
		f.formattedQuery += "+"
	case ast.UnaryOpMinus:
		f.formattedQuery += "-"
	}
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
