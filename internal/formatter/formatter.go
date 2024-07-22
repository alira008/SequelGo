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
}

func NewFormatter(settings Settings, logger *zap.SugaredLogger) Formatter {
	return Formatter{settings: settings, logger: logger}
}

func (f *Formatter) Format(input string) (string, error) {
	l := lexer.NewLexer(input)
	p := parser.NewParser(f.logger, l)
	query := p.Parse()
	if len(p.Errors()) > 0 {
		return "", fmt.Errorf(strings.Join(p.Errors(), "\n"))
	}

	for _, s := range query.Statements {
		f.walkQuery(s)
	}

	return f.formattedQuery, nil
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

func (f *Formatter) visitExpression(expression ast.Expression) {
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
	}
}

func (f *Formatter) visitSelectQuery(selectStatement *ast.SelectStatement) {
	f.printKeyword("select")
	if selectStatement.SelectBody.Distinct {
		f.printKeyword(" distinct")
	}
	f.visitSelectTopArg(selectStatement.SelectBody.Top)
	f.visitSelectItems(selectStatement.SelectBody.SelectItems)
	f.visitSelectTableArg(selectStatement.SelectBody.Table)
	f.visitSelectWhereClause(selectStatement.SelectBody.WhereClause)
	f.visitSelectGroupByClause(selectStatement.SelectBody.GroupByClause)
	f.visitSelectHavingClause(selectStatement.SelectBody.HavingClause)
	f.visitSelectOrderByClause(selectStatement.SelectBody.OrderByClause)
}

func (f *Formatter) visitSelectTopArg(selectTopArg *ast.TopArg) {
	if selectTopArg == nil {
		return
	}

	f.printKeyword(" top ")
	f.visitExpression(selectTopArg.Quantity)
	if selectTopArg.Percent {
		f.printKeyword(" percent")
	}
	if selectTopArg.WithTies {
		f.printKeyword(" with ties")
	}
}

func (f *Formatter) visitSelectItems(items []ast.Expression) {
	for i, e := range items {
        if i == 0 {
            f.increaseIndent()
            f.printNewLine()
            f.decreaseIndent()
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
	f.printKeyword("from ")
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
	switch tableJoin.Type {
	case ast.JTInner:
		f.printKeyword("inner join ")
		break
	case ast.JTLeft:
		f.printKeyword("left join ")
		break
	case ast.JTLeftOuter:
		f.printKeyword("left outer join ")
		break
	case ast.JTRight:
		f.printKeyword("right join ")
		break
	case ast.JTRightOuter:
		f.printKeyword("right outer join ")
		break
	case ast.JTFull:
		f.printKeyword("full join ")
		break
	case ast.JTFullOuter:
		f.printKeyword("full outer join ")
		break
	}

	f.visitTableSource(tableJoin.Table)
	f.printKeyword(" on ")
	if tableJoin.Condition != nil {
		f.visitExpression(tableJoin.Condition)
	}
}

func (f *Formatter) visitSelectWhereClause(expression ast.Expression) {
	if expression == nil {
		return
	}

	f.printNewLine()
	f.printKeyword("where ")
	f.visitExpression(expression)
}

func (f *Formatter) visitSelectGroupByClause(expressions []ast.Expression) {
	if expressions == nil {
		return
	}
	if len(expressions) == 0 {
		return
	}

	f.printNewLine()
	f.printKeyword("group by ")
	for i, e := range expressions {
		if i > 0 {
			f.printSelectColumnComma()
		}
		f.visitExpression(e)
	}
}

func (f *Formatter) visitSelectHavingClause(expression ast.Expression) {
	if expression == nil {
		return
	}

	f.printNewLine()
	f.printKeyword("having ")
	f.visitExpression(expression)
}

func (f *Formatter) visitSelectOrderByClause(orderByClause *ast.OrderByClause) {
	if orderByClause == nil {
		return
	}

	f.printNewLine()
	f.printKeyword("order by ")
	for i, e := range orderByClause.Expressions {
		if i > 0 {
			f.printSelectColumnComma()
		}
		f.visitExpression(e.Column)
		switch e.Type {
		case ast.OBNone:
			break
		case ast.OBAsc:
			f.printKeyword(" asc")
			break
		case ast.OBDesc:
			f.printKeyword(" desc")
			break
		}
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
	f.printKeyword(" offset ")
	f.visitExpression(offsetArg.Value)
	f.printSpace()
	switch offsetArg.RowOrRows {
	case ast.RRRow:
		f.printKeyword("row ")
		break
	case ast.RRRows:
		f.printKeyword("rows ")
		break
	}
}

func (f *Formatter) visitSelectFetchClause(fetchArg *ast.FetchArg) {
	if fetchArg == nil {
		return
	}

	f.printNewLine()
	f.printKeyword("fetch ")
	switch fetchArg.NextOrFirst {
	case ast.NFNext:
		f.printKeyword("next ")
		break
	case ast.NFFirst:
		f.printKeyword("first ")
		break
	}
	f.visitExpression(fetchArg.Value)
	f.printSpace()
	switch fetchArg.RowOrRows {
	case ast.RRRow:
		f.printKeyword("row ")
		break
	case ast.RRRows:
		f.printKeyword("rows ")
		break
	}
	f.printKeyword("only ")
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
	if e.AsTokenPresent {
		f.printKeyword("as ")
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

	f.printKeyword("select")
	if e.Distinct {
		f.printKeyword(" distinct")
	}
	f.visitSelectTopArg(e.Top)
	f.visitSelectItems(e.SelectItems)
	f.visitSelectTableArg(e.Table)
	f.visitSelectWhereClause(e.WhereClause)
	f.visitSelectGroupByClause(e.GroupByClause)
	f.visitSelectHavingClause(e.HavingClause)
	f.visitSelectOrderByClause(e.OrderByClause)

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
		f.visitExpression(e)
		break
	default:
		f.printKeyword(e.Name.TokenLiteral())
		break
	}
}

func (f *Formatter) visitExpressionFunctionCall(e *ast.ExprFunctionCall) {
	f.visitExpressionFunction(e.Name)
	f.formattedQuery += "("
	for i, e := range e.Args {
		if i > 0 {
			f.printExpressionListComma()
		}
		f.visitExpression(e)
	}
	f.formattedQuery += ")"
	if e.OverClause != nil {
		f.visitOverClause(e.OverClause)
	}
}

func (f *Formatter) visitOverClause(oc *ast.FunctionOverClause) {
	if len(oc.PartitionByClause) > 0 {
		f.printKeyword("partition by ")
	}
	for i, e := range oc.PartitionByClause {
		if i > 0 {
			f.printExpressionListComma()
		}
		f.visitExpression(e)
	}

	if len(oc.OrderByClause) > 0 {
		f.printKeyword("order by ")
	}
	for i, e := range oc.OrderByClause {
		if i > 0 {
			f.printExpressionListComma()
		}
		f.visitExpression(e)
	}

	if oc.WindowFrameClause != nil {
		f.visitWindowFrameClause(oc.WindowFrameClause)
	}
}

func (f *Formatter) visitWindowFrameClause(wf *ast.WindowFrameClause) {
	f.visitRowsOrRange(wf.RowsOrRange)

	if wf.End != nil {
		f.printKeyword("between ")
	}

	f.visitWindowFrameBoundType(wf.Start.Type)
	f.visitExpression(wf.Start.Expression)

	if wf.End == nil {
		return
	}

	f.printKeyword("and ")
	f.visitWindowFrameBoundType(wf.End.Type)
	f.visitExpression(wf.End.Expression)

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
		f.printKeyword("preceding ")
	case ast.WFBTFollowing:
		f.printKeyword("following ")
	case ast.WFBTCurrentRow:
		f.printKeyword("current row ")
	case ast.WFBTUnboundedPreceding:
		f.printKeyword("unbounded preceding ")
	case ast.WFBTUnboundedFollowing:
		f.printKeyword("unbounded following ")
	}
}

func (f *Formatter) visitRowsOrRange(r ast.RowsOrRangeType) {
	switch r {
	case ast.RRTRows:
		f.printKeyword("rows ")
	case ast.RRTRange:
		f.printKeyword("range ")
	}
}

func (f *Formatter) visitExpressionCast(e *ast.ExprCast) {
	f.printKeyword("cast(")
	f.visitExpression(e.Expression)
	f.printKeyword(" as ")
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

	f.printKeyword("and ")
	f.visitExpression(e.Right)

	f.decreaseIndent()
}

func (f *Formatter) visitAllLogicalOperatorExpression(e *ast.ExprAllLogicalOperator) {
	f.visitExpression(e.ScalarExpression)
	f.visitComparisonOperatorType(e.ComparisonOperator)
	f.printKeyword(" all ")
	f.visitExpressionSubquery(e.Subquery)
}

func (f *Formatter) visitBetweenLogicalOperatorExpression(e *ast.ExprBetweenLogicalOperator) {
	f.visitExpression(e.TestExpression)
	if e.Not {
		f.printKeyword(" not")
	}
	f.printKeyword(" between ")
	f.visitExpression(e.Begin)
	f.printKeyword(" and ")
	f.visitExpression(e.End)
}

func (f *Formatter) visitExistsLogicalOperatorExpression(e *ast.ExprExistsLogicalOperator) {
	f.printKeyword("exists")
	f.visitExpressionSubquery(e.Subquery)
}

func (f *Formatter) visitInSubqueryLogicalOperatorExpression(e *ast.ExprInSubqueryLogicalOperator) {
	f.visitExpression(e.TestExpression)
	if e.Not {
		f.printKeyword(" not")
	}
	f.printKeyword(" in ")
	f.visitExpressionSubquery(e.Subquery)
}

func (f *Formatter) visitInLogicalOperatorExpression(e *ast.ExprInLogicalOperator) {
	f.visitExpression(e.TestExpression)
	if e.Not {
		f.printKeyword(" not")
	}
	f.printKeyword(" in ")
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
	if e.Not {
		f.printKeyword(" not")
	}
	f.printKeyword(" like ")
	f.visitExpression(e.Pattern)
}

func (f *Formatter) visitNotLogicalOperatorExpression(e *ast.ExprNotLogicalOperator) {
	f.printKeyword("not ")
	f.visitExpression(e.Expression)
}

func (f *Formatter) visitOrLogicalOperatorExpression(e *ast.ExprOrLogicalOperator) {
	f.visitExpression(e.Left)
	f.printKeyword(" or ")
	f.visitExpression(e.Right)
}

func (f *Formatter) visitSomeLogicalOperatorExpression(e *ast.ExprSomeLogicalOperator) {
	f.visitExpression(e.ScalarExpression)
	f.visitComparisonOperatorType(e.ComparisonOperator)
	f.printKeyword(" some ")
	f.visitExpressionSubquery(e.Subquery)
}

func (f *Formatter) visitAnyLogicalOperatorExpression(e *ast.ExprAnyLogicalOperator) {
	f.visitExpression(e.ScalarExpression)
	f.visitComparisonOperatorType(e.ComparisonOperator)
	f.printKeyword(" any ")
	f.visitExpressionSubquery(e.Subquery)
}
