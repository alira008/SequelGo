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
	}
}

func (f *Formatter) visitSelectQuery(selectStatement *ast.SelectStatement) {
	f.printKeyword("select ")
	if selectStatement.SelectBody.Distinct {
		f.printKeyword("distinct ")
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

	f.printKeyword("top ")
	f.visitExpression(selectTopArg.Quantity)
	f.printSpace()
	if selectTopArg.Percent {
		f.printKeyword("percent ")
	}
	if selectTopArg.WithTies {
		f.printKeyword("with ties ")
	}
}

func (f *Formatter) visitSelectItems(items []ast.Expression) {
	for i, e := range items {
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
		f.printSpace()
		switch e.Type {
		case ast.OBNone:
			break
		case ast.OBAsc:
			f.printKeyword("asc ")
			break
		case ast.OBDesc:
			f.printKeyword("desc ")
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
	f.printKeyword("offset ")
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
	f.formattedQuery += e.Value
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
