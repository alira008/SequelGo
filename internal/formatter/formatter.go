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

func (f *Formatter) Format(input string) string {
	l := lexer.NewLexer(input)
	p := parser.NewParser(f.logger, l)
	query := p.Parse()
	if len(p.Errors()) > 0 {
		return ""
	}

	for _, s := range query.Statements {
		f.walkQuery(s)

	}

	return ""
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
	f.visitSelectTopArg(selectStatement.SelectBody.Top)
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
	// f.visitTableSource(selectTableArg.Table)
    if len(selectTableArg.Joins) > 0 {
        f.printNewLine()
    }
    for _, join := range selectTableArg.Joins {
        _ = join
        // f.visitTableJoin(join)
    }
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
