package parser

import (
	"SequelGo/internal/ast"
	"SequelGo/internal/lexer"
	"testing"
)

func TestParseBasicSelectQuery(t *testing.T) {
	select_statement := ast.SelectStatement{
		SelectBody: &ast.SelectBody{
			SelectItems: &[]ast.Expression{
				&ast.ExprStar{},
				&ast.ExprIdentifier{Value: "hello"},
				&ast.ExprStringLiteral{Value: "yes"},
				&ast.ExprQuotedIdentifier{Value: "yessir"},
				&ast.ExprLocalVariable{Value: "nosir"},
				&ast.ExprCompoundIdentifier{Identifiers: []ast.Expression{
					&ast.ExprQuotedIdentifier{Value: "superdb"},
					&ast.ExprIdentifier{Value: "world"},
					&ast.ExprStar{}}},
			},
			TableObject: &ast.ExprIdentifier{Value: "testtable"},
		},
	}
	expected := ast.Query{Statements: []ast.Statement{&select_statement}}

	l := lexer.NewLexer("select *, hello, 'yes', [yessir],  @nosir, [superdb].world.* FROM testtable")
	p := NewParser(l)
	query := p.Parse()

	if len(query.Statements) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(query.Statements))
	}
	for i, stmt := range query.Statements {
		if stmt.TokenLiteral() != expected.Statements[i].TokenLiteral() {
			t.Fatalf("expected %s, got %s", expected.Statements[i].TokenLiteral(), stmt.TokenLiteral())
		}
	}
}
