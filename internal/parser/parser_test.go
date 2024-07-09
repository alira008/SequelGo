package parser

import (
	"SequelGo/internal/ast"
	"SequelGo/internal/lexer"
	"testing"
)

func TestParseBasicSelectQuery(t *testing.T) {
	select_statement := ast.SelectStatement{
		SelectBody: &ast.SelectBody{
			SelectItems: []ast.Expression{
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
			WhereClause: &ast.ExprComparisonOperator{
				Left: &ast.ExprIdentifier{
					Value: "LastPrice",
				},
				Operator: ast.ComparisonOpLess,
				Right:    &ast.ExprNumberLiteral{Value: "10.0"}},
		},
	}
	expected := ast.Query{Statements: []ast.Statement{&select_statement}}

	l := lexer.NewLexer("select *,\n hello,\n 'yes',\n [yessir],\n @nosir, [superdb].world.* FROM testtable where LastPrice < 10.0")
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

func TestParseBuiltinFunctionCall(t *testing.T) {
	select_statement := ast.SelectStatement{
		SelectBody: &ast.SelectBody{
			SelectItems: []ast.Expression{
				&ast.ExprIdentifier{Value: "hello"},
				&ast.ExprFunctionCall{Name: &ast.ExprFunction{Type: ast.FuncSum,
					Name: &ast.ExprIdentifier{Value: "sum"}},
					Args: []ast.Expression{&ast.ExprIdentifier{Value: "price"}}}},
			TableObject: &ast.ExprIdentifier{Value: "testtable"}}}
	expected := ast.Query{Statements: []ast.Statement{&select_statement}}

	l := lexer.NewLexer("select hello, sum(price) FROM testtable")
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

func TestParseSubqueryCall(t *testing.T) {
	select_statement := ast.SelectStatement{
		SelectBody: &ast.SelectBody{
			SelectItems: []ast.Expression{
				&ast.ExprIdentifier{Value: "hello"},
				&ast.ExprSubquery{
					SelectItem:  &ast.ExprIdentifier{Value: "yesirr"},
					TableObject: &ast.ExprIdentifier{Value: "bruh"},
					WhereClause: &ast.ExprComparisonOperator{
						Left: &ast.ExprIdentifier{
							Value: "LastPrice",
						},
						Operator: ast.ComparisonOpLess,
						Right:    &ast.ExprNumberLiteral{Value: "10.0"}},
				}},
			TableObject: &ast.ExprIdentifier{Value: "testtable"},
		},
	}
	expected := ast.Query{Statements: []ast.Statement{&select_statement}}

	l := lexer.NewLexer("select hello,  (select yesirr from bruh where LastPrice < 10.0) FROM testtable")
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

func TestParseSelectItemWithAlias(t *testing.T) {
	select_statement := ast.SelectStatement{
		SelectBody: &ast.SelectBody{
			SelectItems: []ast.Expression{
				&ast.ExprIdentifier{Value: "hello"},
				&ast.ExprWithAlias{
					Expression: &ast.ExprIdentifier{Value: "potate"},
					Alias:      "Potate",
				},
				&ast.ExprSubquery{
					SelectItem: &ast.ExprWithAlias{
						Expression:     &ast.ExprIdentifier{Value: "dt"},
						AsTokenPresent: true,
						Alias:          "Datetime",
					},
					TableObject: &ast.ExprIdentifier{Value: "bruh"},
				}},
			TableObject: &ast.ExprIdentifier{Value: "testtable"},
		},
	}
	expected := ast.Query{Statements: []ast.Statement{&select_statement}}

	input := "select hello, potate 'Potate', (select dt as 'Datetime' from bruh) FROM testtable"

	test(t, expected, input)
}

func TestDistinctTopArg(t *testing.T) {
	select_statement := ast.SelectStatement{
		SelectBody: &ast.SelectBody{
			Distinct: true,
			Top: &ast.TopArg{
				Percent: true,
				Quantity: &ast.ExprNumberLiteral{
					Value: "44",
				},
			},
			SelectItems: []ast.Expression{
				&ast.ExprIdentifier{Value: "hello"},
				&ast.ExprWithAlias{
					Expression: &ast.ExprIdentifier{Value: "potate"},
					Alias:      "Potate",
				},
			},
			TableObject: &ast.ExprIdentifier{Value: "testtable"},
		},
	}
	expected := ast.Query{Statements: []ast.Statement{&select_statement}}

	input := "select distinct top 44 percent hello, potate 'Potate' FROM testtable"

	test(t, expected, input)
}

func test(t *testing.T, expected ast.Query, input string) {
	l := lexer.NewLexer(input)
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
