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

	input := "select *,\n hello,\n 'yes',\n [yessir],\n @nosir, [superdb].world.* FROM testtable where LastPrice < 10.0"

	test(t, expected, input)
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

	input := "select hello, sum(price) FROM testtable"

	test(t, expected, input)
}

func TestParseOrderByClause(t *testing.T) {
	select_statement := ast.SelectStatement{
		SelectBody: &ast.SelectBody{
			SelectItems: []ast.Expression{
				&ast.ExprIdentifier{Value: "Stock"},
				&ast.ExprIdentifier{Value: "PercentChange"},
			},
			TableObject: &ast.ExprIdentifier{Value: "MarketData"},
			OrderByClause: &ast.OrderByClause{
				Expressions: []ast.OrderByArg{
					{Column: &ast.ExprIdentifier{Value: "InsertDate"}, Type: ast.OBDesc},
					{Column: &ast.ExprIdentifier{Value: "InsertTime"}, Type: ast.OBAsc},
					{Column: &ast.ExprIdentifier{Value: "Stock"}},
				},
				OffsetFetch: &ast.OffsetFetchClause{
					Offset: ast.OffsetArg{
						Value:     &ast.ExprNumberLiteral{Value: "4"},
						RowOrRows: ast.RRRow,
					},
					Fetch: &ast.FetchArg{
						Value:       &ast.ExprNumberLiteral{Value: "20"},
						NextOrFirst: ast.NFFirst,
						RowOrRows:   ast.RRRows,
					},
				},
			},
		}}
	expected := ast.Query{Statements: []ast.Statement{&select_statement}}

	input := "select Stock, PercentChange FROM MarketData order by InsertDate Desc, InsertTime asc"
	input += ", Stock offset 4 row fetch first 20 rows only"

	test(t, expected, input)
}

func TestParseSubqueryCall(t *testing.T) {
	select_statement := ast.SelectStatement{
		SelectBody: &ast.SelectBody{
			SelectItems: []ast.Expression{
				&ast.ExprIdentifier{Value: "hello"},
				&ast.ExprSubquery{
					Top: &ast.TopArg{
						Percent:  true,
						Quantity: ast.ExprNumberLiteral{Value: "20"},
					},
					SelectItems: []ast.Expression{
						&ast.ExprIdentifier{Value: "yesirr"},
					},
					TableObject: &ast.ExprIdentifier{Value: "bruh"},
					WhereClause: &ast.ExprComparisonOperator{
						Left: &ast.ExprIdentifier{
							Value: "LastPrice",
						},
						Operator: ast.ComparisonOpLess,
						Right:    &ast.ExprNumberLiteral{Value: "10.0"}},
					OrderByClause: &ast.OrderByClause{
						Expressions: []ast.OrderByArg{
							{
								Column: ast.ExprIdentifier{Value: "LastPrice"},
								Type:   ast.OBDesc,
							},
						},
					},
				}},
			TableObject: &ast.ExprIdentifier{Value: "testtable"},
		},
	}
	expected := ast.Query{Statements: []ast.Statement{&select_statement}}

	input := "select hello,  (select  top 20 percent yesirr from bruh where LastPrice < 10.0 order by LastPrice desc) FROM testtable"

	test(t, expected, input)
}

func TestParseSomeLogicalOperators(t *testing.T) {
	select_statement := ast.SelectStatement{
		SelectBody: &ast.SelectBody{
			SelectItems: []ast.Expression{
				&ast.ExprIdentifier{Value: "Stock"},
				&ast.ExprWithAlias{
					Expression: &ast.ExprUnaryOperator{
						Operator: ast.UnaryOpMinus,
						Right:    &ast.ExprIdentifier{Value: "LastPrice"},
					},
					Alias: &ast.ExprStringLiteral{Value: "NegativeLastPrice"},
				},
				&ast.ExprIdentifier{Value: "LastPrice"},
			},
			TableObject: &ast.ExprIdentifier{Value: "MarketData"},
			WhereClause: &ast.ExprOrLogicalOperator{
				Left: &ast.ExprAndLogicalOperator{
					Left: &ast.ExprComparisonOperator{
						Left: &ast.ExprQuotedIdentifier{
							Value: "LastPrice",
						},
						Operator: ast.ComparisonOpLess,
						Right:    &ast.ExprNumberLiteral{Value: "10.0"}},
					Right: &ast.ExprInLogicalOperator{
						TestExpression: &ast.ExprIdentifier{Value: "Stock"},
						Not:            true,
						Expressions: []ast.Expression{
							&ast.ExprStringLiteral{Value: "AAL"},
							&ast.ExprStringLiteral{Value: "AMZN"},
							&ast.ExprStringLiteral{Value: "GOOGL"},
							&ast.ExprStringLiteral{Value: "ZM"},
						},
					},
				},
				Right: &ast.ExprBetweenLogicalOperator{
					TestExpression: &ast.ExprIdentifier{Value: "PercentChange"},
					Begin:          &ast.ExprNumberLiteral{Value: "1"},
					End:            &ast.ExprNumberLiteral{Value: "4"},
				},
			},
		},
	}
	expected := ast.Query{Statements: []ast.Statement{&select_statement}}

	input := "select Stock, -LastPrice 'NegativeLastPrice', LastPrice FROM MarketData"
	input += " where [LastPrice] < 10.0 and Stock nOT in ('AAL', 'AMZN', 'GOOGL', 'ZM')"
	input += "\n or PercentChange Between 1 and 4"

	test(t, expected, input)
}

func TestParseSelectItemWithAlias(t *testing.T) {
	select_statement := ast.SelectStatement{
		SelectBody: &ast.SelectBody{
			SelectItems: []ast.Expression{
				&ast.ExprIdentifier{Value: "hello"},
				&ast.ExprWithAlias{
					Expression: &ast.ExprIdentifier{Value: "potate"},
					Alias:      &ast.ExprStringLiteral{Value: "Potate"},
				},
				&ast.ExprSubquery{
					SelectItems: []ast.Expression{
						&ast.ExprWithAlias{
							Expression:     &ast.ExprIdentifier{Value: "dt"},
							AsTokenPresent: true,
							Alias:          &ast.ExprQuotedIdentifier{Value: "Datetime"},
						},
					},
					TableObject: &ast.ExprIdentifier{Value: "bruh"},
				}},
			TableObject: &ast.ExprIdentifier{Value: "testtable"},
		},
	}
	expected := ast.Query{Statements: []ast.Statement{&select_statement}}

	input := "select hello, potate 'Potate', (select dt as [Datetime] from bruh) FROM testtable"

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
					Alias:      &ast.ExprIdentifier{Value: "Potate"},
				},
			},
			TableObject: &ast.ExprIdentifier{Value: "testtable"},
		},
	}
	expected := ast.Query{Statements: []ast.Statement{&select_statement}}

	input := "select distinct top 44 percent hello, potate Potate FROM testtable"

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
