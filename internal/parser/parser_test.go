package parser

import (
	"SequelGo/internal/ast"
	"SequelGo/internal/lexer"
	"strings"
	"testing"

	"go.uber.org/zap"
)

func TestParseBasicSelectQuery(t *testing.T) {
	select_statement := ast.SelectStatement{
		SelectBody: &ast.SelectBody{
			SelectKeyword: ast.Keyword{Type: ast.KSelect},
			SelectItems: ast.SelectItems{
				Items: []ast.Expression{
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
			},
			Table: &ast.TableArg{
				FromKeyword: ast.Keyword{Type: ast.KFrom},
				Table: &ast.TableSource{
					Type:   ast.TSTTable,
					Source: &ast.ExprIdentifier{Value: "testtable"},
				},
			},
			WhereClause: &ast.WhereClause{
				WhereKeyword: ast.Keyword{Type: ast.KWhere},
				Clause: &ast.ExprComparisonOperator{
					Left: &ast.ExprIdentifier{
						Value: "LastPrice",
					},
					Operator: ast.ComparisonOpLess,
					Right:    &ast.ExprNumberLiteral{Value: "10.0"}},
			},
		},
	}
	expected := ast.Query{Statements: []ast.Statement{&select_statement}}

	input := "select *,\n hello,\n 'yes',\n [yessir],\n @nosir, [superdb].world.* FROM testtable where LastPrice < 10.0"

	test(t, expected, input)
}

func TestParseBasicSelectQueryWithCte(t *testing.T) {
	select_statement := ast.SelectStatement{
		WithKeyword: &ast.Keyword{Type: ast.KWith},
		CTE: &[]ast.CommonTableExpression{
			{
				Name: "testctename",
				Columns: &ast.ExprExpressionList{
					List: []ast.Expression{
						&ast.ExprIdentifier{Value: "LastPrice"},
						&ast.ExprIdentifier{Value: "PercentChange"},
					},
				},
				AsKeyword: ast.Keyword{Type: ast.KAs},
				Query: ast.SelectBody{
					SelectKeyword: ast.Keyword{Type: ast.KSelect},
					SelectItems: ast.SelectItems{
						Items: []ast.Expression{
							&ast.ExprStar{},
							&ast.ExprIdentifier{Value: "hello"},
							&ast.ExprStringLiteral{Value: "yes"},
						},
					},
					Table: &ast.TableArg{
						FromKeyword: ast.Keyword{Type: ast.KFrom},
						Table: &ast.TableSource{
							Type:   ast.TSTTable,
							Source: &ast.ExprIdentifier{Value: "testtable"},
						},
					},
				},
			},
			{
				Name:      "testctenamedos",
				AsKeyword: ast.Keyword{Type: ast.KAs},
				Query: ast.SelectBody{
					SelectKeyword: ast.Keyword{Type: ast.KSelect},
					SelectItems: ast.SelectItems{
						Items: []ast.Expression{
							&ast.ExprIdentifier{Value: "FirstName"},
							&ast.ExprIdentifier{Value: "LastName"},
						},
					},
					Table: &ast.TableArg{
						FromKeyword: ast.Keyword{Type: ast.KFrom},
						Table: &ast.TableSource{
							Type:   ast.TSTTable,
							Source: &ast.ExprIdentifier{Value: "Users"},
						},
					},
				},
			},
		},
		SelectBody: &ast.SelectBody{
			SelectKeyword: ast.Keyword{Type: ast.KSelect},
			SelectItems: ast.SelectItems{
				Items: []ast.Expression{
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
			},
			Table: &ast.TableArg{
				FromKeyword: ast.Keyword{Type: ast.KFrom},
				Table: &ast.TableSource{
					Type:   ast.TSTTable,
					Source: &ast.ExprIdentifier{Value: "testtable"},
				},
			},
			WhereClause: &ast.WhereClause{
				WhereKeyword: ast.Keyword{Type: ast.KWhere},
				Clause: &ast.ExprComparisonOperator{
					Left: &ast.ExprIdentifier{
						Value: "LastPrice",
					},
					Operator: ast.ComparisonOpLess,
					Right:    &ast.ExprNumberLiteral{Value: "10.0"}},
			},
		},
	}
	expected := ast.Query{Statements: []ast.Statement{&select_statement}}

	input := "with testctename (LastPrice, PercentChange) as (select *, hello, 'yes' FROM testtable), testctenamedos as (select FirstName, LastName from Users) select *,\n hello,\n 'yes',\n [yessir],\n @nosir, [superdb].world.* FROM testtable where LastPrice < 10.0"

	test(t, expected, input)
}

func TestParseBasicSelectQueryWithCast(t *testing.T) {
	float := uint32(24)
	floatPrecision := &float
	select_statement := ast.SelectStatement{
		SelectBody: &ast.SelectBody{
			SelectKeyword: ast.Keyword{Type: ast.KSelect},
			SelectItems: ast.SelectItems{
				Items: []ast.Expression{
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
			},
			Table: &ast.TableArg{
				FromKeyword: ast.Keyword{Type: ast.KFrom},
				Table: &ast.TableSource{
					Type:   ast.TSTTable,
					Source: &ast.ExprIdentifier{Value: "testtable"},
				},
			},
			WhereClause: &ast.WhereClause{
				WhereKeyword: ast.Keyword{Type: ast.KWhere},
				Clause: &ast.ExprComparisonOperator{
					Left: &ast.ExprIdentifier{
						Value: "LastPrice",
					},
					Operator: ast.ComparisonOpLess,
					Right: &ast.ExprCast{
						CastKeyword: ast.Keyword{Type: ast.KCast},
						Expression:  &ast.ExprStringLiteral{Value: "10"},
						AsKeyword:   ast.Keyword{Type: ast.KAs},
						DataType: ast.DataType{
							Kind:           ast.DTFloat,
							FloatPrecision: floatPrecision,
						},
					},
				},
			},
		},
	}
	expected := ast.Query{Statements: []ast.Statement{&select_statement}}

	input := "select *,\n hello,\n 'yes',\n [yessir],\n @nosir, [superdb].world.* FROM"
	input += " testtable where LastPrice < cast('10' as float(24))"

	test(t, expected, input)
}

func TestParseBasicSelectQueryWithJoin(t *testing.T) {
	select_statement := ast.SelectStatement{
		SelectBody: &ast.SelectBody{
			SelectKeyword: ast.Keyword{Type: ast.KSelect},
			SelectItems: ast.SelectItems{
				Items: []ast.Expression{
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
			},
			Table: &ast.TableArg{
				FromKeyword: ast.Keyword{Type: ast.KFrom},
				Table: &ast.TableSource{
					Type: ast.TSTTable,
					Source: &ast.ExprWithAlias{
						Expression: &ast.ExprIdentifier{Value: "testtable"},
						Alias:      &ast.ExprIdentifier{Value: "t"},
					},
				},
				Joins: []ast.Join{
					{
						JoinTypeKeyword: []ast.Keyword{{Type: ast.KInner}, {Type: ast.KJoin}},
						Type:            ast.JTInner,
						Table: &ast.TableSource{
							Type: ast.TSTTable,
							Source: &ast.ExprWithAlias{
								Expression: &ast.ExprIdentifier{Value: "testtable2"},
								Alias:      &ast.ExprIdentifier{Value: "t2"},
							},
						},
						OnKeyword: &ast.Keyword{Type: ast.KOn},
						Condition: &ast.ExprComparisonOperator{
							Left: &ast.ExprCompoundIdentifier{
								Identifiers: []ast.Expression{
									&ast.ExprIdentifier{Value: "t"},
									&ast.ExprIdentifier{Value: "InsertDate"},
								},
							},
							Right: &ast.ExprCompoundIdentifier{
								Identifiers: []ast.Expression{
									&ast.ExprIdentifier{Value: "t2"},
									&ast.ExprIdentifier{Value: "InsertDate"},
								},
							},
							Operator: ast.ComparisonOpEqual,
						},
					},
				},
			},
			WhereClause: &ast.WhereClause{
				WhereKeyword: ast.Keyword{Type: ast.KWhere},
				Clause: &ast.ExprComparisonOperator{
					Left: &ast.ExprIdentifier{
						Value: "LastPrice",
					},
					Operator: ast.ComparisonOpLess,
					Right:    &ast.ExprNumberLiteral{Value: "10.0"}},
			},
		},
	}
	expected := ast.Query{Statements: []ast.Statement{&select_statement}}

	input := "select *,\n hello,\n 'yes',\n [yessir],\n @nosir, [superdb].world.* FROM testtable t"
	input += " inner join testtable2 t2 ON t.InsertDate = t2.InsertDate where LastPrice < 10.0"

	test(t, expected, input)
}

func TestParseBuiltinFunctionCall(t *testing.T) {
	select_statement := ast.SelectStatement{
		SelectBody: &ast.SelectBody{
			SelectKeyword: ast.Keyword{Type: ast.KSelect},
			SelectItems: ast.SelectItems{
				Items: []ast.Expression{
					&ast.ExprIdentifier{Value: "hello"},
					&ast.ExprFunctionCall{
						Name: &ast.ExprFunction{
							Type: ast.FuncSum,
							Name: &ast.ExprIdentifier{Value: "sum"},
						},
						Args: []ast.Expression{
							&ast.ExprIdentifier{Value: "price"},
						},
						OverClause: &ast.FunctionOverClause{
							OverKeyword: ast.Keyword{Type: ast.KOver},
							PartitionByKeyword: &[2]ast.Keyword{
								{Type: ast.KPartition},
								{Type: ast.KBy},
							},
							PartitionByClause: []ast.Expression{
								&ast.ExprIdentifier{Value: "InsertDate"},
								&ast.ExprIdentifier{Value: "Stock"},
							},
							OrderByKeyword: &[2]ast.Keyword{
								{Type: ast.KOrder},
								{Type: ast.KBy},
							},
							OrderByClause: []ast.OrderByArg{
								{
									Column:       &ast.ExprIdentifier{Value: "InsertTime"},
									OrderKeyword: &ast.Keyword{Type: ast.KAsc},
									Type:         ast.OBAsc,
								},
							},
							WindowFrameClause: &ast.WindowFrameClause{
								RowsOrRangeKeyword: ast.Keyword{Type: ast.KRows},
								RowsOrRange:        ast.RRTRows,
								BetweenKeyword:     &ast.Keyword{Type: ast.KBetween},
								Start: &ast.WindowFrameBound{
									BoundKeyword: []ast.Keyword{
										{Type: ast.KPreceding},
									},
									Type:       ast.WFBTPreceding,
									Expression: &ast.ExprNumberLiteral{Value: "10"},
								},
								AndKeyword: &ast.Keyword{Type: ast.KAnd},
								End: &ast.WindowFrameBound{
									BoundKeyword: []ast.Keyword{
										{Type: ast.KCurrent},
										{Type: ast.KRow},
									},
									Type: ast.WFBTCurrentRow,
								},
							},
						},
					},
				},
			},
			Table: &ast.TableArg{
				FromKeyword: ast.Keyword{Type: ast.KFrom},
				Table: &ast.TableSource{
					Type:   ast.TSTTable,
					Source: &ast.ExprIdentifier{Value: "testtable"},
				},
			},
		}}
	expected := ast.Query{Statements: []ast.Statement{&select_statement}}

	input := "select hello, sum(price) over(Partition by InsertDate, Stock Order by InsertTime asc rows between 10 preceding  and current row) FROM testtable"

	test(t, expected, input)
}

func TestParseOrderByClause(t *testing.T) {
	select_statement := ast.SelectStatement{
		SelectBody: &ast.SelectBody{
			SelectKeyword: ast.Keyword{Type: ast.KSelect},
			SelectItems: ast.SelectItems{
				Items: []ast.Expression{
					&ast.ExprIdentifier{Value: "Stock"},
					&ast.ExprIdentifier{Value: "PercentChange"},
				},
			},
			Table: &ast.TableArg{
				FromKeyword: ast.Keyword{Type: ast.KFrom},
				Table: &ast.TableSource{
					Type:   ast.TSTTable,
					Source: &ast.ExprIdentifier{Value: "MarketData"},
				},
			},
			OrderByClause: &ast.OrderByClause{
				OrderByKeyword: [2]ast.Keyword{
					{Type: ast.KOrder},
					{Type: ast.KBy},
				},
				Expressions: []ast.OrderByArg{
					{
						Column:       &ast.ExprIdentifier{Value: "InsertDate"},
						Type:         ast.OBDesc,
						OrderKeyword: &ast.Keyword{Type: ast.KDesc},
					},
					{
						Column:       &ast.ExprIdentifier{Value: "InsertTime"},
						Type:         ast.OBAsc,
						OrderKeyword: &ast.Keyword{Type: ast.KAsc},
					},
					{
						Column: &ast.ExprIdentifier{Value: "Stock"},
					},
				},
				OffsetFetch: &ast.OffsetFetchClause{
					Offset: ast.OffsetArg{
						OffsetKeyword:    ast.Keyword{Type: ast.KOffset},
						Value:            &ast.ExprNumberLiteral{Value: "4"},
						RowOrRows:        ast.RRRow,
						RowOrRowsKeyword: ast.Keyword{Type: ast.KRow},
					},
					Fetch: &ast.FetchArg{
						FetchKeyword:       ast.Keyword{Type: ast.KFetch},
						Value:              &ast.ExprNumberLiteral{Value: "20"},
						NextOrFirst:        ast.NFFirst,
						RowOrRows:          ast.RRRows,
						NextOrFirstKeyword: ast.Keyword{Type: ast.KFirst},
						RowOrRowsKeyword:   ast.Keyword{Type: ast.KRows},
						OnlyKeyword:        ast.Keyword{Type: ast.KOnly},
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
			SelectKeyword: ast.Keyword{Type: ast.KSelect},
			SelectItems: ast.SelectItems{
				Items: []ast.Expression{
					&ast.ExprIdentifier{Value: "hello"},
					&ast.ExprWithAlias{
						Expression: &ast.ExprSubquery{
							SelectBody: ast.SelectBody{
								SelectKeyword: ast.Keyword{Type: ast.KSelect},
								Top: &ast.TopArg{
									TopKeyword:     ast.Keyword{Type: ast.KTop},
									PercentKeyword: &ast.Keyword{Type: ast.KPercent},
									Quantity:       &ast.ExprNumberLiteral{Value: "20"},
								},
								SelectItems: ast.SelectItems{
									Items: []ast.Expression{
										&ast.ExprIdentifier{Value: "yesirr"},
									},
								},
								Table: &ast.TableArg{
									FromKeyword: ast.Keyword{Type: ast.KFrom},
									Table: &ast.TableSource{
										Type:   ast.TSTTable,
										Source: &ast.ExprIdentifier{Value: "bruh"},
									},
								},
								WhereClause: &ast.WhereClause{
									WhereKeyword: ast.Keyword{Type: ast.KWhere},
									Clause: &ast.ExprComparisonOperator{
										Left: &ast.ExprIdentifier{
											Value: "LastPrice",
										},
										Operator: ast.ComparisonOpLess,
										Right:    &ast.ExprNumberLiteral{Value: "10.0"}},
								},
								OrderByClause: &ast.OrderByClause{
									OrderByKeyword: [2]ast.Keyword{
										{Type: ast.KOrder},
										{Type: ast.KBy},
									},
									Expressions: []ast.OrderByArg{
										{
											Column:       &ast.ExprIdentifier{Value: "LastPrice"},
											OrderKeyword: &ast.Keyword{Type: ast.KDesc},
											Type:         ast.OBDesc,
										},
									},
								},
							},
						},
						Alias: &ast.ExprIdentifier{Value: "NetScore"},
					},
				},
			},
			Table: &ast.TableArg{
				FromKeyword: ast.Keyword{Type: ast.KFrom},
				Table: &ast.TableSource{
					Type:   ast.TSTTable,
					Source: &ast.ExprIdentifier{Value: "testtable"},
				},
			},
		},
	}
	expected := ast.Query{Statements: []ast.Statement{&select_statement}}

	input := "select hello,  (select  top 20 percent yesirr from bruh where LastPrice < 10.0"
	input += " order by LastPrice desc) NetScore FROM testtable"

	test(t, expected, input)
}

func TestParseSomeLogicalOperators(t *testing.T) {
	select_statement := ast.SelectStatement{
		SelectBody: &ast.SelectBody{
			SelectKeyword: ast.Keyword{Type: ast.KSelect},
			SelectItems: ast.SelectItems{
				Items: []ast.Expression{
					&ast.ExprIdentifier{Value: "Stock"},
					&ast.ExprIdentifier{Value: "LastPrice"},
				},
			},
			Table: &ast.TableArg{
				FromKeyword: ast.Keyword{Type: ast.KFrom},
				Table: &ast.TableSource{
					Type:   ast.TSTTable,
					Source: &ast.ExprIdentifier{Value: "MarketData"},
				},
			},
			WhereClause: &ast.WhereClause{
				WhereKeyword: ast.Keyword{Type: ast.KWhere},
				Clause: &ast.ExprOrLogicalOperator{
					OrKeyword: ast.Keyword{Type: ast.KOr},
					Left: &ast.ExprAndLogicalOperator{
						AndKeyword: ast.Keyword{Type: ast.KAnd},
						Left: &ast.ExprComparisonOperator{
							Left: &ast.ExprQuotedIdentifier{
								Value: "LastPrice",
							},
							Operator: ast.ComparisonOpLess,
							Right:    &ast.ExprNumberLiteral{Value: "10.0"}},
						Right: &ast.ExprInLogicalOperator{
							InKeyword:      ast.Keyword{Type: ast.KIn},
							TestExpression: &ast.ExprIdentifier{Value: "Stock"},
							NotKeyword:     &ast.Keyword{Type: ast.KNot},
							Expressions: []ast.Expression{
								&ast.ExprStringLiteral{Value: "AAL"},
								&ast.ExprStringLiteral{Value: "AMZN"},
								&ast.ExprStringLiteral{Value: "GOOGL"},
								&ast.ExprStringLiteral{Value: "ZM"},
							},
						},
					},
					Right: &ast.ExprBetweenLogicalOperator{
						BetweenKeyword: ast.Keyword{Type: ast.KBetween},
						TestExpression: &ast.ExprIdentifier{Value: "PercentChange"},
						Begin:          &ast.ExprNumberLiteral{Value: "1"},
						AndKeyword:     ast.Keyword{Type: ast.KAnd},
						End:            &ast.ExprNumberLiteral{Value: "4"},
					},
				},
			},
		},
	}
	expected := ast.Query{Statements: []ast.Statement{&select_statement}}

	input := "select Stock,  LastPrice FROM MarketData"
	input += " where [LastPrice] < 10.0 and Stock nOT in ('AAL', 'AMZN', 'GOOGL', 'ZM')"
	input += "\n or PercentChange Between 1 and 4"

	test(t, expected, input)
}

func TestParseSelectItemWithAlias(t *testing.T) {
	select_statement := ast.SelectStatement{
		SelectBody: &ast.SelectBody{
			SelectKeyword: ast.Keyword{Type: ast.KSelect},
			SelectItems: ast.SelectItems{
				Items: []ast.Expression{
					&ast.ExprIdentifier{Value: "hello"},
					&ast.ExprWithAlias{
						Expression: &ast.ExprIdentifier{Value: "potate"},
						Alias:      &ast.ExprStringLiteral{Value: "Potate"},
					},
					&ast.ExprSubquery{
						SelectBody: ast.SelectBody{
							SelectKeyword: ast.Keyword{Type: ast.KSelect},
							SelectItems: ast.SelectItems{
								Items: []ast.Expression{
									&ast.ExprWithAlias{
										Expression: &ast.ExprIdentifier{Value: "dt"},
										AsKeyword:  &ast.Keyword{Type: ast.KAs},
										Alias:      &ast.ExprQuotedIdentifier{Value: "Datetime"},
									},
								},
							},
							Table: &ast.TableArg{
								FromKeyword: ast.Keyword{Type: ast.KFrom},
								Table: &ast.TableSource{
									Type:   ast.TSTTable,
									Source: &ast.ExprIdentifier{Value: "bruh"},
								},
							},
						},
					}},
			},
			Table: &ast.TableArg{
				FromKeyword: ast.Keyword{Type: ast.KFrom},
				Table: &ast.TableSource{
					Type:   ast.TSTTable,
					Source: &ast.ExprIdentifier{Value: "testtable"},
				},
			},
		},
	}
	expected := ast.Query{Statements: []ast.Statement{&select_statement}}

	input := "select hello, potate 'Potate', (select dt as [Datetime] from bruh) FROM testtable"

	test(t, expected, input)
}

func TestDistinctTopArg(t *testing.T) {
	select_statement := ast.SelectStatement{
		SelectBody: &ast.SelectBody{
			SelectKeyword:   ast.Keyword{Type: ast.KSelect},
			DistinctKeyword: &ast.Keyword{Type: ast.KDistinct},
			Top: &ast.TopArg{
				TopKeyword:     ast.Keyword{Type: ast.KTop},
				PercentKeyword: &ast.Keyword{Type: ast.KPercent},
				Quantity: &ast.ExprNumberLiteral{
					Value: "44",
				},
			},
			SelectItems: ast.SelectItems{
				Items: []ast.Expression{
					&ast.ExprIdentifier{Value: "hello"},
					&ast.ExprWithAlias{
						Expression: &ast.ExprIdentifier{Value: "potate"},
						Alias:      &ast.ExprIdentifier{Value: "Potate"},
					},
				},
			},
			Table: &ast.TableArg{
				FromKeyword: ast.Keyword{Type: ast.KFrom},
				Table: &ast.TableSource{
					Type:   ast.TSTTable,
					Source: &ast.ExprIdentifier{Value: "testtable"},
				},
			},
		},
	}
	expected := ast.Query{Statements: []ast.Statement{&select_statement}}

	input := "select distinct top 44 percent hello, potate Potate FROM testtable -- hello lmao"

	test(t, expected, input)
}

func test(t *testing.T, expected ast.Query, input string) {
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()
	sugar := logger.Sugar()
	l := lexer.NewLexer(input)
	p := NewParser(sugar, l)
	query := p.Parse()

	if len(query.Statements) != 1 {
		t.Fatalf("expected 1 statement, got %d\n %s", len(query.Statements), strings.Join(p.Errors(), "\n"))
	}
	for i, stmt := range query.Statements {
		if stmt.TokenLiteral() != expected.Statements[i].TokenLiteral() {
			t.Fatalf("expected %s, got %s", expected.Statements[i].TokenLiteral(), stmt.TokenLiteral())
		}
	}
}
