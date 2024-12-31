package parser

import (
	"SequelGo/internal/ast"
	"SequelGo/internal/lexer"

	"fmt"
	"strconv"
)

func (p *Parser) parseStatement() (ast.Statement, error) {
	switch p.peekToken.Type {
	case lexer.TSelect:
		startPosition := p.peekToken.Start
		body, err := p.parseSelectBody()
		if err != nil {
			return nil, err
		}

		return &ast.SelectStatement{
			Span:       ast.NewSpanFromLexerPosition(startPosition, body.EndPosition),
			SelectBody: &body,
		}, nil
	case lexer.TWith:
		select_statement, err := p.parseSelectStatement()

		if err != nil {
			return nil, err
		}
		return select_statement, nil
	default:
		return nil, nil
		// return nil, fmt.Errorf("unknown statement type %s", p.currentToken.Value)
	}
}

func (p *Parser) parseSelectStatement() (*ast.SelectStatement, error) {
	p.logger.Debugln("parsing select statement with cte")
	startPositionSelectStatement := p.peekToken.Start
	withKeyword, err := p.consumeKeyword(lexer.TWith)
	if err != nil {
		return nil, err
	}
	ctes := []ast.CommonTableExpression{}
	for !p.peekTokenIs(lexer.TSelect) {
		if len(ctes) > 0 {
			if _, err := p.consumeToken(lexer.TComma); err != nil {
				return nil, err
			}
		}
		startPosition := p.peekToken.Start

		// check for expression name
		token, err := p.consumeTokenAny([]lexer.TokenType{lexer.TIdentifier, lexer.TQuotedIdentifier})
		if err != nil {
			return nil, err
		}

		cteName := token.Value
		var exprList *ast.ExprExpressionList

		if token := p.maybeToken(lexer.TLeftParen); token != nil {
			// parse column list
			expressionList, err := p.parseExpressionList()
			if err != nil {
				return nil, err
			}
			exprList = &expressionList

			// go to the right paren
			if _, err = p.consumeToken(lexer.TRightParen); err != nil {
				return nil, err
			}
		}

		asKeyword, err := p.consumeKeyword(lexer.TAs)
		if err != nil {
			return nil, err
		}

		if _, err := p.consumeToken(lexer.TLeftParen); err != nil {
			return nil, err
		}

		selectBody, err := p.parseSelectBody()
		if err != nil {
			return nil, err
		}

		if selectBody.OrderByClause != nil && len(selectBody.OrderByClause.Expressions) > 0 && selectBody.Top == nil {
			return nil, p.peekErrorString("Order by is not allowed in cte query unless top clause is specified")
		}
		rightParen, err := p.consumeToken(lexer.TRightParen)
		if err != nil {
			return nil, err
		}

		cte := ast.CommonTableExpression{
			Span:      ast.NewSpanFromLexerPosition(startPosition, rightParen.End),
			Name:      cteName,
			Columns:   exprList,
			AsKeyword: *asKeyword,
			Query:     selectBody,
		}
		ctes = append(ctes, cte)
	}

	if err := p.expectPeekMany([]lexer.TokenType{lexer.TSelect}); err != nil {
		return nil, err
	}

	p.logger.Debugln("select body of select statement with cte")
	if p.peekTokenIs(lexer.TSelect) {
		selectBody, err := p.parseSelectBody()
		if err != nil {
			return nil, err
		}
		return &ast.SelectStatement{
			WithKeyword: withKeyword,
			CTE:         &ctes,
			SelectBody:  &selectBody,
			Span: ast.NewSpanFromLexerPosition(
				startPositionSelectStatement,
				selectBody.EndPosition,
			),
		}, nil
	}
	return &ast.SelectStatement{}, nil
}

func (p *Parser) parseTopArg(topKw ast.Keyword) (*ast.TopArg, error) {
	topArg := ast.TopArg{}
	topArg.TopKeyword = topKw
	startPosition := topKw.StartPosition
	p.logger.Debug(p.peekToken)
	numericLiteral, err := p.consumeToken(lexer.TNumericLiteral)
	if err != nil {
		return nil, err
	}
	expr := &ast.ExprNumberLiteral{
		Value: numericLiteral.Value,
		Span:  ast.NewSpanFromToken(*numericLiteral),
	}
	topArg.Quantity = expr
	topArg.Span = ast.NewSpanFromLexerPosition(startPosition, expr.EndPosition)

	if kw := p.maybeKeyword(lexer.TPercent); kw != nil {
		topArg.PercentKeyword = kw
		topArg.Span = ast.NewSpanFromLexerPosition(startPosition, kw.EndPosition)
	}

	if withKw := p.maybeKeyword(lexer.TWith); withKw != nil {
		tiesKw, err := p.consumeKeyword(lexer.TTies)
		if err != nil {
			return nil, err
		}
		topArg.WithTiesKeyword = &[2]ast.Keyword{*withKw, *tiesKw}
		topArg.Span = ast.NewSpanFromLexerPosition(startPosition, tiesKw.EndPosition)
	}

	return &topArg, nil
}

func (p *Parser) parseSelectBody() (ast.SelectBody, error) {
	stmt := ast.SelectBody{}
	startPositionSelectBody := p.peekToken.Start
	kw, err := p.consumeKeyword(lexer.TSelect)
	if err != nil {
		return stmt, err
	}
	stmt.SelectKeyword = *kw
	if kw := p.maybeKeyword(lexer.TDistinct); kw != nil {
		stmt.DistinctKeyword = kw
	}

	// check for optional all keyword
	if kw := p.maybeKeyword(lexer.TAll); kw != nil {
		stmt.AllKeyword = kw
	}

	if kw := p.maybeKeyword(lexer.TTop); kw != nil {
		topArg, err := p.parseTopArg(*kw)
		if err != nil {
			return stmt, err
		}
		stmt.Top = topArg
	}

	selectItems, err := p.parseSelectItems()
	if err != nil {
		return stmt, err
	}
	stmt.SelectItems = *selectItems
	stmt.Span = ast.NewSpanFromLexerPosition(
		startPositionSelectBody,
		selectItems.EndPosition,
	)

	table, err := p.parseTableArg()
	if err != nil {
		return stmt, err
	}
	stmt.Table = table
	stmt.Span = ast.NewSpanFromLexerPosition(
		startPositionSelectBody,
		table.EndPosition,
	)

	if p.peekTokenIs(lexer.TWhere) {
		whereExpression, err := p.parseWhereExpression()
		if err != nil {
			return stmt, err
		}
		stmt.WhereClause = whereExpression
		stmt.Span = ast.NewSpanFromLexerPosition(
			startPositionSelectBody,
			whereExpression.EndPosition,
		)
	}

	if p.peekTokenIs(lexer.TGroup) {
		groupByClause, err := p.parseGroupByClause()
		if err != nil {
			return stmt, err
		}
		stmt.GroupByClause = groupByClause
		stmt.Span = ast.NewSpanFromLexerPosition(
			startPositionSelectBody,
			groupByClause.EndPosition,
		)
	}

	if p.peekTokenIs(lexer.THaving) {
		havingExpression, err := p.parseHavingExpression()
		if err != nil {
			return stmt, err
		}
		stmt.HavingClause = havingExpression
		stmt.Span = ast.NewSpanFromLexerPosition(
			startPositionSelectBody,
			havingExpression.EndPosition,
		)
	}

	if p.peekTokenIs(lexer.TOrder) {
		orderByClause, err := p.parseOrderByClause()
		if err != nil {
			return stmt, err
		}
		stmt.OrderByClause = orderByClause
		stmt.Span = ast.NewSpanFromLexerPosition(
			startPositionSelectBody,
			orderByClause.EndPosition,
		)
	}

	return stmt, nil
}

func (p *Parser) parseSelectSubquery() (ast.ExprSubquery, error) {
	stmt := ast.ExprSubquery{}
	startPositionSubquery := p.peekToken.Start
	p.logger.Debug("parsing subquery")

	selectBody, err := p.parseSelectBody()
	if err != nil {
		return stmt, err
	}
	stmt.SelectBody = selectBody
	stmt.Span = ast.NewSpanFromLexerPosition(
		startPositionSubquery,
		selectBody.EndPosition,
	)

	return stmt, nil
}

func (p *Parser) parseSelectItems() (*ast.SelectItems, error) {
	selectItems := ast.SelectItems{}
	startPositionSelectItems := p.peekToken.Start
	p.logger.Debug(p.peekToken)
	for {
		err := p.expectSelectItemStart()
		if err != nil {
			return nil, err
		}

		expr, err := p.parseExpression(PrecedenceLowest)
		if err != nil {
			return nil, err
		}
		switch v := expr.(type) {
		case *ast.ExprSubquery:
			if len(v.SelectItems.Items) > 1 {
				return nil, p.peekErrorString("Subquery must contain only one column")
			}
			if v.GroupByClause != nil && len(v.GroupByClause.Items) > 1 && v.DistinctKeyword != nil {
				return nil, p.peekErrorString("The 'DISTINCT' keyword can't be used with subqueries that include 'GROUP BY'")
			}
			if v.OrderByClause != nil && len(v.OrderByClause.Expressions) > 1 && v.Top == nil {
				return nil, p.peekErrorString("'ORDER BY' can only be specified when 'TOP' is also specified")
			}
			p.logger.Debugln("subquery in select item")
			break
		}

		asKw := p.maybeKeyword(lexer.TAs)

		if p.peekTokenIsAny([]lexer.TokenType{
			lexer.TIdentifier,
			lexer.TQuotedIdentifier,
			lexer.TStringLiteral,
		}) {
			var alias ast.Expression
			if p.peekTokenIs(lexer.TIdentifier) {
				alias = &ast.ExprIdentifier{
					Value: p.peekToken.Value,
					Span:  ast.NewSpanFromToken(p.peekToken),
				}
			} else if p.peekTokenIs(lexer.TStringLiteral) {
				alias = &ast.ExprStringLiteral{
					Value: p.peekToken.Value,
					Span:  ast.NewSpanFromToken(p.peekToken),
				}
			} else if p.peekTokenIs(lexer.TQuotedIdentifier) {
				alias = &ast.ExprQuotedIdentifier{
					Value: p.peekToken.Value,
					Span:  ast.NewSpanFromToken(p.peekToken),
				}
			}

			p.nextToken()

			selectItem := &ast.ExprWithAlias{
				Expression: expr,
				AsKeyword:  asKw,
				Alias:      alias,
			}
			selectItem.SetSpan(ast.Span{
				StartPosition: expr.GetSpan().StartPosition,
				EndPosition:   alias.GetSpan().EndPosition,
			})
			selectItems.Items = append(selectItems.Items, selectItem)
		} else if asKw == nil {
			selectItems.Items = append(selectItems.Items, expr)
		} else {
			return nil, fmt.Errorf("Missing alias after \"As\" keyword")
		}

		p.logger.Debug(p.peekToken)
		if p.peekToken.Type != lexer.TComma {
			break
		}

		p.nextToken()
	}

	selectItems.SetSpan(ast.NewSpanFromLexerPosition(startPositionSelectItems, selectItems.Items[len(selectItems.Items)-1].GetSpan().EndPosition))
	return &selectItems, nil
}

func (p *Parser) parseTableArg() (*ast.TableArg, error) {
	fromKeyword, err := p.consumeKeyword(lexer.TFrom)
	if err != nil {
		p.logger.Debug("from err")
		return nil, err
	}
	startPosition := fromKeyword.StartPosition
	p.logger.Debug("parsing table arg")
	p.logger.Debug(p.peekToken)

	tableSource, err := p.parseTableSource()
	if err != nil {
		return nil, err
	}

	p.logger.Debugf("peek token: %s", p.peekToken.Value)
	if !p.peekTokenIsAny([]lexer.TokenType{
		lexer.TInner,
		lexer.TLeft,
		lexer.TRight,
		lexer.TFull,
	}) {
		return &ast.TableArg{
			FromKeyword: *fromKeyword,
			Table:       tableSource,
			Span:        ast.NewSpanFromLexerPosition(startPosition, tableSource.EndPosition),
		}, nil
	}

	joins, err := p.parseJoins()
	if err != nil {
		return nil, err
	}

	return &ast.TableArg{
		FromKeyword: *fromKeyword,
		Table:       tableSource,
		Joins:       joins,
		Span:        ast.NewSpanFromLexerPosition(startPosition, joins[len(joins)-1].EndPosition),
	}, nil
}

func (p *Parser) parseTableSource() (*ast.TableSource, error) {
	startPosition := p.peekToken.Start
	err := p.expectTableSourceStart()
	if err != nil {
		return nil, err
	}

	source, err := p.parseExpression(PrecedenceLowest)
	if err != nil {
		return nil, err
	}
	p.logger.Debugf("source %v", source)

	var tableSourceType ast.TableSourceType
	switch source.(type) {
	case *ast.ExprIdentifier, *ast.ExprCompoundIdentifier, *ast.ExprLocalVariable:
		tableSourceType = ast.TSTTable
	case *ast.ExprFunctionCall:
		tableSourceType = ast.TSTTableValuedFunction
	case *ast.ExprSubquery:
		tableSourceType = ast.TSTDerived
	default:
		return nil, p.peekErrorString("Table Name or Function or Subquery")
	}

	// check table alias
	if p.peekTokenIsAny([]lexer.TokenType{
		lexer.TIdentifier,
		lexer.TQuotedIdentifier,
	}) {
		var alias ast.Expression
		if token := p.maybeToken(lexer.TIdentifier); token != nil {
			alias = &ast.ExprIdentifier{
				Value: token.Value,
				Span:  ast.NewSpanFromToken(*token),
			}
		} else if token := p.maybeToken(lexer.TQuotedIdentifier); token != nil {
			alias = &ast.ExprQuotedIdentifier{
				Value: token.Value,
				Span:  ast.NewSpanFromToken(*token),
			}
		}
		source = &ast.ExprWithAlias{
			Span:       ast.NewSpanFromLexerPosition(startPosition, alias.GetSpan().EndPosition),
			Expression: source,
			Alias:      alias,
		}
	}

	return &ast.TableSource{
		Type:   tableSourceType,
		Source: source,
		Span:   ast.NewSpanFromLexerPosition(startPosition, source.GetSpan().EndPosition),
	}, nil
}

func (p *Parser) parseJoins() ([]ast.Join, error) {
	joins := []ast.Join{}

	for {
		startPosition := p.peekToken.Start
		var joinType ast.JoinType
		var joinTypeKeyword []ast.Keyword
		var joinKeyword ast.Keyword
		if kw := p.maybeKeyword(lexer.TInner); kw != nil {
			joinTypeKeyword = append(joinTypeKeyword, *kw)
			joinType = ast.JTInner
			joinKeyword = *kw
		} else if kw := p.maybeKeyword(lexer.TLeft); kw != nil {
			joinTypeKeyword = append(joinTypeKeyword, *kw)
			if kw := p.maybeKeyword(lexer.TOuter); kw != nil {
				joinType = ast.JTLeftOuter
				joinTypeKeyword = append(joinTypeKeyword, *kw)
			} else {
				joinType = ast.JTLeft
			}
		} else if kw := p.maybeKeyword(lexer.TRight); kw != nil {
			joinTypeKeyword = append(joinTypeKeyword, *kw)
			if kw := p.maybeKeyword(lexer.TOuter); kw != nil {
				joinType = ast.JTRightOuter
				joinTypeKeyword = append(joinTypeKeyword, *kw)
			} else {
				joinType = ast.JTRight
			}
		} else if kw := p.maybeKeyword(lexer.TFull); kw != nil {
			joinTypeKeyword = append(joinTypeKeyword, *kw)
			if kw := p.maybeKeyword(lexer.TOuter); kw != nil {
				joinType = ast.JTFullOuter
				joinTypeKeyword = append(joinTypeKeyword, *kw)
			} else {
				joinType = ast.JTFull
			}
		} else {
			break
		}

		kw, err := p.consumeKeyword(lexer.TJoin)
		if err != nil {
			return nil, err
		}
		joinTypeKeyword = append(joinTypeKeyword, *kw)
		p.logger.Debugf("join keyword: %s", joinKeyword.TokenLiteral())

		tableSource, err := p.parseTableSource()
		if err != nil {
			return nil, err
		}

		onKw, err := p.consumeKeyword(lexer.TOn)
		if err != nil {
			return nil, err
		}

		searchCondition, err := p.parseExpression(PrecedenceLowest)
		if err != nil {
			return nil, err
		}

		joins = append(joins, ast.Join{
			JoinTypeKeyword: joinTypeKeyword,
			Type:            joinType,
			Table:           tableSource,
			OnKeyword:       onKw,
			Condition:       searchCondition,
			Span:            ast.NewSpanFromLexerPosition(startPosition, searchCondition.GetSpan().EndPosition),
		})
	}

	return joins, nil
}

func (p *Parser) parseWhereExpression() (*ast.WhereClause, error) {
	whereClause := ast.WhereClause{}
	startPosition := p.peekToken.Start
	whereKw, err := p.consumeKeyword(lexer.TWhere)
	if err != nil {
		return nil, err
	}
	whereClause.WhereKeyword = *whereKw
	p.logger.Debug("parsing where")

	//possible fix
	// p.nextToken()
	expr, err := p.parseExpression(PrecedenceLowest)
	if err != nil {
		return nil, err
	}

	switch expr.(type) {
	case *ast.ExprComparisonOperator,
		*ast.ExprAndLogicalOperator,
		*ast.ExprAllLogicalOperator,
		*ast.ExprBetweenLogicalOperator,
		*ast.ExprExistsLogicalOperator,
		*ast.ExprInSubqueryLogicalOperator,
		*ast.ExprInLogicalOperator,
		*ast.ExprLikeLogicalOperator,
		*ast.ExprNotLogicalOperator,
		*ast.ExprOrLogicalOperator,
		*ast.ExprSomeLogicalOperator,
		*ast.ExprAnyLogicalOperator:
		break
	default:
		return nil, p.peekErrorString("expression after 'WHERE' keyword")
	}
	p.logger.Debugf("expr: %s\n", expr)
	whereClause.Clause = expr
	whereClause.SetSpan(ast.NewSpanFromLexerPosition(startPosition, expr.GetSpan().EndPosition))
	return &whereClause, nil
}

func (p *Parser) parseGroupByClause() (*ast.GroupByClause, error) {
	groupByClause := ast.GroupByClause{}
	startPosition := p.peekToken.Start
	items := []ast.Expression{}
	groupKw, err := p.consumeKeyword(lexer.TGroup)
	if err != nil {
		return nil, err
	}
	byKw, err := p.consumeKeyword(lexer.TBy)
	if err != nil {
		return nil, err
	}
	groupByClause.GroupByKeyword = [2]ast.Keyword{*groupKw, *byKw}

	p.logger.Debug("parsing group by clause")

	for {
		err := p.expectGroupByStart()
		if err != nil {
			return nil, err
		}

		expr, err := p.parseExpression(PrecedenceLowest)
		if err != nil {
			return nil, err
		}
		items = append(items, expr)

		if p.peekToken.Type != lexer.TComma {
			break
		}

		p.nextToken()
	}
	p.nextToken()

	groupByClause.Items = items
	groupByClause.SetSpan(ast.NewSpanFromLexerPosition(startPosition, items[len(items)-1].GetSpan().EndPosition))
	return &groupByClause, nil
}

func (p *Parser) parseHavingExpression() (*ast.HavingClause, error) {
	startPosition := p.peekToken.Start
	havingKw, err := p.consumeKeyword(lexer.THaving)
	if err != nil {
		return nil, err
	}
	havingClause := ast.HavingClause{HavingKeyword: *havingKw}
	p.logger.Debug("parsing having")

	// go to having token
	expr, err := p.parseExpression(PrecedenceLowest)
	if err != nil {
		return nil, err
	}

	switch expr.(type) {
	case *ast.ExprComparisonOperator,
		*ast.ExprAndLogicalOperator,
		*ast.ExprAllLogicalOperator,
		*ast.ExprBetweenLogicalOperator,
		*ast.ExprExistsLogicalOperator,
		*ast.ExprInSubqueryLogicalOperator,
		*ast.ExprInLogicalOperator,
		*ast.ExprLikeLogicalOperator,
		*ast.ExprNotLogicalOperator,
		*ast.ExprOrLogicalOperator,
		*ast.ExprSomeLogicalOperator,
		*ast.ExprAnyLogicalOperator:
		break
	default:
		return nil, p.peekErrorString("expression after 'HAVING' keyword")
	}
	havingClause.Clause = expr
	havingClause.SetSpan(ast.NewSpanFromLexerPosition(startPosition, expr.GetSpan().EndPosition))
	return &havingClause, nil
}

func (p *Parser) parseOrderByClause() (*ast.OrderByClause, error) {
	startPosition := p.peekToken.Start
	orderKw, err := p.consumeKeyword(lexer.TOrder)
	if err != nil {
		return nil, err
	}
	byKw, err := p.consumeKeyword(lexer.TBy)
	if err != nil {
		return nil, err
	}
	p.logger.Debug("parsing order by clause")
	args, err := p.parseOrderByArgs()
	if err != nil {
		return nil, err
	}

	orderByClause := &ast.OrderByClause{
		OrderByKeyword: [2]ast.Keyword{*orderKw, *byKw},
		Expressions:    args,
		Span:           ast.NewSpanFromLexerPosition(startPosition, args[len(args)-1].EndPosition),
	}

	if !p.peekTokenIs(lexer.TOffset) {
		return orderByClause, nil
	}

	offsetFetchClause, err := p.parseOffsetFetchClause()
	if err != nil {
		return nil, err
	}

	orderByClause.OffsetFetch = offsetFetchClause
	orderByClause.Span = ast.NewSpanFromLexerPosition(startPosition, offsetFetchClause.Span.EndPosition)

	return orderByClause, nil
}

func (p *Parser) parseOrderByArgs() ([]ast.OrderByArg, error) {
	items := []ast.OrderByArg{}

	for {
		startPosition := p.peekToken.Start
		err := p.expectPeekMany([]lexer.TokenType{
			lexer.TIdentifier,
			lexer.TNumericLiteral,
			lexer.TLocalVariable,
			lexer.TQuotedIdentifier,
		})
		if err != nil {
			return items, err
		}

		expr, err := p.parseExpression(PrecedenceLowest)
		if err != nil {
			return items, err
		}
		if kw := p.maybeKeyword(lexer.TAsc); kw != nil {
			items = append(items, ast.OrderByArg{
				Column:       expr,
				Type:         ast.OBAsc,
				OrderKeyword: kw,
				Span:         ast.NewSpanFromLexerPosition(startPosition, kw.EndPosition),
			})
		} else if kw := p.maybeKeyword(lexer.TDesc); kw != nil {
			items = append(items, ast.OrderByArg{
				Column:       expr,
				Type:         ast.OBDesc,
				OrderKeyword: kw,
				Span:         ast.NewSpanFromLexerPosition(startPosition, kw.EndPosition),
			})
		} else {
			items = append(items, ast.OrderByArg{
				Column: expr,
				Type:   ast.OBNone,
				Span:   ast.NewSpanFromLexerPosition(startPosition, expr.GetSpan().EndPosition),
			})
		}

		if p.peekToken.Type != lexer.TComma {
			break
		}

		p.nextToken()
	}

	return items, nil
}

func (p *Parser) parseOffsetFetchClause() (*ast.OffsetFetchClause, error) {
	p.logger.Debug("parsing offset fetch clause")
	startPosition := p.peekToken.Start

	offset, err := p.parseOffset()
	if err != nil {
		return nil, err
	}
	offsetFetchClause := ast.OffsetFetchClause{
		Offset: offset,
		Span:   ast.NewSpanFromLexerPosition(startPosition, offset.EndPosition),
	}

	if !p.peekTokenIs(lexer.TFetch) {
		return &offsetFetchClause, nil
	}

	fetch, err := p.parseFetch()
	if err != nil {
		return nil, err
	}

	offsetFetchClause.Fetch = &fetch
	offsetFetchClause.Span = ast.NewSpanFromLexerPosition(startPosition, fetch.EndPosition)

	return &offsetFetchClause, nil
}

func (p *Parser) parseOffset() (ast.OffsetArg, error) {
	startPosition := p.peekToken.Start
	offsetKw, err := p.consumeKeyword(lexer.TOffset)
	offsetArg := ast.OffsetArg{OffsetKeyword: *offsetKw}
	offset, err := p.parseExpression(PrecedenceLowest)
	if err != nil {
		return offsetArg, err
	}

	err = p.expectPeekMany([]lexer.TokenType{lexer.TRow, lexer.TRows})
	if err != nil {
		return offsetArg, err
	}

	offsetArg.Value = offset
	offsetArg.Span = ast.NewSpanFromLexerPosition(startPosition, p.peekToken.End)
	switch p.peekToken.Type {
	case lexer.TRow:
		offsetArg.RowOrRows = ast.RRRow
		kw, _ := p.consumeKeyword(lexer.TRow)
		offsetArg.RowOrRowsKeyword = *kw
		return offsetArg, nil
	case lexer.TRows:
		offsetArg.RowOrRows = ast.RRRows
		kw, _ := p.consumeKeyword(lexer.TRows)
		offsetArg.RowOrRowsKeyword = *kw
		return offsetArg, nil
	default:
		return offsetArg, p.peekErrorString("'ROW' or 'ROWS' after offset expression")
	}
}

func (p *Parser) parseFetch() (ast.FetchArg, error) {
	startPosition := p.peekToken.Start
	fetchKw, err := p.consumeKeyword(lexer.TFetch)
	if err != nil {
		return ast.FetchArg{}, err
	}
	fetchArg := ast.FetchArg{FetchKeyword: *fetchKw}
	if err := p.expectPeekMany([]lexer.TokenType{lexer.TFirst, lexer.TNext}); err != nil {
		return fetchArg, err
	}

	fetchArg.NextOrFirstKeyword = ast.NewKeywordFromToken(p.peekToken)
	p.nextToken()
	switch p.peekToken.Type {
	case lexer.TFirst:
		fetchArg.NextOrFirst = ast.NFFirst
		break
	case lexer.TNext:
		fetchArg.NextOrFirst = ast.NFNext
		break
	}

	fetch, err := p.parseExpression(PrecedenceLowest)
	if err != nil {
		return fetchArg, err
	}

	fetchArg.Value = fetch
	err = p.expectPeekMany([]lexer.TokenType{lexer.TRow, lexer.TRows})
	if err != nil {
		return fetchArg, err
	}

	fetchArg.RowOrRowsKeyword = ast.NewKeywordFromToken(p.peekToken)
	p.nextToken()
	switch p.peekToken.Type {
	case lexer.TRow:
		fetchArg.RowOrRows = ast.RRRow
		break
	case lexer.TRows:
		fetchArg.RowOrRows = ast.RRRows
		break
	}

	onlyKw, err := p.consumeKeyword(lexer.TOnly)
	if err != nil {
		return fetchArg, err
	}
	fetchArg.OnlyKeyword = *onlyKw
	fetchArg.Span = ast.NewSpanFromLexerPosition(startPosition, onlyKw.EndPosition)

	return fetchArg, nil
}

func (p *Parser) parseOverClause() (*ast.FunctionOverClause, error) {
	startPosition := p.peekToken.Start
	overKw, err := p.consumeKeyword(lexer.TOver)
	if err != nil {
		return nil, err
	}
	if _, err := p.consumeToken(lexer.TLeftParen); err != nil {
		return nil, err
	}

	functionOverClause := ast.FunctionOverClause{OverKeyword: *overKw}

	if partitionKw := p.maybeKeyword(lexer.TPartition); partitionKw != nil {
		byKw, err := p.consumeKeyword(lexer.TBy)
		if err != nil {
			return nil, err
		}
		expressions, err := p.parsePartitionClause()
		if err != nil {
			return nil, err
		}
		functionOverClause.PartitionByClause = expressions
		functionOverClause.PartitionByKeyword = &[2]ast.Keyword{*partitionKw, *byKw}
	}

	if orderKw := p.maybeKeyword(lexer.TOrder); orderKw != nil {
		byKw, err := p.consumeKeyword(lexer.TBy)
		if err != nil {
			return nil, err
		}
		args, err := p.parseOrderByArgs()
		if err != nil {
			return nil, err
		}
		functionOverClause.OrderByClause = args
		functionOverClause.OrderByKeyword = &[2]ast.Keyword{*orderKw, *byKw}
	}

	if p.peekTokenIsAny([]lexer.TokenType{lexer.TRows, lexer.TRange}) {
		clause, err := p.parseWindowFrameClause()
		if err != nil {
			return nil, err
		}
		functionOverClause.WindowFrameClause = clause
	}

	rightParen, err := p.consumeToken(lexer.TRightParen)
	if err != nil {
		return nil, err
	}
	functionOverClause.Span = ast.NewSpanFromLexerPosition(startPosition, rightParen.End)

	return &functionOverClause, nil
}

func (p *Parser) parsePartitionClause() ([]ast.Expression, error) {
	args := []ast.Expression{}

	for {
		if !p.peekTokenIs(lexer.TIdentifier) && !p.peekTokenIs(lexer.TQuotedIdentifier) {
			break
		}

		expr, err := p.parseExpression(PrecedenceLowest)
		if err != nil {
			return nil, err
		}

		args = append(args, expr)

		if !p.peekTokenIs(lexer.TComma) {
			break
		}
		p.nextToken()
	}

	if len(args) == 0 {
		return nil, p.peekErrorString("PARTITION BY items in PARTITION BY expression")
	}

	return args, nil
}

func (p *Parser) parseWindowFrameClause() (*ast.WindowFrameClause, error) {
	startPosition := p.peekToken.Start
	var rowsOrRangeType ast.RowsOrRangeType
	var rowsOrRangeKw ast.Keyword
	if kw := p.maybeKeyword(lexer.TRows); kw != nil {
		rowsOrRangeType = ast.RRTRows
		rowsOrRangeKw = *kw
	} else if kw := p.maybeKeyword(lexer.TRange); kw != nil {
		rowsOrRangeType = ast.RRTRange
		rowsOrRangeKw = *kw
	}
	var betweenKw *ast.Keyword
	var windowFrameStart ast.WindowFrameBound
	var windowFrameEnd ast.WindowFrameBound
	followingNeeded := false
	// parse between
	p.logger.Debug("p.peekToken.Value: ", p.peekToken.Value)
	p.logger.Debug("p.peekToken.Value: ", p.peekToken.Value)
	if kw := p.maybeKeyword(lexer.TBetween); kw != nil {
		followingNeeded = true
		betweenKw = kw
	}
	p.logger.Debug("p.peekToken.Value: ", p.peekToken.Value)
	p.logger.Debug("p.peekToken.Value: ", p.peekToken.Value)
	var boundKeywordStart []ast.Keyword
	if boundKw := p.maybeKeyword(lexer.TUnbounded); boundKw != nil {
		boundKeywordStart = append(boundKeywordStart, *boundKw)
		boundKw2, err := p.consumeKeyword(lexer.TPreceding)
		if err != nil {
			return nil, err
		}
		boundKeywordStart = append(boundKeywordStart, *boundKw2)
		windowFrameStart = ast.WindowFrameBound{
			BoundKeyword: boundKeywordStart,
			Type:         ast.WFBTUnboundedPreceding,
			Span:         ast.NewSpanFromLexerPosition(boundKw.StartPosition, boundKw2.EndPosition),
		}
	} else if boundKw := p.maybeKeyword(lexer.TCurrent); boundKw != nil {
		boundKeywordStart = append(boundKeywordStart, *boundKw)
		boundKw2, err := p.consumeKeyword(lexer.TRow)
		if err != nil {
			return nil, err
		}
		boundKeywordStart = append(boundKeywordStart, *boundKw2)
		windowFrameStart = ast.WindowFrameBound{
			BoundKeyword: boundKeywordStart,
			Type:         ast.WFBTCurrentRow,
			Span:         ast.NewSpanFromLexerPosition(boundKw.StartPosition, boundKw2.EndPosition),
		}
	} else if p.peekTokenIs(lexer.TNumericLiteral) {
		expr, err := p.parseExpression(PrecedenceLowest)
		p.logger.Debug("test")
		if err != nil {
			return nil, err
		}

		boundKw, err := p.consumeKeyword(lexer.TPreceding)
		if err != nil {
			return nil, err
		}
		boundKeywordStart = append(boundKeywordStart, *boundKw)
		windowFrameStart = ast.WindowFrameBound{
			BoundKeyword: boundKeywordStart,
			Type:         ast.WFBTPreceding,
			Expression:   expr,
			Span:         ast.NewSpanFromLexerPosition(expr.GetSpan().StartPosition, boundKw.EndPosition),
		}
	} else {
		return nil, p.peekErrorString("UNBOUNDED PRECEDING or peek ROW or <NUMBER> PRECEDING")
	}

	if !followingNeeded {
		return &ast.WindowFrameClause{
			RowsOrRangeKeyword: rowsOrRangeKw,
			RowsOrRange:        rowsOrRangeType,
			Start:              &windowFrameStart,
			BetweenKeyword:     betweenKw,
			Span:               ast.NewSpanFromLexerPosition(startPosition, windowFrameStart.EndPosition),
		}, nil
	}

	andKw, err := p.consumeKeyword(lexer.TAnd)
	if err != nil {
		return nil, err
	}

	var boundKeywordEnd []ast.Keyword
	if boundKw := p.maybeKeyword(lexer.TUnbounded); boundKw != nil {
		boundKeywordEnd = append(boundKeywordEnd, *boundKw)
		boundKw2, err := p.consumeKeyword(lexer.TFollowing)
		if err != nil {
			return nil, err
		}
		boundKeywordEnd = append(boundKeywordEnd, *boundKw2)
		windowFrameEnd = ast.WindowFrameBound{
			Type:         ast.WFBTUnboundedFollowing,
			BoundKeyword: boundKeywordEnd,
			Span:         ast.NewSpanFromLexerPosition(boundKw.StartPosition, boundKw2.EndPosition),
		}
	} else if boundKw := p.maybeKeyword(lexer.TCurrent); boundKw != nil {
		boundKeywordEnd = append(boundKeywordEnd, *boundKw)
		boundKw2, err := p.consumeKeyword(lexer.TRow)
		if err != nil {
			return nil, err
		}
		boundKeywordEnd = append(boundKeywordEnd, *boundKw2)
		windowFrameEnd = ast.WindowFrameBound{
			BoundKeyword: boundKeywordEnd,
			Type:         ast.WFBTCurrentRow,
			Span:         ast.NewSpanFromLexerPosition(boundKw.StartPosition, boundKw2.EndPosition),
		}
	} else if p.peekTokenIs(lexer.TNumericLiteral) {
		expr, err := p.parseExpression(PrecedenceLowest)
		if err != nil {
			return nil, err
		}
		boundKw, err := p.consumeKeyword(lexer.TFollowing)
		if err != nil {
			return nil, err
		}
		boundKeywordEnd = append(boundKeywordEnd, *boundKw)
		windowFrameEnd = ast.WindowFrameBound{
			Type:         ast.WFBTFollowing,
			BoundKeyword: boundKeywordEnd,
			Expression:   expr,
			Span:         ast.NewSpanFromLexerPosition(expr.GetSpan().StartPosition, boundKw.EndPosition),
		}
	} else {
		return nil, p.peekErrorString("UNBOUNDED FOLLOWING or peek ROW or <NUMBER> FOLLOWING")
	}

	return &ast.WindowFrameClause{
		RowsOrRangeKeyword: rowsOrRangeKw,
		RowsOrRange:        rowsOrRangeType,
		Start:              &windowFrameStart,
		BetweenKeyword:     betweenKw,
		AndKeyword:         andKw,
		End:                &windowFrameEnd,
		Span:               ast.NewSpanFromLexerPosition(startPosition, windowFrameEnd.EndPosition),
	}, nil
}

func (p *Parser) parseExpressionList() (ast.ExprExpressionList, error) {
	startPosition := p.peekToken.Start
	expressionList := ast.ExprExpressionList{}

	for {
		err := p.expectExpressionListStart()
		if err != nil {
			return expressionList, err
		}

		expr, err := p.parseExpression(PrecedenceLowest)
		if err != nil {
			return expressionList, err
		}
		expressionList.List = append(expressionList.List, expr)

		if p.peekToken.Type != lexer.TComma {
			break
		}

		p.nextToken()
	}

	expressionList.Span = ast.NewSpanFromLexerPosition(startPosition,
		expressionList.List[len(expressionList.List)-1].GetSpan().EndPosition)
	return expressionList, nil
}

func (p *Parser) parseNumericSize() (*ast.NumericSize, error) {
	startPosition := p.peekToken.Start
	if _, err := p.consumeToken(lexer.TLeftParen); err != nil {
		return nil, err
	}

	numericLiteral, err := p.consumeToken(lexer.TNumericLiteral)
	if err != nil {
		return nil, err
	}

	// parse precision
	precision, err := strconv.ParseUint(numericLiteral.Value, 10, 32)
	if err != nil {
		return nil, p.peekErrorString("could not convert numeric literal to uint32")
	}
	precision32 := uint32(precision)
	if t := p.maybeToken(lexer.TRightParen); t != nil {
		return &ast.NumericSize{
			Precision: precision32,
			Span:      ast.NewSpanFromLexerPosition(startPosition, t.End),
		}, nil
	}

	// parse scale
	if _, err := p.consumeToken(lexer.TComma); err != nil {
		return nil, err
	}

	numericLiteral, err = p.consumeToken(lexer.TNumericLiteral)
	if err != nil {
		return nil, err
	}

	scale, err := strconv.ParseUint(numericLiteral.Value, 10, 32)
	if err != nil {
		return nil, p.peekErrorString("could not convert numeric literal to uint32")
	}
	scale32 := uint32(scale)
	rightParen, err := p.consumeToken(lexer.TRightParen)
	if err != nil {
		return nil, err
	}

	return &ast.NumericSize{
		Precision: precision32,
		Scale:     &scale32,
		Span:      ast.NewSpanFromLexerPosition(startPosition, rightParen.End),
	}, nil
}

func (p *Parser) parseDataType() (*ast.DataType, error) {
	startPosition := p.peekToken.Start
	p.logger.Debugf("peek token: %s", p.peekToken.Value)
	dataTypeToken, err := p.consumeTokenAny(ast.DataTypeTokenTypes)
	if err != nil {
		return nil, err
	}
	p.logger.Debugf("token: %s", dataTypeToken.Value)
	p.logger.Debugf("peek token: %s", p.peekToken.Value)
	dataType := ast.DataType{
		Span: ast.NewSpanFromToken(*dataTypeToken),
	}
	switch dataTypeToken.Type {
	case lexer.TInt:
		dataType.Kind = ast.DTInt
	case lexer.TBigint:
		dataType.Kind = ast.DTBigInt
	case lexer.TTinyint:
		dataType.Kind = ast.DTTinyInt
	case lexer.TSmallint:
		dataType.Kind = ast.DTSmallInt
	case lexer.TBit:
		dataType.Kind = ast.DTBit
	case lexer.TFloat:
		dataType.Kind = ast.DTFloat
		if !p.peekTokenIs(lexer.TLeftParen) {
			break
		}

		if _, err := p.consumeToken(lexer.TLeftParen); err != nil {
			return nil, err
		}
		numberLiteral, err := p.consumeToken(lexer.TNumericLiteral)
		if err != nil {
			return nil, err
		}
		size, err := strconv.ParseUint(numberLiteral.Value, 10, 32)
		if err != nil {
			return nil, err
		}
		size32 := uint32(size)
		dataType.FloatPrecision = &size32
		rightParen, err := p.consumeToken(lexer.TRightParen)
		if err != nil {
			return nil, err
		}
		dataType.Span = ast.NewSpanFromLexerPosition(startPosition, rightParen.End)
	case lexer.TReal:
		dataType.Kind = ast.DTReal
	case lexer.TDate:
		dataType.Kind = ast.DTDate
	case lexer.TDatetime:
		dataType.Kind = ast.DTDatetime
	case lexer.TTime:
		dataType.Kind = ast.DTTime
	case lexer.TDecimal:
		dataType.Kind = ast.DTDecimal
		if !p.peekTokenIs(lexer.TLeftParen) {
			break
		}
		numericSize, err := p.parseNumericSize()
		if err != nil {
			return nil, err
		}
		dataType.DecimalNumericSize = numericSize
		dataType.Span = ast.NewSpanFromLexerPosition(startPosition, numericSize.EndPosition)
	case lexer.TNumeric:
		dataType.Kind = ast.DTNumeric
		if !p.peekTokenIs(lexer.TLeftParen) {
			break
		}
		numericSize, err := p.parseNumericSize()
		if err != nil {
			return nil, err
		}
		dataType.DecimalNumericSize = numericSize
		dataType.Span = ast.NewSpanFromLexerPosition(startPosition, numericSize.EndPosition)
	case lexer.TVarchar:
		dataType.Kind = ast.DTVarchar
		if !p.peekTokenIs(lexer.TLeftParen) {
			break
		}

		if _, err := p.consumeToken(lexer.TLeftParen); err != nil {
			return nil, err
		}
		numericLiteral, err := p.consumeToken(lexer.TNumericLiteral)
		if err != nil {
			return nil, err
		}
		size, err := strconv.ParseUint(numericLiteral.Value, 10, 32)
		if err != nil {
			return nil, p.peekErrorString("could not convert numeric literal to uint32")
		}
		size32 := uint32(size)
		dataType.FloatPrecision = &size32
		rightParen, err := p.consumeToken(lexer.TRightParen)
		if err != nil {
			return nil, err
		}
		dataType.Span = ast.NewSpanFromLexerPosition(startPosition, rightParen.End)
	default:
		return nil, p.peekErrorString("a Builtin Datatype")
	}

	return &dataType, nil
}

func (p *Parser) parseCast() (*ast.ExprCast, error) {
	startPosition := p.peekToken.Start
	castKw, err := p.consumeKeyword(lexer.TCast)
	if err != nil {
		return nil, err
	}
	if _, err := p.consumeToken(lexer.TLeftParen); err != nil {
		return nil, err
	}
	p.logger.Debug("parsing cast expression")

	expr, err := p.parseExpression(PrecedenceLowest)
	if err != nil {
		return nil, err
	}

	p.logger.Debug(expr.TokenLiteral())
	asKw, err := p.consumeKeyword(lexer.TAs)
	if err != nil {
		return nil, err
	}

	dt, err := p.parseDataType()
	if err != nil {
		return nil, err
	}
	p.logger.Debug(dt.TokenLiteral())
	p.logger.Debug(p.peekToken)
	rightParen, err := p.consumeToken(lexer.TRightParen)
	if err != nil {
		return nil, err
	}

	return &ast.ExprCast{
		CastKeyword: *castKw,
		Expression:  expr,
		AsKeyword:   *asKw,
		DataType:    *dt,
		Span:        ast.NewSpanFromLexerPosition(startPosition, rightParen.End),
	}, nil
}

func (p *Parser) parseCompoundIdentifier(expr ast.Expression) (*ast.ExprCompoundIdentifier, error) {
	if p.peekToken.Type == lexer.TPeriod {
		// we are dealing with a qualified identifier
		startPositionCompound := expr.GetSpan().StartPosition
		endPositionCompound := expr.GetSpan().EndPosition
		compound := &[]ast.Expression{expr}
		p.logger.Debug("parsing compound identifier")

		// go to period token
		p.nextToken()
		p.logger.Debug("peek token: ", p.peekToken)

		for {
			err := p.expectCompoundIdentifierStart()
			if err != nil {
				return nil, err
			}
			p.logger.Debug("peek token: ", p.peekToken)

			if token := p.maybeToken(lexer.TAsterisk); token != nil {
				expr := &ast.ExprStar{
					Span: ast.NewSpanFromToken(*token),
				}
				*compound = append(*compound, expr)
				break
			} else if token := p.maybeToken(lexer.TQuotedIdentifier); token != nil {
				expr := &ast.ExprQuotedIdentifier{
					Value: token.Value,
					Span:  ast.NewSpanFromToken(*token),
				}
				*compound = append(*compound, expr)
			} else if token := p.maybeToken(lexer.TIdentifier); token != nil {
				expr := &ast.ExprIdentifier{
					Value: token.Value,
					Span:  ast.NewSpanFromToken(*token),
				}
				*compound = append(*compound, expr)
			}
			endPositionCompound = expr.GetSpan().EndPosition
			if token := p.maybeToken(lexer.TPeriod); token == nil {
				break
			}
		}

		return &ast.ExprCompoundIdentifier{
			Identifiers: *compound,
			Span:        ast.NewSpanFromLexerPosition(startPositionCompound, endPositionCompound),
		}, nil
	}

	return nil, nil
}

func (p *Parser) parseFunction() (*ast.ExprFunction, error) {
	var funcType ast.FuncType
	switch p.peekToken.Type {
	case lexer.TDenseRank:
		funcType = ast.FuncDenseRank
	case lexer.TRank:
		funcType = ast.FuncRank
	case lexer.TRowNumber:
		funcType = ast.FuncRowNumber
	case lexer.TAbs:
		funcType = ast.FuncAbs
	case lexer.TAcos:
		funcType = ast.FuncAcos
	case lexer.TAsin:
		funcType = ast.FuncAsin
	case lexer.TAtan:
		funcType = ast.FuncAtan
	case lexer.TCeiling:
		funcType = ast.FuncCeiling
	case lexer.TCos:
		funcType = ast.FuncCos
	case lexer.TCot:
		funcType = ast.FuncCot
	case lexer.TDegrees:
		funcType = ast.FuncDegrees
	case lexer.TExp:
		funcType = ast.FuncExp
	case lexer.TFloor:
		funcType = ast.FuncFloor
	case lexer.TLog:
		funcType = ast.FuncLog
	case lexer.TLog10:
		funcType = ast.FuncLog10
	case lexer.TPi:
		funcType = ast.FuncPi
	case lexer.TPower:
		funcType = ast.FuncPower
	case lexer.TRadians:
		funcType = ast.FuncRadians
	case lexer.TRands:
		funcType = ast.FuncRands
	case lexer.TRound:
		funcType = ast.FuncRound
	case lexer.TSign:
		funcType = ast.FuncSign
	case lexer.TSin:
		funcType = ast.FuncSin
	case lexer.TSqrt:
		funcType = ast.FuncSqrt
	case lexer.TSquare:
		funcType = ast.FuncSquare
	case lexer.TTan:
		funcType = ast.FuncTan
	case lexer.TFirstValue:
		funcType = ast.FuncFirstValue
	case lexer.TLastValue:
		funcType = ast.FuncLastValue
	case lexer.TLag:
		funcType = ast.FuncLag
	case lexer.TLead:
		funcType = ast.FuncLead
	case lexer.TAvg:
		funcType = ast.FuncAvg
	case lexer.TCount:
		funcType = ast.FuncCount
	case lexer.TMax:
		funcType = ast.FuncMax
	case lexer.TMin:
		funcType = ast.FuncMin
	case lexer.TStdev:
		funcType = ast.FuncStdev
	case lexer.TStdevp:
		funcType = ast.FuncStdevp
	case lexer.TSum:
		funcType = ast.FuncSum
	case lexer.TVar:
		funcType = ast.FuncVar
	case lexer.TVarp:
		funcType = ast.FuncVarp
	case lexer.TGetdate:
		funcType = ast.FuncGetdate
	case lexer.TChecksum:
		funcType = ast.FuncChecksum
	case lexer.TNewId:
		funcType = ast.FuncNewId
	}
	p.logger.Debug("in function parse")
	function := &ast.ExprFunction{
		Type: funcType,
		Name: &ast.ExprBuiltInFunctionName{
			Value: p.peekToken.Value,
			Span:  ast.NewSpanFromToken(p.peekToken),
		},
		Span: ast.NewSpanFromToken(p.peekToken),
	}
	p.nextToken()

	return function, nil
}

func (p *Parser) parseFunctionArgs() (*[]ast.Expression, error) {
	args := []ast.Expression{}
	p.logger.Debug("parsing function args")
	for {
		err := p.expectFunctionArgsStart()
		if err != nil {
			return nil, err
		}

		if token := p.maybeToken(lexer.TLocalVariable); token != nil {
			args = append(args, &ast.ExprLocalVariable{
				Value: token.Value,
				Span:  ast.NewSpanFromToken(*token),
			})
		} else if token := p.maybeToken(lexer.TQuotedIdentifier); token != nil {
			args = append(args, &ast.ExprQuotedIdentifier{
				Value: token.Value,
				Span:  ast.NewSpanFromToken(*token),
			})
		} else if token := p.maybeToken(lexer.TStringLiteral); token != nil {
			args = append(args, &ast.ExprStringLiteral{
				Value: token.Value,
				Span:  ast.NewSpanFromToken(*token),
			})
		} else if token := p.maybeToken(lexer.TNumericLiteral); token != nil {
			args = append(args, &ast.ExprNumberLiteral{
				Value: token.Value,
				Span:  ast.NewSpanFromToken(*token),
			})
		} else if token := p.maybeToken(lexer.TIdentifier); token != nil {
			// check if we have a compound identifier
			identifier := &ast.ExprIdentifier{
				Value: token.Value,
				Span:  ast.NewSpanFromToken(*token),
			}
			compoundIdentifier, err := p.parseCompoundIdentifier(identifier)
			if err != nil {
				return nil, err
			}

			// we have a compoundIdentifier
			if compoundIdentifier != nil {
				endCompoundIdentifierPosition := compoundIdentifier.EndPosition
				if p.peekTokenIs(lexer.TLeftParen) {
					function := &ast.ExprFunction{
						Type: ast.FuncUserDefined,
						Name: compoundIdentifier,
						Span: ast.NewSpanFromLexerPosition(identifier.StartPosition, endCompoundIdentifierPosition),
					}
					functionCall, err := p.parseFunctionCall(function)
					if err != nil {
						return nil, err
					}

					args = append(args, functionCall)
				} else {
					args = append(args, compoundIdentifier)
				}
			} else if p.peekTokenIs(lexer.TLeftParen) {
				function := &ast.ExprFunction{
					Type: ast.FuncUserDefined,
					Name: identifier,
					Span: ast.NewSpanFromLexerPosition(identifier.StartPosition, identifier.EndPosition),
				}

				functionCall, err := p.parseFunctionCall(function)
				if err != nil {
					return nil, err
				}

				args = append(args, functionCall)
			} else {
				args = append(args, identifier)
			}
		} else {
			functionCall, err := p.parseFunctionCall(nil)
			if err != nil {
				return nil, err
			}
			args = append(args, functionCall)
		}

		if token := p.maybeToken(lexer.TRightParen); token == nil {
			break
		}
	}

	return &args, nil
}

func (p *Parser) parseFunctionCall(function *ast.ExprFunction) (*ast.ExprFunctionCall, error) {
	// supposed to be nil if we want to parse a builtin function name
	if function == nil {
		parsedFunction, err := p.parseFunction()
		if err != nil {
			return nil, err
		}
		function = parsedFunction
	}

	// parse function arguments
	_, err := p.consumeToken(lexer.TLeftParen)
	if err != nil {
		return nil, err
	}
	args := []ast.Expression{}
	if !p.peekTokenIs(lexer.TRightParen) {
		functionArgs, err := p.parseFunctionArgs()
		if err != nil {
			return nil, err
		}
		args = *functionArgs
	}

	rightParen, err := p.consumeToken(lexer.TRightParen)
	if err != nil {
		p.logger.Debug("right parenthesis, got ", p.peekToken.Value)
		return nil, err
	}
	// check for over clause
	if !p.peekTokenIs(lexer.TOver) {
		return &ast.ExprFunctionCall{
			Name: function,
			Args: args,
			Span: ast.Span{
				StartPosition: function.StartPosition,
				EndPosition:   rightParen.End,
			},
		}, nil
	}

	overClause, err := p.parseOverClause()
	if err != nil {
		return nil, err
	}

	p.logger.Debug(overClause.TokenLiteral())

	return &ast.ExprFunctionCall{
		Name:       function,
		Args:       args,
		OverClause: overClause,
		Span: ast.Span{
			StartPosition: function.StartPosition,
			EndPosition:   overClause.EndPosition,
		},
	}, nil
}

func (p *Parser) parseBetweenLogicalOperator(left ast.Expression, notKw *ast.Keyword) (*ast.ExprBetweenLogicalOperator, error) {
	betweenKw, err := p.consumeKeyword(lexer.TBetween)
	if err != nil {
		return nil, err
	}

	begin, err := p.parsePrefixExpression()
	if err != nil {
		return nil, err
	}
	p.logger.Debugf("between: begin %s", begin.TokenLiteral())

	andKw, err := p.consumeKeyword(lexer.TAnd)
	if err != nil {
		return nil, err
	}

	end, err := p.parsePrefixExpression()
	if err != nil {
		return nil, err
	}
	p.logger.Debugf("between: end %s", end.TokenLiteral())

	// check if we have and operator
	return &ast.ExprBetweenLogicalOperator{
		BetweenKeyword: *betweenKw,
		TestExpression: left,
		NotKeyword:     notKw,
		Begin:          begin,
		AndKeyword:     *andKw,
		End:            end,
		Span:           ast.NewSpanFromLexerPosition(left.GetSpan().StartPosition, end.GetSpan().EndPosition),
	}, nil
}

func (p *Parser) parseInSubqueryLogicalOperator(left ast.Expression, inKeyword ast.Keyword, notKw *ast.Keyword) (*ast.ExprInSubqueryLogicalOperator, error) {
	statement, err := p.parseSelectSubquery()
	if err != nil {
		return nil, err
	}
	rightParen, err := p.consumeToken(lexer.TRightParen)
	if err != nil {
		return nil, err
	}

	return &ast.ExprInSubqueryLogicalOperator{
		InKeyword:      inKeyword,
		TestExpression: left,
		NotKeyword:     notKw,
		Subquery:       &statement,
		Span:           ast.NewSpanFromLexerPosition(left.GetSpan().StartPosition, rightParen.End),
	}, nil
}

func (p *Parser) parseInExpressionListLogicalOperator(left ast.Expression, inKeyword ast.Keyword, notKw *ast.Keyword) (*ast.ExprInLogicalOperator, error) {
	stmt, err := p.parseExpressionList()
	if err != nil {
		return nil, err
	}
	rightParen, err := p.consumeToken(lexer.TRightParen)
	if err != nil {
		return nil, err
	}

	return &ast.ExprInLogicalOperator{
		InKeyword:      inKeyword,
		TestExpression: left,
		NotKeyword:     notKw,
		Expressions:    stmt.List,
		Span:           ast.NewSpanFromLexerPosition(left.GetSpan().StartPosition, rightParen.End),
	}, nil

}

func (p *Parser) parseInLogicalOperator(left ast.Expression, notKw *ast.Keyword) (ast.Expression, error) {
	inKw, err := p.consumeKeyword(lexer.TIn)
	if err != nil {
		return nil, err
	}
	if _, err := p.consumeToken(lexer.TLeftParen); err != nil {
		return nil, err
	}
	if p.peekTokenIs(lexer.TSelect) {
		inSubquery, err := p.parseInSubqueryLogicalOperator(left, *inKw, notKw)
		if err != nil {
			return nil, err
		}
		return inSubquery, nil
	} else if p.peekTokenIsAny([]lexer.TokenType{
		lexer.TIdentifier,
		lexer.TLocalVariable,
		lexer.TQuotedIdentifier,
		lexer.TStringLiteral,
		lexer.TNumericLiteral,
	}) {
		inExpressionList, err := p.parseInExpressionListLogicalOperator(left, *inKw, notKw)
		if err != nil {
			return nil, err
		}

		return inExpressionList, nil
	}
	return nil, p.peekErrorString("(Subquery or Expression List) after 'IN' keyword")
}
