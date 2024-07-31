package parser

import (
	"SequelGo/internal/ast"
	"SequelGo/internal/lexer"
	"fmt"
	"strconv"
)

func (p *Parser) parseStatement() (ast.Statement, error) {
	switch p.currentToken.Type {
	case lexer.TSelect:
		startPosition := p.currentToken.Start
		body, err := p.parseSelectBody()
		if err != nil {
			return nil, err
		}

		return &ast.SelectStatement{
			Span:       ast.NewSpanFromLexerPosition(startPosition, p.currentToken.End),
			SelectBody: &body,
		}, nil
	case lexer.TWith:
		startPosition := p.currentToken.Start
		select_statement, err := p.parseSelectStatement()

		if err != nil {
			return nil, err
		}

		select_statement.Span = ast.NewSpanFromLexerPosition(startPosition, p.currentToken.End)
		return select_statement, nil
	default:
		return nil, nil
		// return nil, fmt.Errorf("unknown statement type %s", p.currentToken.Value)
	}
}

func (p *Parser) parseSelectStatement() (*ast.SelectStatement, error) {
	p.logger.Debugln("parsing select statement with cte")
	startPositionSelectStatement := p.currentToken.Start
	withKeyword := ast.NewKeywordFromToken(p.currentToken)
	ctes := []ast.CommonTableExpression{}
	for !p.peekTokenIs(lexer.TSelect) {
		if len(ctes) > 0 {
			if err := p.expectPeek(lexer.TComma); err != nil {
				return nil, err
			}
		}
		startPosition := p.currentToken.Start

		// check for expression name
		if err := p.expectPeekMany([]lexer.TokenType{lexer.TIdentifier, lexer.TQuotedIdentifier}); err != nil {
			return nil, err
		}

		cteName := p.currentToken.Value
		var exprList *ast.ExprExpressionList

		if p.peekTokenIs(lexer.TLeftParen) {
			// go to the left paren
			p.nextToken()

			// parse column list
			expressionList, err := p.parseExpressionList()
			if err != nil {
				return nil, err
			}
			exprList = &expressionList

			// go to the right paren
			p.nextToken()
		}

		if err := p.expectPeek(lexer.TAs); err != nil {
			return nil, err
		}
		asKeyword := ast.NewKeywordFromToken(p.currentToken)

		if err := p.expectPeek(lexer.TLeftParen); err != nil {
			return nil, err
		}

		if err := p.expectPeek(lexer.TSelect); err != nil {
			return nil, err
		}

		selectBody, err := p.parseSelectBody()
		if err != nil {
			return nil, err
		}

		if selectBody.OrderByClause != nil && len(selectBody.OrderByClause.Expressions) > 0 && selectBody.Top == nil {
			return nil, p.currentErrorString("Order by is not allowed in cte query unless top clause is specified")
		}

		if err := p.expectPeek(lexer.TRightParen); err != nil {
			return nil, err
		}
		endPosition := p.currentToken.End

		cte := ast.CommonTableExpression{
			Span:      ast.NewSpanFromLexerPosition(startPosition, endPosition),
			Name:      cteName,
			Columns:   exprList,
			AsKeyword: asKeyword,
			Query:     selectBody,
		}
		ctes = append(ctes, cte)
	}

	if err := p.expectPeekMany([]lexer.TokenType{lexer.TSelect}); err != nil {
		return nil, err
	}

	p.logger.Debugln("select body of select statement with cte")
	if p.currentTokenIs(lexer.TSelect) {
		selectBody, err := p.parseSelectBody()
		if err != nil {
			return nil, err
		}
		return &ast.SelectStatement{
			WithKeyword: &withKeyword,
			CTE:         &ctes,
			SelectBody:  &selectBody,
			Span: ast.NewSpanFromLexerPosition(
				startPositionSelectStatement,
				p.currentToken.End,
			),
		}, nil
	}
	return &ast.SelectStatement{}, nil
}

func (p *Parser) parseTopArg() (*ast.TopArg, error) {
	p.nextToken()
	topArg := ast.TopArg{}
	topKw := ast.NewKeywordFromToken(p.currentToken)
	topArg.TopKeyword = topKw
	startPosition := p.currentToken.Start
	p.logger.Debug(p.currentToken)
	p.logger.Debug(p.peekToken)

	if err := p.expectPeek(lexer.TNumericLiteral); err != nil {
		return nil, err
	}
	expr := &ast.ExprNumberLiteral{
		Value: p.currentToken.Value,
		Span:  ast.NewSpanFromLexerPosition(startPosition, p.currentToken.End),
	}
	topArg.Quantity = expr

	if p.peekTokenIs(lexer.TPercent) {
		p.nextToken()
		kw := ast.NewKeywordFromToken(p.currentToken)
		topArg.PercentKeyword = &kw
	}

	if p.peekTokenIs(lexer.TWith) {
		p.nextToken()
		withKw := ast.NewKeywordFromToken(p.currentToken)

		err := p.expectPeek(lexer.TTies)
		if err != nil {
			return nil, err
		}
		tiesKw := ast.NewKeywordFromToken(p.currentToken)
		topArg.WithTiesKeyword = &[2]ast.Keyword{withKw, tiesKw}
	}

	topArg.Span = ast.NewSpanFromLexerPosition(startPosition, p.currentToken.End)
	return &topArg, nil
}

func (p *Parser) parseSelectBody() (ast.SelectBody, error) {
	stmt := ast.SelectBody{}
	stmt.SetLeadingComments(p.popLeadingComments())
	startPositionSelectBody := p.currentToken.Start
	stmt.SelectKeyword = ast.NewKeywordFromToken(p.currentToken)
	if p.peekTokenIs(lexer.TDistinct) {
        leading := p.popLeadingComments()
		p.nextToken()
		kw := ast.NewKeywordFromToken(p.currentToken)
        kw.SetLeadingComments(leading)
        kw.SetTrailingComments(p.popTrailingComments())
		stmt.Distinct = &kw
	}

	// check for optional all keyword
	if p.peekTokenIs(lexer.TAll) {
		p.nextToken()
		kw := ast.NewKeywordFromToken(p.currentToken)
		stmt.AllKeyword = &kw
	}

	if p.peekTokenIs(lexer.TTop) {
		topArg, err := p.parseTopArg()
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

	table, err := p.parseTableArg()
	if err != nil {
		return stmt, err
	}
	stmt.Table = table

	if p.peekTokenIs(lexer.TWhere) {
		whereExpression, err := p.parseWhereExpression()
		if err != nil {
			return stmt, err
		}
		stmt.WhereClause = whereExpression
	}

	if p.peekTokenIs(lexer.TGroup) {
		groupByClause, err := p.parseGroupByClause()
		if err != nil {
			return stmt, err
		}
		stmt.GroupByClause = groupByClause
	}

	if p.peekTokenIs(lexer.THaving) {
		havingExpression, err := p.parseHavingExpression()
		if err != nil {
			return stmt, err
		}
		stmt.HavingClause = havingExpression
	}

	if p.peekTokenIs(lexer.TOrder) {
		orderByClause, err := p.parseOrderByClause()
		if err != nil {
			return stmt, err
		}
		stmt.OrderByClause = orderByClause
	}

	stmt.Span = ast.NewSpanFromLexerPosition(
		startPositionSelectBody,
		p.currentToken.End,
	)

	stmt.SetTrailingComments(p.popTrailingComments())
	return stmt, nil
}

func (p *Parser) parseSelectSubquery() (ast.ExprSubquery, error) {
	stmt := ast.ExprSubquery{}
	startPositionSubquery := p.currentToken.Start
	p.logger.Debug("parsing subquery")

	selectBody, err := p.parseSelectBody()
	if err != nil {
		return stmt, err
	}
	stmt.SelectBody = selectBody
	stmt.Span = ast.NewSpanFromLexerPosition(
		startPositionSubquery,
		p.currentToken.End,
	)

	return stmt, nil
}

func (p *Parser) parseSelectItems() (*ast.SelectItems, error) {
	selectItems := ast.SelectItems{}
	selectItems.SetLeadingComments(p.popLeadingComments())
	startPositionSelectItems := p.currentToken.Start
	p.logger.Debug(p.currentToken)
	p.logger.Debug(p.peekToken)
	for {
		startPosition := p.currentToken.Start

		err := p.expectPeekMany(append([]lexer.TokenType{lexer.TIdentifier,
			lexer.TNumericLiteral,
			lexer.TStringLiteral,
			lexer.TAsterisk,
			lexer.TLocalVariable,
			lexer.TLeftParen,
			lexer.TMinus,
			lexer.TPlus,
			// rework checking keywords
			lexer.TSum,
			lexer.TQuotedIdentifier}, ast.BuiltinFunctionsTokenType...))
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
				return nil, p.currentErrorString("Subquery must contain only one column")
			}
			if v.GroupByClause != nil && len(v.GroupByClause.Items) > 1 && v.Distinct != nil {
				return nil, p.currentErrorString("The 'DISTINCT' keyword can't be used with subqueries that include 'GROUP BY'")
			}
			if v.OrderByClause != nil && len(v.OrderByClause.Expressions) > 1 && v.Top == nil {
				return nil, p.currentErrorString("'ORDER BY' can only be specified when 'TOP' is also specified")
			}
			break
		}
		if (p.peekToken.Type == lexer.TAs ||
			p.peekToken.Type == lexer.TIdentifier ||
			p.peekToken.Type == lexer.TStringLiteral ||
			p.peekToken.Type == lexer.TQuotedIdentifier) && !p.peekToken2IsAny(ast.DataTypeTokenTypes) {
			exprAlias := &ast.ExprWithAlias{Expression: expr}

			if p.peekToken.Type == lexer.TAs {
				p.nextToken()
				kw := ast.NewKeywordFromToken(p.currentToken)
				exprAlias.AsKeyword = &kw
			}

			// needed in case we just parsed AS keyword
			err := p.expectPeekMany([]lexer.TokenType{lexer.TIdentifier, lexer.TStringLiteral, lexer.TQuotedIdentifier})
			if err != nil {
				return nil, err
			}

			alias, err := p.parseExpression(PrecedenceLowest)
			if err != nil {
				return nil, err
			}

			switch alias.(type) {
			case *ast.ExprIdentifier, *ast.ExprStringLiteral, *ast.ExprQuotedIdentifier:
				break
			default:
				err = fmt.Errorf("Expected (Identifier or StringLiteral or QuotedIdentifier) for Alias")
				return nil, err
			}
			exprAlias.Alias = alias
			exprAlias.Span = ast.NewSpanFromLexerPosition(startPosition, p.currentToken.End)
			exprAlias.SetTrailingComments(p.popTrailingComments())
			selectItems.Items = append(selectItems.Items, exprAlias)
		} else {
			selectItems.Items = append(selectItems.Items, expr)
		}

		if p.peekToken.Type != lexer.TComma {
			break
		}

		p.nextToken()
	}

	selectItems.SetSpan(ast.NewSpanFromLexerPosition(startPositionSelectItems, p.currentToken.End))
	selectItems.SetTrailingComments(p.popTrailingComments())
	return &selectItems, nil
}

func (p *Parser) parseTableArg() (*ast.TableArg, error) {
	err := p.expectPeek(lexer.TFrom)
	if err != nil {
		p.logger.Debug("from err")
		return nil, err
	}
	fromKeyword := ast.NewKeywordFromToken(p.currentToken)
	startPosition := p.currentToken.Start
	p.logger.Debug("parsing table arg")
	p.logger.Debug(p.currentToken)

	tableSource, err := p.parseTableSource()
	if err != nil {
		return nil, err
	}

	if !p.peekTokenIs(lexer.TInner) ||
		p.peekTokenIs(lexer.TLeft) ||
		p.peekTokenIs(lexer.TRight) ||
		p.peekTokenIs(lexer.TFull) {
		return &ast.TableArg{
			FromKeyword: fromKeyword,
			Table:       tableSource,
			Span:        ast.NewSpanFromLexerPosition(startPosition, p.currentToken.End),
		}, nil
	}

	joins, err := p.parseJoins()
	if err != nil {
		return nil, err
	}

	return &ast.TableArg{
		FromKeyword: fromKeyword,
		Table:       tableSource,
		Joins:       joins,
		Span:        ast.NewSpanFromLexerPosition(startPosition, p.currentToken.End),
	}, nil
}

func (p *Parser) parseTableSource() (*ast.TableSource, error) {
	startPosition := p.currentToken.Start
	err := p.expectPeekMany([]lexer.TokenType{lexer.TIdentifier, lexer.TLocalVariable, lexer.TLeftParen})
	if err != nil {
		return nil, err
	}

	source, err := p.parseExpression(PrecedenceLowest)
	if err != nil {
		return nil, err
	}
	baseNode := ast.NewSpanFromLexerPosition(startPosition, p.currentToken.End)
	switch v := source.(type) {
	case *ast.ExprIdentifier, *ast.ExprCompoundIdentifier, *ast.ExprLocalVariable:
		return &ast.TableSource{
			Type:   ast.TSTTable,
			Source: source,
			Span:   baseNode,
		}, err
	case *ast.ExprFunctionCall:
		return &ast.TableSource{
			Type:   ast.TSTTableValuedFunction,
			Source: source,
			Span:   baseNode,
		}, err
	case *ast.ExprSubquery:
		return &ast.TableSource{
			Type:   ast.TSTDerived,
			Source: source,
			Span:   baseNode,
		}, err
	case *ast.ExprWithAlias:
		var tableType ast.TableSourceType
		switch v.Expression.(type) {
		case *ast.ExprIdentifier, *ast.ExprCompoundIdentifier, *ast.ExprLocalVariable:
			tableType = ast.TSTTable
			break
		case *ast.ExprFunctionCall:
			tableType = ast.TSTTableValuedFunction
			break
		case *ast.ExprSubquery:
			tableType = ast.TSTDerived
			break
		}
		return &ast.TableSource{
			Type:   tableType,
			Source: v,
			Span:   baseNode,
		}, err
	default:
		return nil, p.currentErrorString("expected Table Name or Function or Subquery")
	}

}

func (p *Parser) parseJoins() ([]ast.Join, error) {
	joins := []ast.Join{}

	for {
		startPosition := p.currentToken.Start
		var joinType ast.JoinType
		var joinTypeKeyword []ast.Keyword
		var joinKeyword ast.Keyword
		if p.peekTokenIs(lexer.TInner) {
			p.nextToken()
			joinTypeKeyword = append(joinTypeKeyword, ast.NewKeywordFromToken(p.currentToken))
			if err := p.expectPeek(lexer.TJoin); err != nil {
				return nil, err
			}
			joinType = ast.JTInner
			joinKeyword = ast.NewKeywordFromToken(p.currentToken)
		} else if p.peekTokenIs(lexer.TLeft) {
			p.nextToken()
			joinTypeKeyword = append(joinTypeKeyword, ast.NewKeywordFromToken(p.currentToken))
			if p.peekTokenIs(lexer.TOuter) {
				p.nextToken()
				joinType = ast.JTLeftOuter
				joinTypeKeyword = append(joinTypeKeyword, ast.NewKeywordFromToken(p.currentToken))
			} else if p.peekTokenIs(lexer.TJoin) {
				joinType = ast.JTLeft
			}

			if err := p.expectPeek(lexer.TJoin); err != nil {
				return nil, err
			}
			joinKeyword = ast.NewKeywordFromToken(p.currentToken)
		} else if p.peekTokenIs(lexer.TRight) {
			p.nextToken()
			joinTypeKeyword = append(joinTypeKeyword, ast.NewKeywordFromToken(p.currentToken))
			if p.peekTokenIs(lexer.TOuter) {
				p.nextToken()
				joinType = ast.JTRightOuter
				joinTypeKeyword = append(joinTypeKeyword, ast.NewKeywordFromToken(p.currentToken))
			} else if p.peekTokenIs(lexer.TJoin) {
				joinType = ast.JTRight
			}

			if err := p.expectPeek(lexer.TJoin); err != nil {
				return nil, err
			}
			joinKeyword = ast.NewKeywordFromToken(p.currentToken)
		} else if p.peekTokenIs(lexer.TFull) {
			p.nextToken()
			joinTypeKeyword = append(joinTypeKeyword, ast.NewKeywordFromToken(p.currentToken))
			if p.peekTokenIs(lexer.TOuter) {
				p.nextToken()
				joinType = ast.JTFullOuter
				joinTypeKeyword = append(joinTypeKeyword, ast.NewKeywordFromToken(p.currentToken))
			} else if p.peekTokenIs(lexer.TJoin) {
				joinType = ast.JTFull
			}

			if err := p.expectPeek(lexer.TJoin); err != nil {
				return nil, err
			}
			joinKeyword = ast.NewKeywordFromToken(p.currentToken)
		} else {
			break
		}

		tableSource, err := p.parseTableSource()
		if err != nil {
			return nil, err
		}

		if err := p.expectPeek(lexer.TOn); err != nil {
			return nil, err
		}
		onKeyword := ast.NewKeywordFromToken(p.currentToken)
		p.nextToken()

		searchCondition, err := p.parseExpression(PrecedenceLowest)
		if err != nil {
			return nil, err
		}

		joins = append(joins, ast.Join{
			JoinTypeKeyword: joinTypeKeyword,
			JoinKeyword:     joinKeyword,
			Type:            joinType,
			Table:           tableSource,
			OnKeyword:       &onKeyword,
			Condition:       searchCondition,
			Span:            ast.NewSpanFromLexerPosition(startPosition, p.currentToken.End),
		})
	}

	return joins, nil
}

func (p *Parser) parseWhereExpression() (*ast.WhereClause, error) {
	whereClause := ast.WhereClause{}
	startPosition := p.currentToken.Start
	// go to where token
	p.nextToken()
	whereClause.WhereKeyword = ast.NewKeywordFromToken(p.currentToken)
	p.logger.Debug("parsing where")

	p.nextToken()
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
		p.errorToken = ETCurrent
		return nil, p.currentErrorString("expected expression after 'WHERE' keyword")
	}
	p.logger.Debugf("expr: %s\n", expr)
	whereClause.Clause = expr
	whereClause.Clause.SetSpan(ast.NewSpanFromLexerPosition(startPosition, p.currentToken.End))
	return &whereClause, nil
}

func (p *Parser) parseGroupByClause() (*ast.GroupByClause, error) {
	p.nextToken()
	groupByClause := ast.GroupByClause{}
	startPosition := p.currentToken.Start
	items := []ast.Expression{}
	groupKw := ast.NewKeywordFromToken(p.currentToken)
	err := p.expectPeek(lexer.TBy)
	if err != nil {
		return nil, err
	}
	byKw := ast.NewKeywordFromToken(p.currentToken)
	groupByClause.GroupByKeyword = [2]ast.Keyword{groupKw, byKw}

	p.logger.Debug("parsing group by clause")

	for {
		err := p.expectPeekMany([]lexer.TokenType{
			lexer.TIdentifier,
			lexer.TNumericLiteral,
			lexer.TLocalVariable,
			lexer.TQuotedIdentifier,
		})
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
	groupByClause.SetSpan(ast.NewSpanFromLexerPosition(startPosition, p.currentToken.End))
	return &groupByClause, nil
}

func (p *Parser) parseHavingExpression() (*ast.HavingClause, error) {
	p.nextToken()
	havingClause := ast.HavingClause{HavingKeyword: ast.NewKeywordFromToken(p.currentToken)}
	startPosition := p.currentToken.Start
	p.logger.Debug("parsing having")

	p.nextToken()
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
		return nil, p.currentErrorString("expected expression after 'HAVING' keyword")
	}
	havingClause.Clause = expr
	havingClause.Clause.SetSpan(ast.NewSpanFromLexerPosition(startPosition, p.currentToken.End))
	return &havingClause, nil
}

func (p *Parser) parseOrderByClause() (*ast.OrderByClause, error) {
	p.nextToken()
	orderKeyword := ast.NewKeywordFromToken(p.currentToken)
	startPosition := p.currentToken.Start
	err := p.expectPeek(lexer.TBy)
	if err != nil {
		return nil, err
	}
	byKeyword := ast.NewKeywordFromToken(p.currentToken)
	p.logger.Debug("parsing order by clause")
	args, err := p.parseOrderByArgs()
	if err != nil {
		return nil, err
	}
	orderByClause := &ast.OrderByClause{
		OrderByKeyword: [2]ast.Keyword{orderKeyword, byKeyword},
		Expressions:    args,
		Span:           ast.NewSpanFromLexerPosition(startPosition, p.currentToken.End),
	}

	if !p.peekTokenIs(lexer.TOffset) {
		return orderByClause, nil
	}

	offsetFetchClause, err := p.parseOffsetFetchClause()
	if err != nil {
		return nil, err
	}

	orderByClause.OffsetFetch = offsetFetchClause
	orderByClause.Span = ast.NewSpanFromLexerPosition(startPosition, p.currentToken.End)

	return orderByClause, nil
}

func (p *Parser) parseOrderByArgs() ([]ast.OrderByArg, error) {
	items := []ast.OrderByArg{}

	for {
		startPosition := p.currentToken.Start
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
		if p.peekTokenIs(lexer.TAsc) {
			p.nextToken()
			orderKeyword := ast.NewKeywordFromToken(p.currentToken)
			items = append(items, ast.OrderByArg{
				Column:       expr,
				Type:         ast.OBAsc,
				OrderKeyword: &orderKeyword,
				Span:         ast.NewSpanFromLexerPosition(startPosition, p.currentToken.End),
			})
		} else if p.peekTokenIs(lexer.TDesc) {
			p.nextToken()
			orderKeyword := ast.NewKeywordFromToken(p.currentToken)
			items = append(items, ast.OrderByArg{
				Column:       expr,
				Type:         ast.OBDesc,
				OrderKeyword: &orderKeyword,
				Span:         ast.NewSpanFromLexerPosition(startPosition, p.currentToken.End),
			})
		} else {
			items = append(items, ast.OrderByArg{
				Column: expr,
				Type:   ast.OBNone,
				Span:   ast.NewSpanFromLexerPosition(startPosition, p.currentToken.End),
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
	startPosition := p.currentToken.Start
	p.nextToken()

	offset, err := p.parseOffset()
	if err != nil {
		return nil, err
	}
	offsetFetchClause := ast.OffsetFetchClause{
		Offset: offset,
		Span:   ast.NewSpanFromLexerPosition(startPosition, p.currentToken.End),
	}

	if !p.peekTokenIs(lexer.TFetch) {
		return &offsetFetchClause, nil
	}

	p.nextToken()
	fetch, err := p.parseFetch()
	if err != nil {
		return nil, err
	}

	offsetFetchClause.Fetch = &fetch
	offsetFetchClause.Span = ast.NewSpanFromLexerPosition(startPosition, p.currentToken.End)

	return &offsetFetchClause, nil
}

func (p *Parser) parseOffset() (ast.OffsetArg, error) {
	startPosition := p.currentToken.Start
	offsetKeyword := ast.NewKeywordFromToken(p.currentToken)
	p.nextToken()
	offsetArg := ast.OffsetArg{OffsetKeyword: offsetKeyword}
	offset, err := p.parseExpression(PrecedenceLowest)
	if err != nil {
		return offsetArg, err
	}

	err = p.expectPeekMany([]lexer.TokenType{lexer.TRow, lexer.TRows})
	if err != nil {
		return offsetArg, err
	}

	offsetArg.Value = offset
	offsetArg.Span = ast.NewSpanFromLexerPosition(startPosition, p.currentToken.End)
	offsetArg.RowOrRowsKeyword = ast.NewKeywordFromToken(p.currentToken)
	switch p.currentToken.Type {
	case lexer.TRow:
		offsetArg.RowOrRows = ast.RRRow
		return offsetArg, nil
	case lexer.TRows:
		offsetArg.RowOrRows = ast.RRRows
		return offsetArg, nil
	default:
		return offsetArg, p.currentErrorString("Expected 'ROW' or 'ROWS' after offset expression")
	}
}

func (p *Parser) parseFetch() (ast.FetchArg, error) {
	startPosition := p.currentToken.Start
	fetchArg := ast.FetchArg{FetchKeyword: ast.NewKeywordFromToken(p.currentToken)}
	if err := p.expectPeekMany([]lexer.TokenType{lexer.TFirst, lexer.TNext}); err != nil {
		return fetchArg, err
	}

	fetchArg.NextOrFirstKeyword = ast.NewKeywordFromToken(p.currentToken)
	switch p.currentToken.Type {
	case lexer.TFirst:
		fetchArg.NextOrFirst = ast.NFFirst
		break
	case lexer.TNext:
		fetchArg.NextOrFirst = ast.NFNext
		break
	}

	p.nextToken()
	fetch, err := p.parseExpression(PrecedenceLowest)
	if err != nil {
		return fetchArg, err
	}

	fetchArg.Value = fetch
	err = p.expectPeekMany([]lexer.TokenType{lexer.TRow, lexer.TRows})
	if err != nil {
		return fetchArg, err
	}

	fetchArg.RowOrRowsKeyword = ast.NewKeywordFromToken(p.currentToken)
	switch p.currentToken.Type {
	case lexer.TRow:
		fetchArg.RowOrRows = ast.RRRow
		break
	case lexer.TRows:
		fetchArg.RowOrRows = ast.RRRows
		break
	}

	if err = p.expectPeek(lexer.TOnly); err != nil {
		return fetchArg, err
	}
	fetchArg.OnlyKeyword = ast.NewKeywordFromToken(p.currentToken)
	fetchArg.Span = ast.NewSpanFromLexerPosition(startPosition, p.currentToken.End)

	return fetchArg, nil
}

func (p *Parser) parseOverClause() (*ast.FunctionOverClause, error) {
	p.nextToken()
	startPosition := p.currentToken.Start
	overKeyword := ast.NewKeywordFromToken(p.currentToken)
	if err := p.expectPeek(lexer.TLeftParen); err != nil {
		return nil, err
	}

	functionOverClause := ast.FunctionOverClause{OverKeyword: overKeyword}

	if p.peekTokenIs(lexer.TPartition) {
		p.nextToken()
		partitionKeyword := ast.NewKeywordFromToken(p.currentToken)
		if err := p.expectPeek(lexer.TBy); err != nil {
			return nil, err
		}
		pbyKeyword := ast.NewKeywordFromToken(p.currentToken)
		expressions, err := p.parsePartitionClause()
		if err != nil {
			return nil, err
		}
		functionOverClause.PartitionByClause = expressions
		functionOverClause.PartitionByKeyword = &[2]ast.Keyword{partitionKeyword, pbyKeyword}
	}

	if p.peekTokenIs(lexer.TOrder) {
		p.nextToken()
		orderKeyword := ast.NewKeywordFromToken(p.currentToken)
		if err := p.expectPeek(lexer.TBy); err != nil {
			return nil, err
		}
		obyKeyword := ast.NewKeywordFromToken(p.currentToken)
		args, err := p.parseOrderByArgs()
		if err != nil {
			return nil, err
		}
		functionOverClause.OrderByClause = args
		functionOverClause.OrderByKeyword = &[2]ast.Keyword{orderKeyword, obyKeyword}
	}

	if p.peekTokenIs(lexer.TRows) || p.peekTokenIs(lexer.TRange) {
		clause, err := p.parseWindowFrameClause()
		if err != nil {
			return nil, err
		}
		functionOverClause.WindowFrameClause = clause
	}

	err := p.expectPeek(lexer.TRightParen)
	if err != nil {
		return nil, err
	}
	functionOverClause.Span = ast.NewSpanFromLexerPosition(startPosition, p.currentToken.End)

	return &functionOverClause, nil
}

func (p *Parser) parsePartitionClause() ([]ast.Expression, error) {
	args := []ast.Expression{}

	for {
		if !p.peekTokenIs(lexer.TIdentifier) && !p.peekTokenIs(lexer.TQuotedIdentifier) {
			break
		}
		p.nextToken()

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
		return nil, p.currentErrorString("PARTITION BY items in PARTITION BY expression")
	}

	return args, nil
}

func (p *Parser) parseWindowFrameClause() (*ast.WindowFrameClause, error) {
	startPosition := p.currentToken.Start
	var rowsOrRangeType ast.RowsOrRangeType
	if p.peekTokenIs(lexer.TRows) {
		rowsOrRangeType = ast.RRTRows
	} else if p.peekTokenIs(lexer.TRange) {
		rowsOrRangeType = ast.RRTRange
	}
	p.nextToken()
	rowsOrRangeKeyword := ast.NewKeywordFromToken(p.currentToken)
	var betweenKeyword *ast.Keyword

	var windowFrameStart ast.WindowFrameBound
	var windowFrameEnd ast.WindowFrameBound
	followingNeeded := false
	// parse between
	p.logger.Debug("p.currentToken.Value: ", p.currentToken.Value)
	p.logger.Debug("p.peekToken.Value: ", p.peekToken.Value)
	if p.peekTokenIs(lexer.TBetween) {
		followingNeeded = true
		p.nextToken()
		betweenKeywordTemp := ast.NewKeywordFromToken(p.currentToken)
		betweenKeyword = &betweenKeywordTemp
	}
	p.logger.Debug("p.currentToken.Value: ", p.currentToken.Value)
	p.logger.Debug("p.peekToken.Value: ", p.peekToken.Value)
	var boundKeywordStart []ast.Keyword
	if p.peekTokenIs(lexer.TUnbounded) {
		p.nextToken()
		boundKeywordStart = append(boundKeywordStart, ast.NewKeywordFromToken(p.currentToken))

		if err := p.expectPeek(lexer.TPreceding); err != nil {
			return nil, err
		}
		boundKeywordStart = append(boundKeywordStart, ast.NewKeywordFromToken(p.currentToken))
		windowFrameStart = ast.WindowFrameBound{
			BoundKeyword: boundKeywordStart,
			Type:         ast.WFBTUnboundedPreceding,
			Span:         ast.NewSpanFromLexerPosition(startPosition, p.currentToken.End),
		}
	} else if p.peekTokenIs(lexer.TCurrent) {
		p.nextToken()
		boundKeywordStart = append(boundKeywordStart, ast.NewKeywordFromToken(p.currentToken))
		if err := p.expectPeek(lexer.TRow); err != nil {
			return nil, err
		}
		boundKeywordStart = append(boundKeywordStart, ast.NewKeywordFromToken(p.currentToken))
		windowFrameStart = ast.WindowFrameBound{
			BoundKeyword: boundKeywordStart,
			Type:         ast.WFBTCurrentRow,
			Span:         ast.NewSpanFromLexerPosition(startPosition, p.currentToken.End),
		}
	} else if p.peekTokenIs(lexer.TNumericLiteral) {
		p.nextToken()
		expr, err := p.parseExpression(PrecedenceLowest)
		p.logger.Debug("test")
		if err != nil {
			return nil, err
		}
		if err := p.expectPeek(lexer.TPreceding); err != nil {
			return nil, err
		}
		boundKeywordStart = append(boundKeywordStart, ast.NewKeywordFromToken(p.currentToken))
		windowFrameStart = ast.WindowFrameBound{
			BoundKeyword: boundKeywordStart,
			Type:         ast.WFBTPreceding,
			Expression:   expr,
			Span:         ast.NewSpanFromLexerPosition(startPosition, p.currentToken.End),
		}
	} else {
		return nil, p.currentErrorString("Expected UNBOUNDED PRECEDING or CURRENT ROW or <NUMBER> PRECEDING")
	}

	if !followingNeeded {
		return &ast.WindowFrameClause{
			RowsOrRangeKeyword: rowsOrRangeKeyword,
			RowsOrRange:        rowsOrRangeType,
			Start:              &windowFrameStart,
			BetweenKeyword:     betweenKeyword,
			Span:               ast.NewSpanFromLexerPosition(startPosition, p.currentToken.End),
		}, nil
	}

	startPositionFrameEnd := p.currentToken.Start
	if err := p.expectPeek(lexer.TAnd); err != nil {
		return nil, err
	}
	andKeyword := ast.NewKeywordFromToken(p.currentToken)

	var boundKeywordEnd []ast.Keyword
	if p.peekTokenIs(lexer.TUnbounded) {
		p.nextToken()
		boundKeywordEnd = append(boundKeywordEnd, ast.NewKeywordFromToken(p.currentToken))
		if err := p.expectPeek(lexer.TFollowing); err != nil {
			return nil, err
		}
		boundKeywordEnd = append(boundKeywordEnd, ast.NewKeywordFromToken(p.currentToken))
		windowFrameEnd = ast.WindowFrameBound{
			Type:         ast.WFBTUnboundedFollowing,
			BoundKeyword: boundKeywordEnd,
			Span:         ast.NewSpanFromLexerPosition(startPositionFrameEnd, p.currentToken.End),
		}
	} else if p.peekTokenIs(lexer.TCurrent) {
		p.nextToken()
		boundKeywordEnd = append(boundKeywordEnd, ast.NewKeywordFromToken(p.currentToken))
		if err := p.expectPeek(lexer.TRow); err != nil {
			return nil, err
		}
		boundKeywordEnd = append(boundKeywordEnd, ast.NewKeywordFromToken(p.currentToken))
		windowFrameEnd = ast.WindowFrameBound{
			BoundKeyword: boundKeywordEnd,
			Type:         ast.WFBTCurrentRow,
			Span:         ast.NewSpanFromLexerPosition(startPositionFrameEnd, p.currentToken.End),
		}
	} else if p.peekTokenIs(lexer.TNumericLiteral) {
		p.nextToken()
		expr, err := p.parseExpression(PrecedenceLowest)
		if err != nil {
			return nil, err
		}
		if err := p.expectPeek(lexer.TFollowing); err != nil {
			return nil, err
		}
		boundKeywordEnd = append(boundKeywordEnd, ast.NewKeywordFromToken(p.currentToken))
		windowFrameEnd = ast.WindowFrameBound{
			Type:         ast.WFBTFollowing,
			BoundKeyword: boundKeywordEnd,
			Expression:   expr,
			Span:         ast.NewSpanFromLexerPosition(startPositionFrameEnd, p.currentToken.End),
		}
	} else {
		return nil, p.currentErrorString("Expected UNBOUNDED FOLLOWING or CURRENT ROW or <NUMBER> FOLLOWING")
	}

	return &ast.WindowFrameClause{
		RowsOrRangeKeyword: rowsOrRangeKeyword,
		RowsOrRange:        rowsOrRangeType,
		Start:              &windowFrameStart,
		BetweenKeyword:     betweenKeyword,
		AndKeyword:         &andKeyword,
		End:                &windowFrameEnd,
		Span:               ast.NewSpanFromLexerPosition(startPosition, p.currentToken.End),
	}, nil
}

func (p *Parser) parseExpressionList() (ast.ExprExpressionList, error) {
	startPosition := p.currentToken.Start
	expressionList := ast.ExprExpressionList{}

	for {
		err := p.expectPeekMany([]lexer.TokenType{lexer.TIdentifier,
			lexer.TQuotedIdentifier,
			lexer.TNumericLiteral,
			lexer.TStringLiteral,
			lexer.TLocalVariable})
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

	expressionList.Span = ast.NewSpanFromLexerPosition(startPosition, p.currentToken.End)
	return expressionList, nil
}

func (p *Parser) parseNumericSize() (*ast.NumericSize, error) {
	startPosition := p.currentToken.Start
	if err := p.expectPeek(lexer.TLeftParen); err != nil {
		return nil, err
	}

	if err := p.expectPeek(lexer.TNumericLiteral); err != nil {
		return nil, p.currentErrorString("Expected a numeric literal for casting expression")
	}

	// parse precision
	precision, err := strconv.ParseUint(p.currentToken.Value, 10, 32)
	if err != nil {
		return nil, p.currentErrorString("could not convert numeric literal to uint32")
	}
	precision32 := uint32(precision)
	if p.peekTokenIs(lexer.TRightParen) {
		p.nextToken()
		return &ast.NumericSize{
			Precision: precision32,
			Span:      ast.NewSpanFromLexerPosition(startPosition, p.currentToken.End),
		}, nil
	}

	// parse scale
	if err := p.expectPeek(lexer.TComma); err != nil {
		return nil, p.currentErrorString("Expected a comma before scale")
	}

	p.nextToken()

	if err := p.expectPeek(lexer.TNumericLiteral); err != nil {
		return nil, p.currentErrorString("Expected a numeric literal for scale when casting expression")
	}

	scale, err := strconv.ParseUint(p.currentToken.Value, 10, 32)
	if err != nil {
		return nil, p.currentErrorString("could not convert numeric literal to uint32")
	}
	scale32 := uint32(scale)

	if err := p.expectPeek(lexer.TRightParen); err != nil {
		return nil, p.currentErrorString("Expected a right parenthesis after scale")
	}

	return &ast.NumericSize{
		Precision: precision32,
		Scale:     &scale32,
		Span:      ast.NewSpanFromLexerPosition(startPosition, p.currentToken.End),
	}, nil
}

func (p *Parser) parseDataType() (*ast.DataType, error) {
	startPosition := p.currentToken.Start
	if err := p.expectPeekMany(ast.DataTypeTokenTypes); err != nil {
		return nil, err
	}

	var dataType ast.DataType
	switch p.currentToken.Type {
	case lexer.TInt:
		dataType = ast.DataType{Kind: ast.DTInt}
		break
	case lexer.TBigint:
		dataType = ast.DataType{Kind: ast.DTBigInt}
		break
	case lexer.TTinyint:
		dataType = ast.DataType{Kind: ast.DTTinyInt}
		break
	case lexer.TSmallint:
		dataType = ast.DataType{Kind: ast.DTSmallInt}
		break
	case lexer.TBit:
		dataType = ast.DataType{Kind: ast.DTBit}
		break
	case lexer.TFloat:
		dataType = ast.DataType{Kind: ast.DTFloat}
		if !p.peekTokenIs(lexer.TLeftParen) {
			break
		}

		if err := p.expectPeek(lexer.TLeftParen); err != nil {
			return nil, err
		}
		if err := p.expectPeek(lexer.TNumericLiteral); err != nil {
			return nil, p.currentErrorString("Expected a numeric literal for casting expression")
		}
		size, err := strconv.ParseUint(p.currentToken.Value, 10, 32)
		if err != nil {
			return nil, p.currentErrorString("could not convert numeric literal to uint32")
		}
		size32 := uint32(size)
		dataType.FloatPrecision = &size32
		if err := p.expectPeek(lexer.TRightParen); err != nil {
			return nil, p.currentErrorString("Expected a numeric literal for casting expression")
		}
		break
	case lexer.TReal:
		dataType = ast.DataType{Kind: ast.DTReal}
		break
	case lexer.TDate:
		dataType = ast.DataType{Kind: ast.DTDate}
		break
	case lexer.TDatetime:
		dataType = ast.DataType{Kind: ast.DTDatetime}
		break
	case lexer.TTime:
		dataType = ast.DataType{Kind: ast.DTTime}
		break
	case lexer.TDecimal:
		dataType = ast.DataType{Kind: ast.DTDecimal}
		if !p.peekTokenIs(lexer.TLeftParen) {
			break
		}
		numericSize, err := p.parseNumericSize()
		if err != nil {
			return nil, err
		}
		dataType.DecimalNumericSize = numericSize
		break
	case lexer.TNumeric:
		dataType = ast.DataType{Kind: ast.DTNumeric}
		if !p.peekTokenIs(lexer.TLeftParen) {
			break
		}
		numericSize, err := p.parseNumericSize()
		if err != nil {
			return nil, err
		}
		dataType.DecimalNumericSize = numericSize
		break
	case lexer.TVarchar:
		dataType = ast.DataType{Kind: ast.DTVarchar}
		if !p.peekTokenIs(lexer.TLeftParen) {
			break
		}

		if err := p.expectPeek(lexer.TLeftParen); err != nil {
			return nil, err
		}
		if err := p.expectPeek(lexer.TNumericLiteral); err != nil {
			return nil, p.currentErrorString("Expected a numeric literal for casting expression")
		}
		size, err := strconv.ParseUint(p.currentToken.Value, 10, 32)
		if err != nil {
			return nil, p.currentErrorString("could not convert numeric literal to uint32")
		}
		size32 := uint32(size)
		dataType.FloatPrecision = &size32
		if err := p.expectPeek(lexer.TRightParen); err != nil {
			return nil, p.currentErrorString("Expected a numeric literal for casting expression")
		}
		break
	default:
		return nil, p.currentErrorString("Expected a Builtin Datatype")
	}

	dataType.Span = ast.NewSpanFromLexerPosition(startPosition, p.currentToken.End)
	return &dataType, nil
}

func (p *Parser) parseCast() (*ast.ExprCast, error) {
	startPosition := p.currentToken.Start
	castKeyword := ast.NewKeywordFromToken(p.currentToken)
	if err := p.expectPeek(lexer.TLeftParen); err != nil {
		return nil, err
	}
	p.logger.Debug("parsing cast expression")
	p.nextToken()

	expr, err := p.parseExpression(PrecedenceLowest)
	if err != nil {
		return nil, err
	}

	p.logger.Debug(expr.TokenLiteral())
	if err := p.expectPeek(lexer.TAs); err != nil {
		return nil, err
	}
	asKeyword := ast.NewKeywordFromToken(p.currentToken)

	dt, err := p.parseDataType()
	if err != nil {
		return nil, err
	}
	p.logger.Debug(dt.TokenLiteral())
	p.logger.Debug(p.currentToken)
	p.logger.Debug(p.peekToken)
	if err := p.expectPeek(lexer.TRightParen); err != nil {
		return nil, err
	}

	return &ast.ExprCast{
		CastKeyword: castKeyword,
		Expression:  expr,
		AsKeyword:   asKeyword,
		DataType:    *dt,
		Span:        ast.NewSpanFromLexerPosition(startPosition, p.currentToken.End),
	}, nil
}

func (p *Parser) parseCompoundIdentifier(expr ast.Expression) (*ast.ExprCompoundIdentifier, error) {
	if p.peekToken.Type == lexer.TPeriod {
		// we are dealing with a qualified identifier
		startPositionCompound := p.currentToken.Start
		compound := &[]ast.Expression{expr}
		p.logger.Debug("parsing compound identifier")

		// go to period token
		p.nextToken()
		p.logger.Debug("current token: ", p.currentToken)

		for {
			startPosition := p.currentToken.Start
			err := p.expectPeekMany([]lexer.TokenType{lexer.TIdentifier, lexer.TQuotedIdentifier, lexer.TAsterisk})
			if err != nil {
				return nil, err
			}
			p.logger.Debug("current token: ", p.currentToken)

			if p.currentToken.Type == lexer.TAsterisk {
				expr := &ast.ExprStar{
					Span: ast.NewSpanFromLexerPosition(startPosition, p.currentToken.End),
				}
				*compound = append(*compound, expr)
				break
			} else if p.currentToken.Type == lexer.TQuotedIdentifier {
				expr := &ast.ExprQuotedIdentifier{
					Value: p.currentToken.Value,
					Span:  ast.NewSpanFromLexerPosition(startPosition, p.currentToken.End),
				}
				*compound = append(*compound, expr)
			} else {
				expr := &ast.ExprIdentifier{
					Value: p.currentToken.Value,
					Span:  ast.NewSpanFromLexerPosition(startPosition, p.currentToken.End),
				}
				*compound = append(*compound, expr)
			}

			if p.peekToken.Type != lexer.TPeriod {
				break
			}

			p.nextToken()
		}

		return &ast.ExprCompoundIdentifier{
			Identifiers: *compound,
			Span:        ast.NewSpanFromLexerPosition(startPositionCompound, p.currentToken.End),
		}, nil
	}

	return nil, nil
}

func (p *Parser) parseFunction() (*ast.ExprFunction, error) {
	var funcType ast.FuncType
	switch p.currentToken.Type {
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
		Name: &ast.ExprIdentifier{Value: p.currentToken.Value},
		Span: ast.NewSpanFromLexerPosition(p.currentToken.Start, p.currentToken.End),
	}

	return function, nil
}

func (p *Parser) parseFunctionArgs() (*[]ast.Expression, error) {
	args := []ast.Expression{}
	p.logger.Debug("parsing function args")
	for {
		startPosition := p.currentToken.Start
		err := p.expectPeekMany(append([]lexer.TokenType{
			lexer.TIdentifier,
			lexer.TNumericLiteral,
			lexer.TStringLiteral,
			lexer.TLocalVariable,
			lexer.TQuotedIdentifier,
		}, ast.BuiltinFunctionsTokenType...))
		if err != nil {
			return nil, err
		}

		if p.currentToken.Type == lexer.TLocalVariable {
			args = append(args, &ast.ExprLocalVariable{
				Value: p.currentToken.Value,
				Span:  ast.NewSpanFromLexerPosition(startPosition, p.currentToken.End),
			})
		} else if p.currentToken.Type == lexer.TQuotedIdentifier {
			args = append(args, &ast.ExprQuotedIdentifier{
				Value: p.currentToken.Value,
				Span:  ast.NewSpanFromLexerPosition(startPosition, p.currentToken.End),
			})
		} else if p.currentToken.Type == lexer.TStringLiteral {
			args = append(args, &ast.ExprStringLiteral{
				Value: p.currentToken.Value,
				Span:  ast.NewSpanFromLexerPosition(startPosition, p.currentToken.End),
			})
		} else if p.currentToken.Type == lexer.TNumericLiteral {
			args = append(args, &ast.ExprNumberLiteral{
				Value: p.currentToken.Value,
				Span:  ast.NewSpanFromLexerPosition(startPosition, p.currentToken.End),
			})
		} else if p.currentTokenIs(lexer.TIdentifier) {
			// check if we have a compound identifier
			identifier := &ast.ExprIdentifier{
				Value: p.currentToken.Value,
				Span:  ast.NewSpanFromLexerPosition(startPosition, p.currentToken.End),
			}
			compoundIdentifier, err := p.parseCompoundIdentifier(identifier)
			if err != nil {
				return nil, err
			}

			// we have a compoundIdentifier
			if compoundIdentifier != nil {
				compoundIdentifierPosition := p.currentToken.End
				if p.peekTokenIs(lexer.TLeftParen) {
					function := &ast.ExprFunction{
						Type: ast.FuncUserDefined,
						Name: compoundIdentifier,
						Span: ast.NewSpanFromLexerPosition(startPosition, compoundIdentifierPosition),
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
				// p.logger.Info("iner func")
				function := &ast.ExprFunction{
					Type: ast.FuncUserDefined,
					Name: identifier,
					Span: ast.NewSpanFromLexerPosition(startPosition, p.currentToken.End),
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

		if p.peekTokenIs(lexer.TRightParen) {
			break
		}
		p.nextToken()
	}

	return &args, nil
}

func (p *Parser) parseFunctionCall(function *ast.ExprFunction) (*ast.ExprFunctionCall, error) {
	if function == nil {
		parsedFunction, err := p.parseFunction()
		if err != nil {
			return nil, err
		}
		function = parsedFunction
	}
	// compoundIdentifier, err := p.parseCompoundIdentifier(function.Name)
	// if err != nil {
	// 	return nil, err
	// }
	// if compoundIdentifier != nil {
	// 	function.Name = compoundIdentifier
	// }

	// parse function arguments
	err := p.expectPeek(lexer.TLeftParen)
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

	err = p.expectPeek(lexer.TRightParen)
	if err != nil {
		p.logger.Debug("expected right parenthesis, got ", p.peekToken.Value)
		return nil, err
	}
	// check for over clause
	if !p.peekTokenIs(lexer.TOver) {
		return &ast.ExprFunctionCall{
			Name: function,
			Args: args,
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
	}, nil
}

func (p *Parser) parseBetweenLogicalOperator(left ast.Expression, notKw *ast.Keyword) (*ast.ExprBetweenLogicalOperator, error) {
	betweenKeyword := ast.NewKeywordFromToken(p.currentToken)
	p.nextToken()

	begin, err := p.parsePrefixExpression()
	if err != nil {
		return nil, err
	}
	p.logger.Debugf("between: begin %s", begin.TokenLiteral())
	if err := p.expectPeek(lexer.TAnd); err != nil {
		return nil, err
	}
	andKeyword := ast.NewKeywordFromToken(p.currentToken)

	p.nextToken()
	end, err := p.parsePrefixExpression()
	if err != nil {
		return nil, err
	}
	p.logger.Debugf("between: end %s", end.TokenLiteral())

	// check if we have and operator
	return &ast.ExprBetweenLogicalOperator{
		BetweenKeyword: betweenKeyword,
		TestExpression: left,
		NotKeyword:     notKw,
		Begin:          begin,
		AndKeyword:     andKeyword,
		End:            end,
	}, nil
}

func (p *Parser) parseInSubqueryLogicalOperator(left ast.Expression, inKeyword ast.Keyword, notKw *ast.Keyword) (*ast.ExprInSubqueryLogicalOperator, error) {
	p.nextToken()
	statement, err := p.parseSelectSubquery()
	if err != nil {
		return nil, err
	}
	if err := p.expectPeek(lexer.TRightParen); err != nil {
		return nil, err
	}

	return &ast.ExprInSubqueryLogicalOperator{
		InKeyword:      inKeyword,
		TestExpression: left,
		NotKeyword:     notKw,
		Subquery:       &statement,
	}, nil
}

func (p *Parser) parseInExpressionListLogicalOperator(left ast.Expression, inKeyword ast.Keyword, notKw *ast.Keyword) (*ast.ExprInLogicalOperator, error) {
	stmt, err := p.parseExpressionList()
	if err != nil {
		return nil, err
	}
	if err := p.expectPeek(lexer.TRightParen); err != nil {
		return nil, err
	}

	return &ast.ExprInLogicalOperator{
		InKeyword:      inKeyword,
		TestExpression: left,
		NotKeyword:     notKw,
		Expressions:    stmt.List,
	}, nil

}

func (p *Parser) parseInLogicalOperator(left ast.Expression, notKw *ast.Keyword) (ast.Expression, error) {
	inKeyword := ast.NewKeywordFromToken(p.currentToken)
	p.expectPeek(lexer.TLeftParen)
	if p.peekTokenIs(lexer.TSelect) {
		inSubquery, err := p.parseInSubqueryLogicalOperator(left, inKeyword, notKw)
		if err != nil {
			return nil, err
		}
		return inSubquery, nil
	} else if p.peekTokenIs(lexer.TIdentifier) ||
		p.peekTokenIs(lexer.TLocalVariable) ||
		p.peekTokenIs(lexer.TQuotedIdentifier) ||
		p.peekTokenIs(lexer.TStringLiteral) ||
		p.peekTokenIs(lexer.TNumericLiteral) {
		inExpressionList, err := p.parseInExpressionListLogicalOperator(left, inKeyword, notKw)
		if err != nil {
			return nil, err
		}

		return inExpressionList, nil
	}
	p.errorToken = ETCurrent
	return nil, p.currentErrorString("Expected (Subquery or Expression List) after 'IN' keyword")
}
