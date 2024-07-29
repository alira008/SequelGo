package parser

import (
	"SequelGo/internal/ast"
	"SequelGo/internal/lexer"
	"fmt"
)

func (p *Parser) parseExpression(precedence Precedence) (ast.Expression, error) {
	startPosition := p.currentToken.Start
	leftExpr, err := p.parsePrefixExpression()
	endPosition := p.currentToken.End
	if err != nil {
		return nil, err
	}
	leftExpr.SetSpan(ast.NewSpanFromLexerPosition(startPosition, endPosition))

	// parse infix sql expressions using stacks to keep track of precedence
	for precedence < checkPrecedence(p.peekToken.Type) {
		p.nextToken()

		startPosition = p.currentToken.Start
		leftExpr, err = p.parseInfixExpression(leftExpr)
		if err != nil {
			return nil, err
		}
		endPosition = p.currentToken.End
		leftExpr.SetSpan(ast.NewSpanFromLexerPosition(startPosition, endPosition))
	}

	return leftExpr, nil
}

func (p *Parser) parsePrefixExpression() (ast.Expression, error) {
	p.logger.Debugf("parsing prefix expression. currentToken %s", p.currentToken)
	var newExpr ast.Expression
	switch p.currentToken.Type {
	case lexer.TIdentifier, lexer.TNumericLiteral, lexer.TStringLiteral, lexer.TAsterisk, lexer.TLocalVariable, lexer.TQuotedIdentifier:
		switch p.currentToken.Type {
		case lexer.TLocalVariable:
			newExpr = &ast.ExprLocalVariable{
				Value: p.currentToken.Value,
				Span:  ast.NewSpanFromLexerPosition(p.currentToken.Start, p.currentToken.End),
			}
		case lexer.TQuotedIdentifier:
			newExpr = &ast.ExprQuotedIdentifier{
				Value: p.currentToken.Value,
				Span:  ast.NewSpanFromLexerPosition(p.currentToken.Start, p.currentToken.End),
			}
		case lexer.TStringLiteral:
			newExpr = &ast.ExprStringLiteral{
				Value: p.currentToken.Value,
				Span:  ast.NewSpanFromLexerPosition(p.currentToken.Start, p.currentToken.End),
			}
		case lexer.TNumericLiteral:
			newExpr = &ast.ExprNumberLiteral{
				Value: p.currentToken.Value,
				Span:  ast.NewSpanFromLexerPosition(p.currentToken.Start, p.currentToken.End),
			}
		case lexer.TIdentifier:
			newExpr = &ast.ExprIdentifier{
				Value: p.currentToken.Value,
				Span:  ast.NewSpanFromLexerPosition(p.currentToken.Start, p.currentToken.End),
			}
		case lexer.TAsterisk:
			newExpr = &ast.ExprStar{
				Span: ast.NewSpanFromLexerPosition(p.currentToken.Start, p.currentToken.End),
			}
		}

		// parsing compound identifiers
		if p.currentTokenIs(lexer.TIdentifier) || p.currentTokenIs(lexer.TQuotedIdentifier) {
			parsedCompoundIdentifier, err := p.parseCompoundIdentifier(newExpr)
			if err != nil {
				return nil, err
			}

			// we have a compoundIdentifier
			if parsedCompoundIdentifier != nil {
				newExpr = parsedCompoundIdentifier
			}
		}

		// parsing user functions
		if p.peekTokenIs(lexer.TLeftParen) {
			p.logger.Debugln("parsing user defined function")
			function := &ast.ExprFunction{
				Type: ast.FuncUserDefined,
				Name: newExpr,
				Span: ast.NewSpanFromLexerPosition(p.currentToken.Start, p.currentToken.End),
			}

			functionCall, err := p.parseFunctionCall(function)
			if err != nil {
				return nil, err
			}

			newExpr = functionCall
		}

		if (p.peekTokenIs(lexer.TAs) || p.peekTokenIs(lexer.TIdentifier) ||
			p.peekTokenIs(lexer.TStringLiteral) || p.peekTokenIs(lexer.TQuotedIdentifier)) &&
			!p.peekToken2IsAny(ast.DataTypeTokenTypes) {
			expr := &ast.ExprWithAlias{Expression: newExpr}

			if p.peekToken.Type == lexer.TAs {
				p.nextToken()
				kw := ast.NewKeywordFromToken(p.currentToken)
				expr.AsKeyword = &kw
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
				err = p.currentErrorString("Expected (Identifier or StringLiteral or QuotedIdentifier) for Alias")
				return nil, err
			}
			expr.Alias = alias
			newExpr = expr
		}
	case lexer.TDenseRank,
		lexer.TRank,
		lexer.TRowNumber,
		lexer.TAbs,
		lexer.TAcos,
		lexer.TAsin,
		lexer.TAtan,
		lexer.TCeiling,
		lexer.TCos,
		lexer.TCot,
		lexer.TDegrees,
		lexer.TExp,
		lexer.TFloor,
		lexer.TLog,
		lexer.TLog10,
		lexer.TPi,
		lexer.TPower,
		lexer.TRadians,
		lexer.TRands,
		lexer.TRound,
		lexer.TSign,
		lexer.TSin,
		lexer.TSqrt,
		lexer.TSquare,
		lexer.TTan,
		lexer.TFirstValue,
		lexer.TLastValue,
		lexer.TLag,
		lexer.TLead,
		lexer.TAvg,
		lexer.TCount,
		lexer.TMax,
		lexer.TMin,
		lexer.TStdev,
		lexer.TStdevp,
		lexer.TSum,
		lexer.TVar,
		lexer.TVarp,
		lexer.TGetdate,
		lexer.TChecksum,
		lexer.TNewId:
		functionCall, err := p.parseFunctionCall(nil)
		if err != nil {
			return nil, err
		}

		return functionCall, nil
	case lexer.TLeftParen:
		// start of subquery
		startPosition := p.currentToken.Start
		if p.peekTokenIs(lexer.TSelect) {
			p.nextToken()
			subquery, err := p.parseSelectSubquery()
			endPosition := p.currentToken.End
			subquery.Span = ast.NewSpanFromLexerPosition(startPosition, endPosition)
			if err != nil {
				return nil, err
			}
			p.expectPeek(lexer.TRightParen)

			p.logger.Debug("parsing subquery alias")
			p.logger.Debug(p.peekToken)
			if (p.peekTokenIs(lexer.TAs) || p.peekTokenIs(lexer.TIdentifier) ||
				p.peekTokenIs(lexer.TStringLiteral) || p.peekTokenIs(lexer.TQuotedIdentifier)) &&
				!p.peekToken2IsAny(ast.DataTypeTokenTypes) {
				p.logger.Debug("parsing subquery alias")
				exprWithAlias := &ast.ExprWithAlias{
					Expression: &subquery,
				}

				p.logger.Debug("parsing subquery alias")
				if p.peekToken.Type == lexer.TAs {
					p.nextToken()
					kw := ast.NewKeywordFromToken(p.currentToken)
					exprWithAlias.AsKeyword = &kw
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
				exprWithAlias.Alias = alias
				return exprWithAlias, nil
			}

			return &subquery, nil
		} else if p.peekTokenIs(lexer.TIdentifier) ||
			p.peekTokenIs(lexer.TLocalVariable) ||
			p.peekTokenIs(lexer.TQuotedIdentifier) ||
			p.peekTokenIs(lexer.TStringLiteral) ||
			p.peekTokenIs(lexer.TNumericLiteral) {
			stmt, err := p.parseExpressionList()
			if err != nil {
				return nil, err
			}
			if err := p.expectPeek(lexer.TRightParen); err != nil {
				return nil, err
			}
			p.logger.Debug("stmt: ", stmt)
			return &stmt, nil
		}
	case lexer.TPlus, lexer.TMinus:
		var operator ast.UnaryOperatorType
		switch p.currentToken.Type {
		case lexer.TPlus:
			operator = ast.UnaryOpPlus
			break
		case lexer.TMinus:
			operator = ast.UnaryOpMinus
			break
		}

		p.nextToken()

		right, err := p.parseExpression(PrecedenceLowest)
		if err != nil {
			return nil, err
		}
		newExpr = &ast.ExprUnaryOperator{
			Operator: operator,
			Right:    right,
		}
		break
	case lexer.TExists:
        existsKw := ast.NewKeywordFromToken(p.currentToken)
		p.nextToken()

		expr, err := p.parseExpression(PrecedenceLowest)
		if err != nil {
			return nil, err
		}
		// check if it is a subquery
		switch v := expr.(type) {
		case *ast.ExprSubquery:
			newExpr = &ast.ExprExistsLogicalOperator{
                ExistsKeyword: existsKw,
				Subquery: v,
			}
			break
		default:
			p.errorToken = ETCurrent
			return nil, fmt.Errorf("Expected subquery after 'EXISTS' keyword")
		}
		break
	case lexer.TNot:
        notKw := ast.NewKeywordFromToken(p.currentToken)
		p.nextToken()

		expr, err := p.parseExpression(PrecedenceLowest)
		if err != nil {
			return nil, err
		}

		p.logger.Debugln(expr.TokenLiteral())
		// check if it is a subquery
		newExpr = &ast.ExprNotLogicalOperator{
			Expression: expr,
            NotKeyword: notKw,
		}
		break
	case lexer.TCast:
		expr, err := p.parseCast()
		if err != nil {
			return nil, err
		}
		p.logger.Debug(expr.TokenLiteral())
		newExpr = expr
	default:
		p.errorToken = ETCurrent
		return nil, fmt.Errorf("Unimplemented expression %s", p.currentToken.Type.String())
	}

	return newExpr, nil

}

func (p *Parser) parseInfixExpression(left ast.Expression) (ast.Expression, error) {
	p.logger.Debug("parsing infix expression, ", p.currentToken.String())
	switch p.currentToken.Type {
	case lexer.TAnd:
        andKw := ast.NewKeywordFromToken(p.currentToken)
		precedence := checkPrecedence(p.currentToken.Type)
		p.nextToken()

		right, err := p.parseExpression(precedence)
		if err != nil {
			return nil, err
		}
		return &ast.ExprAndLogicalOperator{
			Left:  left,
            AndKeyword: andKw,
			Right: right,
		}, nil
	case lexer.TOr:
        orKw := ast.NewKeywordFromToken(p.currentToken)
		precedence := checkPrecedence(p.currentToken.Type)
		p.nextToken()

		right, err := p.parseExpression(precedence)
		if err != nil {
			return nil, err
		}
		return &ast.ExprOrLogicalOperator{
			Left:  left,
            OrKeyword: orKw,
			Right: right,
		}, nil
	case lexer.TPlus,
		lexer.TMinus,
		lexer.TAsterisk,
		lexer.TDivide,
		lexer.TMod:
		var operator ast.ArithmeticOperatorType
		switch p.currentToken.Type {
		case lexer.TPlus:
			operator = ast.ArithmeticOpPlus
		case lexer.TMinus:
			operator = ast.ArithmeticOpMinus
		case lexer.TAsterisk:
			operator = ast.ArithmeticOpMult
		case lexer.TDivide:
			operator = ast.ArithmeticOpDiv
		case lexer.TMod:
			operator = ast.ArithmeticOpMod
		}
		precedence := checkPrecedence(p.currentToken.Type)
		p.nextToken()

		right, err := p.parseExpression(precedence)
		if err != nil {
			return nil, err
		}
		return &ast.ExprArithmeticOperator{
			Left:     left,
			Operator: operator,
			Right:    right,
		}, nil
	case lexer.TEqual,
		lexer.TNotEqualBang,
		lexer.TNotEqualArrow,
		lexer.TGreaterThan,
		lexer.TLessThan,
		lexer.TGreaterThanEqual,
		lexer.TLessThanEqual:

		var operator ast.ComparisonOperatorType
		switch p.currentToken.Type {
		case lexer.TEqual:
			operator = ast.ComparisonOpEqual
		case lexer.TNotEqualBang:
			operator = ast.ComparisonOpNotEqualBang
		case lexer.TNotEqualArrow:
			operator = ast.ComparisonOpNotEqualArrow
		case lexer.TGreaterThan:
			operator = ast.ComparisonOpGreater
		case lexer.TLessThan:
			operator = ast.ComparisonOpLess
		case lexer.TGreaterThanEqual:
			operator = ast.ComparisonOpGreaterEqual
		case lexer.TLessThanEqual:
			operator = ast.ComparisonOpLessEqual
		}
		precedence := checkPrecedence(p.currentToken.Type)
		p.nextToken()

		if p.currentTokenIs(lexer.TAll) {
			allKw := ast.NewKeywordFromToken(p.currentToken)
			p.nextToken()
			right, err := p.parseExpression(precedence)
			if err != nil {
				return nil, err
			}
			switch v := right.(type) {
			case *ast.ExprSubquery:
				return &ast.ExprAllLogicalOperator{
					AllKeyword:         allKw,
					ScalarExpression:   left,
					ComparisonOperator: operator,
					Subquery:           v,
				}, nil
			}
			p.errorToken = ETCurrent
			return nil, fmt.Errorf("sub query was not provided for All Expression")
		} else if p.currentTokenIs(lexer.TSome) {
			someKw := ast.NewKeywordFromToken(p.currentToken)
			p.nextToken()
			right, err := p.parseExpression(precedence)
			if err != nil {
				return nil, err
			}
			switch v := right.(type) {
			case *ast.ExprSubquery:
				return &ast.ExprSomeLogicalOperator{
					SomeKeyword:        someKw,
					ScalarExpression:   left,
					ComparisonOperator: operator,
					Subquery:           v,
				}, nil
			}
			p.errorToken = ETCurrent
			return nil, p.currentErrorString("(Subquery) was not provided for Some Expression")
		} else if p.currentTokenIs(lexer.TAny) {
			anyKw := ast.NewKeywordFromToken(p.currentToken)
			p.nextToken()
			right, err := p.parseExpression(precedence)
			if err != nil {
				return nil, err
			}
			switch v := right.(type) {
			case *ast.ExprSubquery:
				return &ast.ExprAnyLogicalOperator{
					AnyKeyword:         anyKw,
					ScalarExpression:   left,
					ComparisonOperator: operator,
					Subquery:           v,
				}, nil
			}
			p.errorToken = ETCurrent
			return nil, p.currentErrorString("(Subquery) was not provided for Any Expression")
		}

		// parse the expression of the operator
		right, err := p.parseExpression(precedence)
		if err != nil {
			return nil, err
		}
		return &ast.ExprComparisonOperator{
			Left:     left,
			Operator: operator,
			Right:    right,
		}, nil
	case lexer.TBetween:
		betweenOp, err := p.parseBetweenLogicalOperator(left, nil)
		if err != nil {
			return nil, err
		}
		return betweenOp, nil
	case lexer.TIn:
		inLogicalOp, err := p.parseInLogicalOperator(left, nil)
		if err != nil {
			return nil, err
		}
		return inLogicalOp, nil
	case lexer.TLike:
		likeKw := ast.NewKeywordFromToken(p.currentToken)
		precedence := checkPrecedence(p.currentToken.Type)
		p.nextToken()

		right, err := p.parseExpression(precedence)
		if err != nil {
			return nil, err
		}

		return &ast.ExprLikeLogicalOperator{
			LikeKeyword:     likeKw,
			MatchExpression: left,
			Pattern:         right,
		}, nil
	case lexer.TNot:
		notKw := ast.NewKeywordFromToken(p.currentToken)
		p.nextToken()

		if p.currentTokenIs(lexer.TBetween) {
			betweenOp, err := p.parseBetweenLogicalOperator(left, &notKw)
			if err != nil {
				return nil, err
			}
			return betweenOp, nil
		} else if p.currentTokenIs(lexer.TIn) {
			inLogicalOp, err := p.parseInLogicalOperator(left, &notKw)
			if err != nil {
				return nil, err
			}
			return inLogicalOp, nil
		} else if p.currentTokenIs(lexer.TLike) {
			likeKw := ast.NewKeywordFromToken(p.currentToken)
			precedence := checkPrecedence(p.currentToken.Type)
			p.nextToken()

			right, err := p.parseExpression(precedence)
			if err != nil {
				return nil, err
			}

			return &ast.ExprLikeLogicalOperator{
				LikeKeyword:     likeKw,
				MatchExpression: left,
				NotKeyword:      &notKw,
				Pattern:         right,
			}, nil
		} else {
			p.errorToken = ETCurrent
			return nil, p.currentErrorString("(BETWEEN Expression or IN Expression or LIKE Expression) after 'Test Expression NOT' Expression")
		}
	}
	p.errorToken = ETCurrent
	return nil, p.currentErrorString("Unimplemented expression")
}
