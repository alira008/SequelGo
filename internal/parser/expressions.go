package parser

import (
	"SequelGo/internal/ast"
	"SequelGo/internal/lexer"
	"fmt"
)

func (p *Parser) parseExpression(precedence Precedence) (ast.Expression, error) {
	leftExpr, err := p.parsePrefixExpression()
	if err != nil {
		return nil, err
	}

	// parse infix sql expressions using stacks to keep track of precedence
	for precedence < p.peekPrecedence() {
		leftExpr, err = p.parseInfixExpression(leftExpr)
		if err != nil {
			return nil, err
		}
	}

	return leftExpr, nil
}

func (p *Parser) parsePrefixExpression() (ast.Expression, error) {
	p.logger.Debugf("parsing prefix expression. peekToken %s", p.peekToken.String())
	var newExpr ast.Expression
	switch p.peekToken.Type {
	case lexer.TIdentifier,
		lexer.TNumericLiteral,
		lexer.TStringLiteral,
		lexer.TAsterisk,
		lexer.TLocalVariable,
		lexer.TQuotedIdentifier:
		switch p.peekToken.Type {
		case lexer.TLocalVariable:
			newExpr = &ast.ExprLocalVariable{
				Value: p.peekToken.Value,
				Span:  ast.NewSpanFromToken(p.peekToken),
			}
		case lexer.TQuotedIdentifier:
			newExpr = &ast.ExprQuotedIdentifier{
				Value: p.peekToken.Value,
				Span:  ast.NewSpanFromToken(p.peekToken),
			}
		case lexer.TStringLiteral:
			newExpr = &ast.ExprStringLiteral{
				Value: p.peekToken.Value,
				Span:  ast.NewSpanFromToken(p.peekToken),
			}
		case lexer.TNumericLiteral:
			newExpr = &ast.ExprNumberLiteral{
				Value: p.peekToken.Value,
				Span:  ast.NewSpanFromToken(p.peekToken),
			}
		case lexer.TIdentifier:
			newExpr = &ast.ExprIdentifier{
				Value: p.peekToken.Value,
				Span:  ast.NewSpanFromToken(p.peekToken),
			}
		case lexer.TAsterisk:
			newExpr = &ast.ExprStar{
				Span: ast.NewSpanFromToken(p.peekToken),
			}
		}

		// parsing compound identifiers
		couldBeCompound := false
		if p.peekTokenIsAny([]lexer.TokenType{lexer.TIdentifier, lexer.TQuotedIdentifier}) {
			couldBeCompound = true
		}
		p.nextToken()

		if couldBeCompound {
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
				Span: ast.NewSpanFromToken(p.peekToken),
			}

			functionCall, err := p.parseFunctionCall(function)
			if err != nil {
				return nil, err
			}

			newExpr = functionCall
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
		p.consumeToken(lexer.TLeftParen)
		startPosition := p.peekToken.Start
		if p.peekTokenIs(lexer.TSelect) {
			subquery, err := p.parseSelectSubquery()
			endPosition := p.peekToken.End
			subquery.Span = ast.NewSpanFromLexerPosition(startPosition, endPosition)
			if err != nil {
				return nil, err
			}
			_, err = p.consumeToken(lexer.TRightParen)
			if err != nil {
				return nil, err
			}

			return &subquery, nil
		} else if p.peekTokenIsAny([]lexer.TokenType{
			lexer.TIdentifier,
			lexer.TLocalVariable,
			lexer.TQuotedIdentifier,
			lexer.TStringLiteral,
			lexer.TNumericLiteral,
		}) {
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
		switch p.peekToken.Type {
		case lexer.TPlus:
			operator = ast.UnaryOpPlus
			break
		case lexer.TMinus:
			operator = ast.UnaryOpMinus
			break
		}

		if err := p.expectPeek(lexer.TNumericLiteral); err != nil {
			return nil, err
		}
		newExpr = &ast.ExprUnaryOperator{
			Operator: operator,
			Right:    &ast.ExprNumberLiteral{Value: p.peekToken.Value},
		}
		break
	case lexer.TExists:
		existsKw, err := p.consumeKeyword(lexer.TExists)
		if err != nil {
			return nil, err
		}

		expr, err := p.parseExpression(PrecedenceLowest)
		if err != nil {
			return nil, err
		}
		// check if it is a subquery
		switch v := expr.(type) {
		case *ast.ExprSubquery:
			newExpr = &ast.ExprExistsLogicalOperator{
				ExistsKeyword: *existsKw,
				Subquery:      v,
			}
			break
		default:
			return nil, fmt.Errorf("Expected subquery after 'EXISTS' keyword")
		}
		break
	case lexer.TNot:
		notKw, err := p.consumeKeyword(lexer.TNot)
		if err != nil {
			return nil, err
		}

		expr, err := p.parseExpression(PrecedenceLowest)
		if err != nil {
			return nil, err
		}

		p.logger.Debugln(expr.TokenLiteral())
		// check if it is a subquery
		newExpr = &ast.ExprNotLogicalOperator{
			Expression: expr,
			NotKeyword: *notKw,
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
		return nil, p.peekErrorString(fmt.Sprintf("Unimplemented expression \"%s\"", p.peekToken.Type.String()))
	}

	return newExpr, nil

}

func (p *Parser) parseInfixExpression(left ast.Expression) (ast.Expression, error) {
	p.logger.Debug("parsing infix expression, ", p.peekToken.String())
	startPosition := p.peekToken.Start
	switch p.peekToken.Type {
	case lexer.TAnd:
		precedence := p.peekPrecedence()
		andKw, err := p.consumeKeyword(lexer.TAnd)
		if err != nil {
			return nil, err
		}
		right, err := p.parseExpression(precedence)
		if err != nil {
			return nil, err
		}
		return &ast.ExprAndLogicalOperator{
			Left:       left,
			AndKeyword: *andKw,
			Right:      right,
			Span: ast.Span{
				StartPosition: startPosition,
				EndPosition:   right.GetSpan().EndPosition,
			},
		}, nil
	case lexer.TOr:
		precedence := p.peekPrecedence()
		orKw, err := p.consumeKeyword(lexer.TOr)
		if err != nil {
			return nil, err
		}
		right, err := p.parseExpression(precedence)
		if err != nil {
			return nil, err
		}
		return &ast.ExprOrLogicalOperator{
			Left:      left,
			OrKeyword: *orKw,
			Right:     right,
			Span: ast.Span{
				StartPosition: startPosition,
				EndPosition:   right.GetSpan().EndPosition,
			},
		}, nil
	case lexer.TPlus,
		lexer.TMinus,
		lexer.TAsterisk,
		lexer.TDivide,
		lexer.TMod:
		var operator ast.ArithmeticOperatorType
		switch p.peekToken.Type {
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
		precedence := p.peekPrecedence()
		p.nextToken()

		right, err := p.parseExpression(precedence)
		if err != nil {
			return nil, err
		}
		// fmt.Println("mod test", right.TokenLiteral())
		return &ast.ExprArithmeticOperator{
			Left:     left,
			Operator: operator,
			Right:    right,
			Span: ast.Span{
				StartPosition: startPosition,
				EndPosition:   right.GetSpan().EndPosition,
			},
		}, nil
	case lexer.TEqual,
		lexer.TNotEqualBang,
		lexer.TNotEqualArrow,
		lexer.TGreaterThan,
		lexer.TLessThan,
		lexer.TGreaterThanEqual,
		lexer.TLessThanEqual:

		var operator ast.ComparisonOperatorType
		switch p.peekToken.Type {
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
		precedence := p.peekPrecedence()
		p.nextToken()

		if allKw := p.maybeKeyword(lexer.TAll); allKw != nil {
			right, err := p.parseExpression(precedence)
			if err != nil {
				return nil, err
			}
			switch v := right.(type) {
			case *ast.ExprSubquery:
				return &ast.ExprAllLogicalOperator{
					AllKeyword:         *allKw,
					ScalarExpression:   left,
					ComparisonOperator: operator,
					Subquery:           v,
					Span: ast.Span{
						StartPosition: startPosition,
						EndPosition:   right.GetSpan().EndPosition,
					},
				}, nil
			}
			return nil, fmt.Errorf("sub query was not provided for All Expression")
		} else if someKw := p.maybeKeyword(lexer.TSome); someKw != nil {
			right, err := p.parseExpression(precedence)
			if err != nil {
				return nil, err
			}
			switch v := right.(type) {
			case *ast.ExprSubquery:
				return &ast.ExprSomeLogicalOperator{
					SomeKeyword:        *someKw,
					ScalarExpression:   left,
					ComparisonOperator: operator,
					Subquery:           v,
					Span: ast.Span{
						StartPosition: startPosition,
						EndPosition:   right.GetSpan().EndPosition,
					},
				}, nil
			}
			return nil, p.peekErrorString("(Subquery) was not provided for Some Expression")
		} else if anyKw := p.maybeKeyword(lexer.TAny); anyKw != nil {
			right, err := p.parseExpression(precedence)
			if err != nil {
				return nil, err
			}
			switch v := right.(type) {
			case *ast.ExprSubquery:
				return &ast.ExprAnyLogicalOperator{
					AnyKeyword:         *anyKw,
					ScalarExpression:   left,
					ComparisonOperator: operator,
					Subquery:           v,
					Span: ast.Span{
						StartPosition: startPosition,
						EndPosition:   right.GetSpan().EndPosition,
					},
				}, nil
			}
			return nil, p.peekErrorString("(Subquery) was not provided for Any Expression")
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
			Span: ast.Span{
				StartPosition: startPosition,
				EndPosition:   right.GetSpan().EndPosition,
			},
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
		precedence := p.peekPrecedence()
		likeKw, err := p.consumeKeyword(lexer.TLike)
		if err != nil {
			return nil, err
		}

		right, err := p.parseExpression(precedence)
		if err != nil {
			return nil, err
		}

		return &ast.ExprLikeLogicalOperator{
			LikeKeyword:     *likeKw,
			MatchExpression: left,
			Pattern:         right,
			Span: ast.Span{
				StartPosition: startPosition,
				EndPosition:   right.GetSpan().EndPosition,
			},
		}, nil
	case lexer.TNot:
		notKw, err := p.consumeKeyword(lexer.TNot)
		if err != nil {
			return nil, err
		}

		if p.peekTokenIs(lexer.TBetween) {
			betweenOp, err := p.parseBetweenLogicalOperator(left, notKw)
			if err != nil {
				return nil, err
			}
			return betweenOp, nil
		} else if p.peekTokenIs(lexer.TIn) {
			inLogicalOp, err := p.parseInLogicalOperator(left, notKw)
			if err != nil {
				return nil, err
			}
			return inLogicalOp, nil
		} else if p.peekTokenIs(lexer.TLike) {
			precedence := p.peekPrecedence()
			likeKw, err := p.consumeKeyword(lexer.TLike)
			if err != nil {
				return nil, err
			}

			right, err := p.parseExpression(precedence)
			if err != nil {
				return nil, err
			}

			return &ast.ExprLikeLogicalOperator{
				LikeKeyword:     *likeKw,
				MatchExpression: left,
				NotKeyword:      notKw,
				Pattern:         right,
				Span: ast.Span{
					StartPosition: startPosition,
					EndPosition:   right.GetSpan().EndPosition,
				},
			}, nil
		} else {
			return nil, p.peekErrorString("(BETWEEN Expression or IN Expression or LIKE Expression) after 'Test Expression NOT' Expression")
		}
	}
	return nil, p.peekErrorString(fmt.Sprintf("Unimplemented expression %s", p.peekToken.Type.String()))
}
