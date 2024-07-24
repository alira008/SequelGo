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
	leftExpr.SetBaseNode(ast.NewBaseNodeFromLexerPosition(startPosition, endPosition))
	if err != nil {
		return nil, err
	}

	// parse infix sql expressions using stacks to keep track of precedence
	for precedence < checkPrecedence(p.peekToken.Type) {
		p.nextToken()

		startPosition = p.currentToken.Start
		leftExpr, err = p.parseInfixExpression(leftExpr)
		endPosition = p.currentToken.End
		leftExpr.SetBaseNode(ast.NewBaseNodeFromLexerPosition(startPosition, endPosition))
		if err != nil {
			return nil, err
		}
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
				Value:    p.currentToken.Value,
				BaseNode: ast.NewBaseNodeFromLexerPosition(p.currentToken.Start, p.currentToken.End),
			}
		case lexer.TQuotedIdentifier:
			newExpr = &ast.ExprQuotedIdentifier{
				Value:    p.currentToken.Value,
				BaseNode: ast.NewBaseNodeFromLexerPosition(p.currentToken.Start, p.currentToken.End),
			}
		case lexer.TStringLiteral:
			newExpr = &ast.ExprStringLiteral{
				Value:    p.currentToken.Value,
				BaseNode: ast.NewBaseNodeFromLexerPosition(p.currentToken.Start, p.currentToken.End),
			}
		case lexer.TNumericLiteral:
			newExpr = &ast.ExprNumberLiteral{
				Value:    p.currentToken.Value,
				BaseNode: ast.NewBaseNodeFromLexerPosition(p.currentToken.Start, p.currentToken.End),
			}
		case lexer.TIdentifier:
			newExpr = &ast.ExprIdentifier{
				Value:    p.currentToken.Value,
				BaseNode: ast.NewBaseNodeFromLexerPosition(p.currentToken.Start, p.currentToken.End),
			}
		case lexer.TAsterisk:
			newExpr = &ast.ExprStar{
				BaseNode: ast.NewBaseNodeFromLexerPosition(p.currentToken.Start, p.currentToken.End),
			}
		}

		if p.peekToken.Type == lexer.TPeriod {
			// we are dealing with a qualified identifier
			startPositionCompound := p.currentToken.Start
			compound := &[]ast.Expression{newExpr}
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
						BaseNode: ast.NewBaseNodeFromLexerPosition(startPosition, p.currentToken.End),
					}
					*compound = append(*compound, expr)
					break
				} else if p.currentToken.Type == lexer.TQuotedIdentifier {
					expr := &ast.ExprQuotedIdentifier{
						Value:    p.currentToken.Value,
						BaseNode: ast.NewBaseNodeFromLexerPosition(startPosition, p.currentToken.End),
					}
					*compound = append(*compound, expr)
				} else {
					expr := &ast.ExprIdentifier{
						Value:    p.currentToken.Value,
						BaseNode: ast.NewBaseNodeFromLexerPosition(startPosition, p.currentToken.End),
					}
					*compound = append(*compound, expr)
				}

				if p.peekToken.Type != lexer.TPeriod {
					break
				}

				p.nextToken()
			}

			newExpr = &ast.ExprCompoundIdentifier{
                Identifiers: *compound,
                BaseNode: ast.NewBaseNodeFromLexerPosition(startPositionCompound, p.currentToken.End),
            }
		}

		if p.peekTokenIs(lexer.TLeftParen) {
			p.logger.Debugln("parsing user defined function")
			function := &ast.ExprFunction{
				Type:     ast.FuncUserDefined,
				Name:     newExpr,
				BaseNode: ast.NewBaseNodeFromLexerPosition(p.currentToken.Start, p.currentToken.End),
			}
			// parse function arguments
			err := p.expectPeek(lexer.TLeftParen)
			if err != nil {
				return nil, err
			}
			args := []ast.Expression{}
			if !p.peekTokenIs(lexer.TRightParen) {
				p.logger.Debug("parsing function args")
				for {
					startPosition := p.currentToken.Start
					p.logger.Debug(p.currentToken)
					p.logger.Debug(p.peekToken)
					err = p.expectPeekMany([]lexer.TokenType{lexer.TIdentifier,
						lexer.TNumericLiteral,
						lexer.TStringLiteral,
						lexer.TLocalVariable,
						lexer.TQuotedIdentifier,
					})
					if err != nil {
						return nil, err
					}

					if p.currentToken.Type == lexer.TLocalVariable {
						args = append(args, &ast.ExprLocalVariable{
							Value:    p.currentToken.Value,
							BaseNode: ast.NewBaseNodeFromLexerPosition(startPosition, p.currentToken.End),
						})
					} else if p.currentToken.Type == lexer.TQuotedIdentifier {
						args = append(args, &ast.ExprQuotedIdentifier{
							Value:    p.currentToken.Value,
							BaseNode: ast.NewBaseNodeFromLexerPosition(startPosition, p.currentToken.End),
						})
					} else if p.currentToken.Type == lexer.TStringLiteral {
						args = append(args, &ast.ExprStringLiteral{
							Value:    p.currentToken.Value,
							BaseNode: ast.NewBaseNodeFromLexerPosition(startPosition, p.currentToken.End),
						})
					} else if p.currentToken.Type == lexer.TNumericLiteral {
						args = append(args, &ast.ExprNumberLiteral{
							Value:    p.currentToken.Value,
							BaseNode: ast.NewBaseNodeFromLexerPosition(startPosition, p.currentToken.End),
						})
					} else {
						args = append(args, &ast.ExprIdentifier{
							Value:    p.currentToken.Value,
							BaseNode: ast.NewBaseNodeFromLexerPosition(startPosition, p.currentToken.End),
						})
					}

					if p.peekTokenIs(lexer.TRightParen) {
						break
					}
					p.nextToken()
				}
			}

			p.logger.Debug(args)
			err = p.expectPeek(lexer.TRightParen)
			if err != nil {
				p.logger.Debug("expected right parenthesis, got ", p.peekToken.Value)
				p.logger.Debug(p.currentToken)
				return nil, err
			}

			newExpr = &ast.ExprFunctionCall{
				Name: function,
				Args: args,
			}
		}

		if (p.peekTokenIs(lexer.TAs) || p.peekTokenIs(lexer.TIdentifier) ||
			p.peekTokenIs(lexer.TStringLiteral) || p.peekTokenIs(lexer.TQuotedIdentifier)) &&
			!p.peekToken2IsAny(ast.DataTypeTokenTypes) {
			expr := &ast.ExprWithAlias{AsTokenPresent: false, Expression: newExpr}

			if p.peekToken.Type == lexer.TAs {
				expr.AsTokenPresent = true
				p.nextToken()
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
			Type:     funcType,
			Name:     &ast.ExprIdentifier{Value: p.currentToken.Value},
			BaseNode: ast.NewBaseNodeFromLexerPosition(p.currentToken.Start, p.currentToken.End),
		}
		// parse function arguments
		err := p.expectPeek(lexer.TLeftParen)
		if err != nil {
			return nil, err
		}
		args := []ast.Expression{}
		if !p.peekTokenIs(lexer.TRightParen) {

			p.logger.Debug("parsing function args")
			for {
				startPosition := p.currentToken.Start
				err = p.expectPeekMany([]lexer.TokenType{lexer.TIdentifier,
					lexer.TNumericLiteral,
					lexer.TStringLiteral,
					lexer.TLocalVariable,
					lexer.TQuotedIdentifier,
				})
				if err != nil {
					return nil, err
				}

				if p.currentToken.Type == lexer.TLocalVariable {
					args = append(args, &ast.ExprLocalVariable{
						Value:    p.currentToken.Value,
						BaseNode: ast.NewBaseNodeFromLexerPosition(startPosition, p.currentToken.End),
					})
				} else if p.currentToken.Type == lexer.TQuotedIdentifier {
					args = append(args, &ast.ExprQuotedIdentifier{
						Value:    p.currentToken.Value,
						BaseNode: ast.NewBaseNodeFromLexerPosition(startPosition, p.currentToken.End),
					})
				} else if p.currentToken.Type == lexer.TStringLiteral {
					args = append(args, &ast.ExprStringLiteral{
						Value:    p.currentToken.Value,
						BaseNode: ast.NewBaseNodeFromLexerPosition(startPosition, p.currentToken.End),
					})
				} else if p.currentToken.Type == lexer.TNumericLiteral {
					args = append(args, &ast.ExprNumberLiteral{
						Value:    p.currentToken.Value,
						BaseNode: ast.NewBaseNodeFromLexerPosition(startPosition, p.currentToken.End),
					})
				} else {
					args = append(args, &ast.ExprIdentifier{
						Value:    p.currentToken.Value,
						BaseNode: ast.NewBaseNodeFromLexerPosition(startPosition, p.currentToken.End),
					})
				}

				if p.peekTokenIs(lexer.TRightParen) {
					break
				}
				p.nextToken()
			}
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
	case lexer.TLeftParen:
		// start of subquery
		startPosition := p.currentToken.Start
		if p.peekTokenIs(lexer.TSelect) {
			p.nextToken()
			subquery, err := p.parseSelectSubquery()
			endPosition := p.currentToken.End
			subquery.BaseNode = ast.NewBaseNodeFromLexerPosition(startPosition, endPosition)
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
					AsTokenPresent: false,
					Expression:     &subquery,
				}

				p.logger.Debug("parsing subquery alias")
				if p.peekToken.Type == lexer.TAs {
					exprWithAlias.AsTokenPresent = true
					p.nextToken()
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
			p.expectPeek(lexer.TRightParen)
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
		p.nextToken()

		expr, err := p.parseExpression(PrecedenceLowest)
		if err != nil {
			return nil, err
		}
		// check if it is a subquery
		switch v := expr.(type) {
		case *ast.ExprSubquery:
			newExpr = &ast.ExprExistsLogicalOperator{
				Subquery: v,
			}
			break
		default:
			p.errorToken = ETCurrent
			return nil, fmt.Errorf("Expected subquery after 'EXISTS' keyword")
		}
		break
	case lexer.TNot:
		p.nextToken()

		expr, err := p.parseExpression(PrecedenceLowest)
		if err != nil {
			return nil, err
		}

		p.logger.Debugln(expr.TokenLiteral())
		// check if it is a subquery
		newExpr = &ast.ExprNotLogicalOperator{
			Expression: expr,
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
		precedence := checkPrecedence(p.currentToken.Type)
		p.nextToken()

		right, err := p.parseExpression(precedence)
		if err != nil {
			return nil, err
		}
		return &ast.ExprAndLogicalOperator{
			Left:  left,
			Right: right,
		}, nil
	case lexer.TOr:
		precedence := checkPrecedence(p.currentToken.Type)
		p.nextToken()

		right, err := p.parseExpression(precedence)
		if err != nil {
			return nil, err
		}
		return &ast.ExprOrLogicalOperator{
			Left:  left,
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
			p.nextToken()
			right, err := p.parseExpression(precedence)
			if err != nil {
				return nil, err
			}
			switch v := right.(type) {
			case *ast.ExprSubquery:
				return &ast.ExprAllLogicalOperator{
					ScalarExpression:   left,
					ComparisonOperator: operator,
					Subquery:           v,
				}, nil
			}
			p.errorToken = ETCurrent
			return nil, fmt.Errorf("sub query was not provided for All Expression")
		} else if p.currentTokenIs(lexer.TSome) {
			p.nextToken()
			right, err := p.parseExpression(precedence)
			if err != nil {
				return nil, err
			}
			switch v := right.(type) {
			case *ast.ExprSubquery:
				return &ast.ExprSomeLogicalOperator{
					ScalarExpression:   left,
					ComparisonOperator: operator,
					Subquery:           v,
				}, nil
			}
			p.errorToken = ETCurrent
			return nil, p.currentErrorString("(Subquery) was not provided for Some Expression")
		} else if p.currentTokenIs(lexer.TAny) {
			p.nextToken()
			right, err := p.parseExpression(precedence)
			if err != nil {
				return nil, err
			}
			switch v := right.(type) {
			case *ast.ExprSubquery:
				return &ast.ExprAnyLogicalOperator{
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
		p.nextToken()

		begin, err := p.parsePrefixExpression()
		if err != nil {
			return nil, err
		}
		p.logger.Debugf("between: begin %s", begin.TokenLiteral())
		if err := p.expectPeek(lexer.TAnd); err != nil {
			return nil, err
		}

		p.nextToken()
		end, err := p.parsePrefixExpression()
		if err != nil {
			return nil, err
		}
		p.logger.Debugf("between: end %s", end.TokenLiteral())

		// check if we have and operator
		return &ast.ExprBetweenLogicalOperator{
			TestExpression: left,
			Begin:          begin,
			End:            end,
		}, nil
	case lexer.TIn:
		p.expectPeek(lexer.TLeftParen)
		if p.peekTokenIs(lexer.TSelect) {
			p.nextToken()
			statement, err := p.parseSelectSubquery()
			if err != nil {
				return nil, err
			}
			p.expectPeek(lexer.TRightParen)

			return &ast.ExprInSubqueryLogicalOperator{
				TestExpression: left,
				Subquery:       &statement,
			}, nil
		} else if p.peekTokenIs(lexer.TIdentifier) ||
			p.peekTokenIs(lexer.TLocalVariable) ||
			p.peekTokenIs(lexer.TQuotedIdentifier) ||
			p.peekTokenIs(lexer.TStringLiteral) ||
			p.peekTokenIs(lexer.TNumericLiteral) {
			stmt, err := p.parseExpressionList()
			if err != nil {
				return nil, err
			}
			p.expectPeek(lexer.TRightParen)

			return &ast.ExprInLogicalOperator{
				TestExpression: left,
				Expressions:    stmt.List,
			}, nil
		}
		p.errorToken = ETCurrent
		return nil, p.currentErrorString("Expected (Subquery or Expression List) after 'IN' keyword")
	case lexer.TLike:
		precedence := checkPrecedence(p.currentToken.Type)
		p.nextToken()

		right, err := p.parseExpression(precedence)
		if err != nil {
			return nil, err
		}

		return &ast.ExprLikeLogicalOperator{
			MatchExpression: left,
			Pattern:         right,
		}, nil
	case lexer.TNot:
		p.nextToken()

		if p.currentTokenIs(lexer.TBetween) {
			p.nextToken()

			begin, err := p.parsePrefixExpression()
			if err != nil {
				return nil, err
			}
			p.logger.Debugf("between: begin %s", begin.TokenLiteral())
			if err := p.expectPeek(lexer.TAnd); err != nil {
				return nil, err
			}

			p.nextToken()
			end, err := p.parsePrefixExpression()
			if err != nil {
				return nil, err
			}
			p.logger.Debugf("between: end %s", end.TokenLiteral())

			// check if we have and operator
			return &ast.ExprBetweenLogicalOperator{
				TestExpression: left,
				Not:            true,
				Begin:          begin,
				End:            end,
			}, nil
		} else if p.currentTokenIs(lexer.TIn) {
			p.expectPeek(lexer.TLeftParen)
			if p.peekTokenIs(lexer.TSelect) {
				p.nextToken()
				statement, err := p.parseSelectSubquery()
				if err != nil {
					return nil, err
				}
				p.expectPeek(lexer.TRightParen)

				return &ast.ExprInSubqueryLogicalOperator{
					TestExpression: left,
					Not:            true,
					Subquery:       &statement,
				}, nil
			} else if p.peekTokenIs(lexer.TIdentifier) ||
				p.peekTokenIs(lexer.TLocalVariable) ||
				p.peekTokenIs(lexer.TQuotedIdentifier) ||
				p.peekTokenIs(lexer.TStringLiteral) ||
				p.peekTokenIs(lexer.TNumericLiteral) {
				stmt, err := p.parseExpressionList()
				if err != nil {
					return nil, err
				}
				p.expectPeek(lexer.TRightParen)

				return &ast.ExprInLogicalOperator{
					TestExpression: left,
					Not:            true,
					Expressions:    stmt.List,
				}, nil
			}
			p.errorToken = ETCurrent
			return nil, p.currentErrorString("(Subquery or Expression List) after 'NOT IN' keyword")
		} else if p.currentTokenIs(lexer.TLike) {
			precedence := checkPrecedence(p.currentToken.Type)
			p.nextToken()

			right, err := p.parseExpression(precedence)
			if err != nil {
				return nil, err
			}

			return &ast.ExprLikeLogicalOperator{
				MatchExpression: left,
				Not:             true,
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
