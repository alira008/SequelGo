package parser

import (
	"SequelGo/internal/ast"
	"SequelGo/internal/lexer"
	"fmt"
	"strings"
)

type ErrorToken int

const (
	ETCurrent ErrorToken = iota
	ETPeek
	ETNone
)

type Parser struct {
	l            *lexer.Lexer
	currentToken lexer.Token
	peekToken    lexer.Token
	errorToken   ErrorToken
	errors       []string
}

func NewParser(lexer *lexer.Lexer) *Parser {
	parser := &Parser{l: lexer}

	parser.nextToken()
	parser.nextToken()

	return parser
}

func (p *Parser) nextToken() {
	p.currentToken = p.peekToken
	p.peekToken = p.l.NextToken()
	p.errorToken = ETNone
}

func (p *Parser) currentTokenIs(t lexer.TokenType) bool {
	return p.currentToken.Type == t
}

func (p *Parser) peekTokenIs(t lexer.TokenType) bool {
	return p.peekToken.Type == t
}

func (p *Parser) expectPeek(t lexer.TokenType) error {
	if p.peekToken.Type == t {
		p.nextToken()
		return nil
	}

	p.errorToken = ETPeek
	return p.peekError(t)
}

func (p *Parser) expectPeekMany(ts []lexer.TokenType) error {
	for _, t := range ts {
		if p.peekToken.Type == t {
			p.nextToken()
			return nil
		}
	}

	p.errorToken = ETPeek
	return p.peekErrorMany(ts)
}

func (p *Parser) peekError(t lexer.TokenType) error {
	p.errorToken = ETPeek
	return fmt.Errorf(
		"expected (%s) got (%s) instead\n%s",
		t.String(),
		p.peekToken.Type.String(),
		p.l.CurrentLine(),
	)
}

func (p *Parser) peekErrorMany(ts []lexer.TokenType) error {
	var expectedTokenTypes []string
	builtinFuncPresent := false
	for _, t := range ts {
		if t.IsBuiltinFunction() && !builtinFuncPresent {
			builtinFuncPresent = true
			expectedTokenTypes = append(expectedTokenTypes, fmt.Sprintf("Builtin Functions"))
			continue
		} else if t.IsBuiltinFunction() && builtinFuncPresent {
			continue
		}

		expectedTokenTypes = append(expectedTokenTypes, fmt.Sprintf("%s", t.String()))
	}

	p.errorToken = ETPeek
	return fmt.Errorf(
		"expected (%s) got (%s) instead\n%s",
		strings.Join(expectedTokenTypes, " or "),
		p.peekToken.Type.String(),
		p.l.CurrentLine(),
	)
}

func (p *Parser) currentErrorString(expected string) error {
	p.errorToken = ETCurrent
	return fmt.Errorf(
		"expected %s\n%s",
		expected,
		p.l.CurrentLine(),
	)
}

func (p *Parser) Errors() []string {
	return p.errors
}

func (p *Parser) Parse() ast.Query {
	query := ast.Query{Statements: []ast.Statement{}}

	for p.currentToken.Type != lexer.TEndOfFile {
		stmt, err := p.parseStatement()

		if err != nil {
			if p.errorToken == ETCurrent {
				fmt.Printf("[Error Line: %d Col: %d]: %s\n", p.currentToken.End.Line,
					p.currentToken.End.Col+1, err.Error())
			} else {
				fmt.Printf("[Error Line: %d Col: %d]: %s\n", p.peekToken.End.Line,
					p.peekToken.End.Col+1, err.Error())
			}
			p.nextToken()
			continue
		}

		query.Statements = append(query.Statements, stmt)

		p.nextToken()
	}
	return query
}

func (p *Parser) parseStatement() (ast.Statement, error) {
	switch p.currentToken.Type {
	case lexer.TSelect:
		body, err := p.parseSelectBody()
		if err == nil {
			return &ast.SelectStatement{SelectBody: &body}, nil
		}

		return nil, err
	default:
		return nil, nil
		// return nil, fmt.Errorf("unknown statement type %s", p.currentToken.Value)
	}
}

func (p *Parser) parseSelectStatement() *ast.SelectStatement {

	return &ast.SelectStatement{}
}

func (p *Parser) parseTopArg() (*ast.TopArg, error) {
	p.nextToken()
	p.nextToken()

	expr, err := p.parseExpression(PrecedenceLowest)
	if err != nil {
		return nil, err
	}

	topArg := ast.TopArg{Quantity: expr}
	if p.peekTokenIs(lexer.TPercent) {
		topArg.Percent = true
		p.nextToken()
	}

	if p.peekTokenIs(lexer.TWith) {
		p.nextToken()

		err = p.expectPeek(lexer.TTies)
		if err != nil {
			return nil, err
		}
		topArg.Percent = true
		p.nextToken()
	}

	return &topArg, nil
}

func (p *Parser) parseSelectBody() (ast.SelectBody, error) {
	stmt := ast.SelectBody{}
	if p.peekTokenIs(lexer.TDistinct) {
		stmt.Distinct = true
		p.nextToken()
	}

	// check for optional all keyword
	if p.peekTokenIs(lexer.TAll) {
		p.nextToken()
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
	stmt.SelectItems = selectItems

	tableObject, err := p.parseTableObject()
	if err != nil {
		return stmt, err
	}
	stmt.TableObject = tableObject

	whereExpression, err := p.parseWhereExpression()
	if err != nil {
		return stmt, err
	}
	stmt.WhereClause = whereExpression

	return stmt, nil
}

func (p *Parser) parseSelectSubquery() (ast.ExprSubquery, error) {
	stmt := ast.ExprSubquery{}

	selectItems, err := p.parseSelectItems()
	if err != nil {
		return stmt, err
	}

	stmt.SelectItems = selectItems

	tableObject, err := p.parseTableObject()
	if err != nil {
		return stmt, err
	}
	stmt.TableObject = tableObject

	whereExpression, err := p.parseWhereExpression()
	if err != nil {
		return stmt, err
	}
	stmt.WhereClause = whereExpression

	return stmt, nil
}

func (p *Parser) parseSelectItems() ([]ast.Expression, error) {
	items := []ast.Expression{}

	for {
		err := p.expectPeekMany([]lexer.TokenType{lexer.TIdentifier,
			lexer.TNumericLiteral,
			lexer.TStringLiteral,
			lexer.TAsterisk,
			lexer.TLocalVariable,
			lexer.TLeftParen,
			lexer.TMinus,
			lexer.TPlus,
			// rework checking keywords
			lexer.TSum,
			lexer.TQuotedIdentifier})
		if err != nil {
			return items, err
		}

		expr, err := p.parseExpression(PrecedenceLowest)
		if err != nil {
			return items, err
		}
		switch v := expr.(type) {
		case *ast.ExprSubquery:
			if len(v.SelectItems) > 1 {
				return items, fmt.Errorf("Subquery must contain only one column")
			}
			break
		}
		items = append(items, expr)

		if p.peekToken.Type != lexer.TComma {
			break
		}

		p.nextToken()
	}

	return items, nil
}

func (p *Parser) parseTableObject() (ast.Expression, error) {
	err := p.expectPeek(lexer.TFrom)
	if err != nil {
		return nil, err
	}

	err = p.expectPeekMany([]lexer.TokenType{lexer.TIdentifier, lexer.TLocalVariable})
	if err != nil {
		return nil, err
	}

	tableObject, err := p.parseExpression(PrecedenceLowest)
	if err != nil {
		return tableObject, err
	}

	return tableObject, nil
}

func (p *Parser) parseWhereExpression() (ast.Expression, error) {
	fmt.Printf("parsing where\n")
	if !p.peekTokenIs(lexer.TWhere) {
		return nil, nil
	}

	// go to where token
	p.nextToken()
	p.nextToken()
	expr, err := p.parseExpression(PrecedenceLowest)
	if err != nil {
		return expr, err
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
		return nil, fmt.Errorf("expected expression after 'WHERE' keyword")
	}
	return expr, nil
}

func (p *Parser) parseExpressionList() (ast.ExprExpressionList, error) {
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

	return expressionList, nil
}

func (p *Parser) parseExpression(precedence Precedence) (ast.Expression, error) {
	leftExpr, err := p.parsePrefixExpression()
	if err != nil {
		return nil, err
	}

	// parse infix sql expressions using stacks to keep track of precedence
	for precedence < checkPrecedence(p.peekToken.Type) {
		p.nextToken()

		leftExpr, err = p.parseInfixExpression(leftExpr)
		if err != nil {
			return nil, err
		}
	}

	return leftExpr, nil
}

func (p *Parser) parsePrefixExpression() (ast.Expression, error) {
	// fmt.Printf("parsing prefix expression\n")
	var newExpr ast.Expression
	switch p.currentToken.Type {
	case lexer.TIdentifier, lexer.TNumericLiteral, lexer.TStringLiteral, lexer.TAsterisk, lexer.TLocalVariable, lexer.TQuotedIdentifier:
		switch p.currentToken.Type {
		case lexer.TLocalVariable:
			newExpr = &ast.ExprLocalVariable{Value: p.currentToken.Value}
		case lexer.TQuotedIdentifier:
			newExpr = &ast.ExprQuotedIdentifier{Value: p.currentToken.Value}
		case lexer.TStringLiteral:
			newExpr = &ast.ExprStringLiteral{Value: p.currentToken.Value}
		case lexer.TNumericLiteral:
			newExpr = &ast.ExprNumberLiteral{Value: p.currentToken.Value}
		case lexer.TIdentifier:
			newExpr = &ast.ExprIdentifier{Value: p.currentToken.Value}
		case lexer.TAsterisk:
			newExpr = &ast.ExprStar{}
		}

		if p.peekToken.Type == lexer.TPeriod {
			// we are dealing with a qualified identifier
			compound := &[]ast.Expression{newExpr}
			fmt.Printf("parsing compound identifier\n")

			// go to period token
			p.nextToken()
			fmt.Printf("current token: %v\n", p.currentToken)

			for {
				err := p.expectPeekMany([]lexer.TokenType{lexer.TIdentifier, lexer.TQuotedIdentifier, lexer.TAsterisk})
				if err != nil {
					return nil, err
				}
				fmt.Printf("current token: %v\n", p.currentToken)

				if p.currentToken.Type == lexer.TAsterisk {
					expr := &ast.ExprStar{}
					*compound = append(*compound, expr)
					break
				} else if p.currentToken.Type == lexer.TQuotedIdentifier {
					expr := &ast.ExprQuotedIdentifier{Value: p.currentToken.Value}
					*compound = append(*compound, expr)
				} else {
					expr := &ast.ExprIdentifier{Value: p.currentToken.Value}
					*compound = append(*compound, expr)
				}

				if p.peekToken.Type != lexer.TPeriod {
					break
				}

				p.nextToken()
			}

			newExpr = &ast.ExprCompoundIdentifier{Identifiers: *compound}
		}

		if p.peekToken.Type == lexer.TAs ||
			p.peekToken.Type == lexer.TIdentifier ||
			p.peekToken.Type == lexer.TStringLiteral ||
			p.peekToken.Type == lexer.TQuotedIdentifier {
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
				err = fmt.Errorf("Expected (Identifier or StringLiteral or QuotedIdentifier) for Alias")
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
		lexer.TGetdate:
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
		}
		function := &ast.ExprFunction{Type: funcType, Name: &ast.ExprIdentifier{Value: p.currentToken.Value}}
		// parse function arguments
		err := p.expectPeek(lexer.TLeftParen)
		if err != nil {
			return nil, err
		}
		args := []ast.Expression{}
		if p.peekTokenIs(lexer.TRightParen) {
			p.nextToken()
			return &ast.ExprFunctionCall{
				Name: function,
				Args: args,
			}, nil
		}

		for {
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
				args = append(args, &ast.ExprLocalVariable{Value: p.currentToken.Value})
			} else if p.currentToken.Type == lexer.TQuotedIdentifier {
				args = append(args, &ast.ExprQuotedIdentifier{Value: p.currentToken.Value})
			} else if p.currentToken.Type == lexer.TStringLiteral {
				args = append(args, &ast.ExprStringLiteral{Value: p.currentToken.Value})
			} else if p.currentToken.Type == lexer.TNumericLiteral {
				args = append(args, &ast.ExprNumberLiteral{Value: p.currentToken.Value})
			} else {
				args = append(args, &ast.ExprIdentifier{Value: p.currentToken.Value})
			}

			if p.peekTokenIs(lexer.TRightParen) || p.peekTokenIs(lexer.TComma) {
				break
			}
			p.nextToken()
		}

		err = p.expectPeek(lexer.TRightParen)
		if err != nil {
			fmt.Printf("expected right parenthesis, got %s\n", p.peekToken.Value)
			return nil, err
		}

		return &ast.ExprFunctionCall{
			Name: function,
			Args: args,
		}, nil
	case lexer.TLeftParen:
		// start of subquery
		if p.peekTokenIs(lexer.TSelect) {
			p.nextToken()
			statement, err := p.parseSelectSubquery()
			if err != nil {
				return nil, err
			}
			p.expectPeek(lexer.TRightParen)

			return &statement, nil
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
			fmt.Printf("stmt: %v\n", stmt)
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
		// check if it is a subquery
		newExpr = &ast.ExprNotLogicalOperator{
			Expression: expr,
		}
		break
	default:
		p.errorToken = ETCurrent
		return nil, fmt.Errorf("Unimplemented expression %s", p.currentToken.Type.String())
	}

	return newExpr, nil

}

func (p *Parser) parseInfixExpression(left ast.Expression) (ast.Expression, error) {
	fmt.Printf("parsing infix expression, %s\n", p.currentToken.String())
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
		precedence := checkPrecedence(p.currentToken.Type)
		p.nextToken()

		right, err := p.parseExpression(precedence)
		if err != nil {
			return nil, err
		}

		// check if we have and operator
		switch v := right.(type) {
		case *ast.ExprAndLogicalOperator:
			return &ast.ExprBetweenLogicalOperator{
				TestExpression: left,
				Begin:          v.Left,
				End:            v.Right,
			}, nil
		default:
			p.errorToken = ETCurrent
			return nil, p.currentErrorString("Expected 'AND' logical operator after 'BETWEEN' keyword")
		}
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
			precedence := checkPrecedence(p.currentToken.Type)
			p.nextToken()

			right, err := p.parseExpression(precedence)
			if err != nil {
				return nil, err
			}

			// check if we have and operator
			switch v := right.(type) {
			case *ast.ExprAndLogicalOperator:
				return &ast.ExprBetweenLogicalOperator{
					TestExpression: left,
					Not:            true,
					Begin:          v.Left,
					End:            v.Right,
				}, nil
			default:
				p.errorToken = ETCurrent
				return nil, p.currentErrorString("Expected (Subquery) after 'NOT BETWEEN' keyword")
			}
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
