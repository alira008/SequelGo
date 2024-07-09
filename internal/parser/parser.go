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
	ETPeak
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

	p.errorToken = ETPeak
	return p.peekError(t)
}

func (p *Parser) expectPeekMany(ts []lexer.TokenType) error {
	for _, t := range ts {
		if p.peekToken.Type == t {
			p.nextToken()
			return nil
		}
	}

	p.errorToken = ETPeak
	return p.peekErrorMany(ts)
}

func (p *Parser) peekError(t lexer.TokenType) error {
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

	return fmt.Errorf(
		"expected (%s) got (%s) instead\n%s",
		strings.Join(expectedTokenTypes, " or "),
		p.peekToken.Type.String(),
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

func (p *Parser) parseSelectBody() (ast.SelectBody, error) {
	stmt := ast.SelectBody{}
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
	err := p.expectPeekMany([]lexer.TokenType{lexer.TIdentifier,
		lexer.TNumericLiteral,
		lexer.TStringLiteral,
		lexer.TAsterisk,
		lexer.TLocalVariable,
		lexer.TLeftParen,
		lexer.TSum,
		lexer.TQuotedIdentifier})
	if err != nil {
		return stmt, err
	}

	stmt.SelectItem, err = p.parseExpression(PrecedenceLowest)
	if err != nil {
		return stmt, err
	}

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

	if expr == nil {
		return nil, fmt.Errorf("Expected an expression after where")
	}
	return expr, nil
}

func (p *Parser) parseExpressionList() (ast.ExprExpressionList, error) {
	items := []ast.Expression{}
	expressionList := ast.ExprExpressionList{List: items}

	for {
		err := p.expectPeekMany([]lexer.TokenType{lexer.TIdentifier,
			lexer.TIdentifier,
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
		items = append(items, expr)

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

		if p.peekToken.Type == lexer.TAs || p.peekToken.Type == lexer.TStringLiteral {
			expr := &ast.ExprWithAlias{AsTokenPresent: false, Expression: newExpr}
			if p.peekToken.Type == lexer.TAs {
				expr.AsTokenPresent = true
				p.nextToken()
			}
			err := p.expectPeek(lexer.TStringLiteral)
			if err != nil {
				return nil, err
			}
			expr.Alias = p.currentToken.Value
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
			p.peekTokenIs(lexer.TNumericLiteral) {
			stmt, err := p.parseExpressionList()
			if err != nil {
				return nil, err
			}
			p.expectPeek(lexer.TRightParen)

			return &stmt, nil
		}
	default:
		p.errorToken = ETCurrent
		return nil, fmt.Errorf("Unimplemented expression %s", p.currentToken.Type.String())
	}

	return newExpr, nil

}

func (p *Parser) parseInfixExpression(left ast.Expression) (ast.Expression, error) {
	fmt.Printf("parsing infix expression\n")
	switch p.currentToken.Type {
	case lexer.TPlus,
		lexer.TMinus,
		lexer.TAsterisk,
		lexer.TDivide,
		lexer.TAnd,
		lexer.TOr:
		var operator ast.OperatorType
		switch p.currentToken.Type {
		case lexer.TPlus:
			operator = ast.OpPlus
		case lexer.TMinus:
			operator = ast.OpMinus
		case lexer.TAsterisk:
			operator = ast.OpMult
		case lexer.TDivide:
			operator = ast.OpDiv
		case lexer.TAnd:
			operator = ast.OpAnd
		case lexer.TOr:
			operator = ast.OpOr
		}
		precedence := checkPrecedence(p.currentToken.Type)
		p.nextToken()

		right, err := p.parseExpression(precedence)
		if err != nil {
			return nil, err
		}
		fmt.Printf("left expression: %v\n", left)
		fmt.Printf("right expression: %v\n", right)
		return &ast.ExprBinary{
			Left:     left,
			Operator: operator,
			Right:    right,
		}, nil
	case lexer.TEqual,
		lexer.TNotEqual,
		lexer.TGreaterThan,
		lexer.TLessThan,
		lexer.TGreaterThanEqual,
		lexer.TLessThanEqual:

		var operator ast.OperatorType
		switch p.currentToken.Type {
		case lexer.TEqual:
			operator = ast.OpEqual
		case lexer.TNotEqual:
			operator = ast.OpNotEqual
		case lexer.TGreaterThan:
			operator = ast.OpGreater
		case lexer.TLessThan:
			operator = ast.OpLess
		case lexer.TGreaterThanEqual:
			operator = ast.OpGreaterEqual
		case lexer.TLessThanEqual:
			operator = ast.OpLessEqual
		}
		precedence := checkPrecedence(p.currentToken.Type)
		p.nextToken()

		// parse the expression of the operator
		right, err := p.parseExpression(precedence)
		if err != nil {
			return nil, err
		}
		return &ast.ExprBinary{
			Left:     left,
			Operator: operator,
			Right:    right,
		}, nil

	}
	p.errorToken = ETCurrent
	return nil, fmt.Errorf("Unimplemented expression %s", p.currentToken.Type.String())
}
