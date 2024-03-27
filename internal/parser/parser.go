package parser

import (
	"SequelGo/internal/ast"
	"SequelGo/internal/lexer"
	"fmt"
)

type Parser struct {
	l            *lexer.Lexer
	currentToken lexer.Token
	peekToken    lexer.Token
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
}

func (p *Parser) currentTokenIs(t lexer.TokenType) bool {
	return p.currentToken.Type == t
}

func (p *Parser) peekTokenIs(t lexer.TokenType) bool {
	return p.peekToken.Type == t
}

func (p *Parser) expectPeek(t lexer.TokenType) bool {
	if p.peekToken.Type == t {
		p.nextToken()
		return true
	} else {
		p.peekError(t)
		return false
	}
}

func (p *Parser) expectPeekMany(ts []lexer.TokenType) bool {
	for _, t := range ts {
		if p.peekToken.Type == t {
			p.nextToken()
			return true
		}
	}

	return false
}

func (p *Parser) peekError(t lexer.TokenType) {
	msg := fmt.Sprintf("expected next token to be %d, got %d instead", t, p.peekToken.Type)
	p.errors = append(p.errors, msg)
}

func (p *Parser) Errors() []string {
	return p.errors
}

func (p *Parser) Parse() ast.Query {
	query := ast.Query{Statements: []ast.Statement{}}

	for p.currentToken.Type != lexer.TEndOfFile {
		stmt, err := p.parseStatement()

		if err != nil {
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
		body := p.parseSelectBody()
		if body != nil {
			return &ast.SelectStatement{SelectBody: body}, nil
		}

		return nil, fmt.Errorf("error parsing select statement")
	default:
		return nil, fmt.Errorf("unknown statement type")
	}
}

func (p *Parser) parseSelectStatement() *ast.SelectStatement {

	return &ast.SelectStatement{}
}

func (p *Parser) parseSelectBody() *ast.SelectBody {
	selectItems := p.parseSelectItems()
	tableObject := p.parseTableObject()
	whereExpression := p.parseWhereExpression()

	stmt := &ast.SelectBody{SelectItems: selectItems, TableObject: tableObject, WhereClause: whereExpression}
	return stmt
}

func (p *Parser) parseSelectItems() *[]ast.Expression {
	items := &[]ast.Expression{}

	for {
		if !p.expectPeekMany([]lexer.TokenType{lexer.TIdentifier, lexer.TNumericLiteral, lexer.TStringLiteral, lexer.TAsterisk, lexer.TLocalVariable, lexer.TQuotedIdentifier, lexer.TAsterisk}) {
			fmt.Printf("expected identifier, got %s\n", p.peekToken.Value)
			return nil
		}

		expr := p.parseExpression(PrecedenceLowest)
		*items = append(*items, expr)

		if p.peekToken.Type != lexer.TComma {
			break
		}

		p.nextToken()
	}

	return items
}

func (p *Parser) parseTableObject() ast.Expression {
	if !p.expectPeek(lexer.TFrom) {
		fmt.Printf("expected FROM, got %s\n", p.peekToken.Value)
		return nil
	}

	if !p.expectPeekMany([]lexer.TokenType{lexer.TIdentifier, lexer.TLocalVariable}) {
		fmt.Printf("expected identifier, got %s\n", p.peekToken.Value)
	}

	tableObject := p.parseExpression(PrecedenceLowest)

	return tableObject
}

func (p *Parser) parseWhereExpression() ast.Expression {
	fmt.Printf("parsing where\n")
	if !p.peekTokenIs(lexer.TWhere) {
		return nil
	}

	// go to where token
	p.nextToken()
	p.nextToken()

	return p.parseExpression(PrecedenceLowest)
}

func (p *Parser) parseExpression(precedence Precedence) ast.Expression {
	leftExpr := p.parsePrefixExpression()
    fmt.Printf("left expression: %v\n", leftExpr)

	// parse infix sql expressions using stacks to keep track of precedence
	for precedence < checkPrecedence(p.peekToken.Type) {
		p.nextToken()

		leftExpr = p.parseInfixExpression(leftExpr)
	}

	return leftExpr
}

func (p *Parser) parsePrefixExpression() ast.Expression {
	fmt.Printf("parsing prefix expression\n")
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
		default:
			return nil
		}

		if p.peekToken.Type == lexer.TPeriod {
			// we are dealing with a qualified identifier
			compound := &[]ast.Expression{newExpr}
			fmt.Printf("parsing compound identifier\n")

			// go to period token
			p.nextToken()
			fmt.Printf("current token: %v\n", p.currentToken)

			for {
				if !p.expectPeekMany([]lexer.TokenType{lexer.TIdentifier, lexer.TQuotedIdentifier, lexer.TAsterisk}) {
					fmt.Printf("expected identifier, got %s\n", p.peekToken.Value)
					return nil
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

	default:
		return nil
	}

	return newExpr

}

func (p *Parser) parseInfixExpression(left ast.Expression) ast.Expression {
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

		right := p.parseExpression(precedence)
		if right == nil {
			return nil
		}
        fmt.Printf("left expression: %v\n", left)
        fmt.Printf("right expression: %v\n", right)
		return &ast.ExprBinary{
			Left:     left,
			Operator: operator,
			Right:    right,
		}
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
		right := p.parseExpression(precedence)
		return &ast.ExprBinary{
			Left:     left,
			Operator: operator,
			Right:    right,
		}

	}
	return nil
}
