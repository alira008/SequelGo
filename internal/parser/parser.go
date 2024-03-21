package parser

import (
	"SequelGo/internal/ast"
	"SequelGo/internal/lexer"
	"fmt"
	"strconv"
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

func (p *Parser) expectMany(ts []lexer.TokenType) bool {
	for _, t := range ts {
		if p.peekToken.Type == t {
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
			continue
		}
		fmt.Printf("yayayayaya\n")

		query.Statements = append(query.Statements, stmt)

		p.nextToken()
	}
	return query
}

func (p *Parser) parseStatement() (ast.Statement, error) {
	switch p.currentToken.Type {
	case lexer.TSelect:
		stmt := p.parseSelectStatement()
		if stmt == nil {
			return nil, fmt.Errorf("error parsing select statement")
		}
		return stmt, nil
	default:
		return nil, fmt.Errorf("unknown statement type")
	}
}

func (p *Parser) parseSelectStatement() *ast.SelectStatement {
	selectItems := p.parseSelectItems()
	tableObject := p.parseTableObject()
	if tableObject == nil {
		fmt.Printf("expected identifier, got %s\n", p.currentToken.Value)
		fmt.Printf("expected identifier, got %v\n", p.peekToken.Type)
		return nil
	}

	stmt := &ast.SelectStatement{SelectItems: selectItems, TableObject: tableObject}
	return stmt
}

func (p *Parser) parseSelectItems() *[]*ast.Expr {
	items := &[]*ast.Expr{}

	for {

		if !p.expectMany([]lexer.TokenType{lexer.TIdentifier, lexer.TNumericLiteral, lexer.TStringLiteral, lexer.TAsterisk, lexer.TLocalVariable, lexer.TQuotedStringLiteral}) {
			fmt.Printf("expected identifier, got %s\n", p.peekToken.Value)
			return nil
		}
		p.nextToken()

		expr := p.parseExpression(PrecedenceLowest)
		*items = append(*items, expr)

		if p.peekToken.Type != lexer.TComma {
			break
		}
		p.nextToken()
	}

	return items
}

func (p *Parser) parseTableObject() *ast.Expr {
	if !p.expectPeek(lexer.TFrom) {
		return nil
	}

	return nil
}

func (p *Parser) parseExpression(precedence Precedence) *ast.Expr {
	leftExpr := p.parsePrefixExpression()

	// parse infix sql expressions using stacks to keep track of precedence
	for precedence < checkPrecedence(p.peekToken.Type) {
		p.nextToken()

		leftExpr = p.parseInfixExpression(leftExpr)
	}

	return leftExpr
}

func (p *Parser) parsePrefixExpression() *ast.Expr {
	switch p.currentToken.Type {
	case lexer.TIdentifier, lexer.TNumericLiteral, lexer.TStringLiteral, lexer.TAsterisk, lexer.TLocalVariable, lexer.TQuotedStringLiteral:
		newExpr := &ast.Expr{}
		switch p.currentToken.Type {
		case lexer.TLocalVariable:
			newExpr.ExprType = ast.ExprLocalVar
			newExpr.Identifier = p.currentToken.Value
		case lexer.TQuotedStringLiteral:
			newExpr.ExprType = ast.ExprLiteralQuotedString
			newExpr.QuotedStringLiteral = p.currentToken.Value
		case lexer.TStringLiteral:
			newExpr.ExprType = ast.ExprLiteralString
			newExpr.StringLiteral = p.currentToken.Value
		case lexer.TNumericLiteral:
			newExpr.ExprType = ast.ExprLiteralNumber
			n, err := strconv.ParseFloat(p.currentToken.Value, 64)
			if err != nil {
				return nil
			}
			newExpr.NumberLiteral = n
		case lexer.TIdentifier:
			newExpr.ExprType = ast.ExprIdentifier
			newExpr.Identifier = p.currentToken.Value
		case lexer.TAsterisk:
			newExpr.ExprType = ast.ExprStar
		default:
			return nil
		}
		return newExpr
	}

	return nil
}

func (p *Parser) parseInfixExpression(left ast.Expression) *ast.Expr {
	return nil
}
