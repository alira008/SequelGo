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

func (p *Parser) peekError(t lexer.TokenType) {
	msg := fmt.Sprintf("expected next token to be %d, got %d instead", t, p.peekToken.Type)
	p.errors = append(p.errors, msg)
}

func (p *Parser) Errors() []string {
	return p.errors
}

func (p *Parser) Parse() ast.Query {
	query := ast.Query{}

	for p.currentToken.Type != lexer.TEndOfFile {
		stmt := p.parseStatement()

		if stmt != nil {
			query.Statements = append(query.Statements, stmt)
		}

		p.nextToken()
	}

	return query
}

func (p *Parser) parseStatement() ast.Statement {
	switch p.currentToken.Type {
	case lexer.TSelect:
		return p.parseSelectStatement()
	default:
		return nil
	}
}

func (p *Parser) parseSelectStatement() *ast.SelectStatement {
	p.nextToken()
	expr := p.parseExpression(PrecedenceLowest)
	stmt := &ast.SelectStatement{SelectItems: &[]*ast.Expr{expr}}
	return stmt
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
	case lexer.TIdentifier:
		fallthrough
	case lexer.TNumericLiteral:
		fallthrough
	case lexer.TStringLiteral:
		fallthrough
	case lexer.TAsterisk:
		fallthrough
	case lexer.TLocalVariable:
		fallthrough
	case lexer.TQuotedStringLiteral:
		return &ast.Expr{ExprType: ast.ExprLiteralString, StringLiteral: p.currentToken.Value}
	}

	return nil
}

func (p *Parser) parseInfixExpression(left ast.Expression) *ast.Expr {
	return nil
}
