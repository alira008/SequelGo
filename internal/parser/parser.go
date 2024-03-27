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
	// if tableObject == nil {
	// 	fmt.Printf("expected identifier, got %s\n", p.currentToken.Value)
	// 	fmt.Printf("expected identifier, got %v\n", p.peekToken.Type)
	// 	return nil
	// }

	stmt := &ast.SelectBody{SelectItems: selectItems, TableObject: tableObject}
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

func (p *Parser) parseExpression(precedence Precedence) ast.Expression {
	leftExpr := p.parsePrefixExpression()

	// parse infix sql expressions using stacks to keep track of precedence
	for precedence < checkPrecedence(p.peekToken.Type) {
		p.nextToken()

		leftExpr = p.parseInfixExpression(leftExpr)
	}

	return leftExpr
}

func (p *Parser) parsePrefixExpression() ast.Expression {
	fmt.Printf("parsing prefix expression\n")
	fmt.Printf("current token: %v\n", p.currentToken)
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

                fmt.Printf("current token: %v\n", p.currentToken)
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
	return nil
}
