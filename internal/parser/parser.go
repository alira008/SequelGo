package parser

import "SequelGo/internal/lexer"

type Parser struct {
	l        *lexer.Lexer
	currentToken lexer.Token
	peekToken    lexer.Token
}

func New(lexer *lexer.Lexer) *Parser {
	parser := &Parser{l: lexer}
	return parser
}

func (p *Parser) nextToken() {
	p.currentToken = p.peekToken
	p.peekToken = p.l.NextToken()
}
