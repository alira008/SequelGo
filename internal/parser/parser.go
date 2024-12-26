package parser

import (
	"SequelGo/internal/ast"
	"SequelGo/internal/lexer"

	// "encoding/json"
	"fmt"
	"strings"

	"go.uber.org/zap"
)

type ErrorToken int

const (
	ETCurrent ErrorToken = iota
	ETPeek
	ETNone
)

type Parser struct {
	logger           *zap.SugaredLogger
	l                *lexer.Lexer
	currentToken     lexer.Token
	peekToken        lexer.Token
	peekToken2       lexer.Token
	errorToken       ErrorToken
	errors           []string
	trailingComments []ast.Comment
	leadingComments  []ast.Comment
	afterComments    []ast.Comment
	Comments         []ast.Comment
}

func NewParser(logger *zap.SugaredLogger, lexer *lexer.Lexer) *Parser {
	parser := &Parser{logger: logger, l: lexer}

	parser.nextToken()
	parser.nextToken()
	parser.nextToken()

	return parser
}

func (p *Parser) nextToken() {
	p.currentToken = p.peekToken
	p.peekToken = p.peekToken2
	p.peekToken2 = p.l.NextToken()

	for p.peekToken2Is(lexer.TCommentLine) {
		if p.isLeadingComment(p.currentToken, p.peekToken2) {
			p.leadingComments = append(p.leadingComments, ast.NewComment(p.peekToken2))
		} else if p.isTrailingComment(p.currentToken, p.peekToken2) {
			p.trailingComments = append(p.trailingComments, ast.NewComment(p.peekToken2))
		} else {
			p.afterComments = append(p.afterComments, ast.NewComment(p.peekToken2))
		}
		p.Comments = append(p.Comments, ast.NewComment(p.peekToken2))
		p.peekToken2 = p.l.NextToken()
	}
	p.errorToken = ETNone
}

func (p *Parser) currentTokenIs(t lexer.TokenType) bool {
	return p.currentToken.Type == t
}

func (p *Parser) peekTokenIs(t lexer.TokenType) bool {
	return p.peekToken.Type == t
}

func (p *Parser) peekToken2Is(t lexer.TokenType) bool {
	return p.peekToken2.Type == t
}

func (p *Parser) peekTokenIsAny(t []lexer.TokenType) bool {
	for _, token := range t {
		if p.peekToken.Type == token {
			return true
		}
	}

	return false
}

func (p *Parser) peekToken2IsAny(t []lexer.TokenType) bool {
	for _, token := range t {
		if p.peekToken2.Type == token {
			return true
		}
	}

	return false
}

func (p *Parser) consumeKeyword(t lexer.TokenType) (*ast.Keyword, error) {
	if p.peekToken.Type == t {
		p.nextToken()
		return nil, fmt.Errorf("expected keyword, %s", t.String())
	}

	p.errorToken = ETPeek
	return nil, fmt.Errorf("expected keyword, %s", t.String())
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
	arrows := ""
	for i := 0; i < p.currentToken.Start.Col-1; i++ {
		arrows += " "
	}
	for i := p.currentToken.Start.Col; i <= p.currentToken.End.Col; i++ {
		arrows += "^"
	}
	return fmt.Errorf(
		"expected (%s) got (%s) nstead\n%s\n%s",
		t.String(),
		p.peekToken.Type.String(),
		p.l.CurrentLine(),
		arrows,
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
	arrows := ""
	for i := 0; i < p.peekToken.Start.Col-1; i++ {
		arrows += " "
	}
	for i := p.peekToken.Start.Col; i <= p.peekToken.End.Col; i++ {
		arrows += "^"
	}
	return fmt.Errorf(
		"expected (%s) got (%s) instead\n%s\n%s",
		strings.Join(expectedTokenTypes, " or "),
		p.peekToken.Type.String(),
		p.l.CurrentLine(),
		arrows,
	)
}

func (p *Parser) currentErrorString(expected string) error {
	p.errorToken = ETCurrent
	arrows := ""
	for i := 0; i < p.currentToken.Start.Col-1; i++ {
		arrows += " "
	}
	for i := p.currentToken.Start.Col; i <= p.currentToken.End.Col; i++ {
		arrows += "^"
	}
	return fmt.Errorf(
		"expected (%s) got (%s) instead\n%s",
		expected,
		p.l.CurrentLine(),
		arrows,
	)
}

func (p *Parser) isLeadingComment(current, comment lexer.Token) bool {
	return comment.Start.Line < current.Start.Line
}

func (p *Parser) isTrailingComment(current, comment lexer.Token) bool {
	return comment.Start.Line == current.Start.Line && comment.Start.Col > current.Start.Col
}

func (p *Parser) popLeadingComments() []ast.Comment {
	leading := p.leadingComments
	if leading != nil {
		p.leadingComments = p.afterComments
		p.afterComments = nil
	}
	return leading
}

func (p *Parser) popTrailingComments() []ast.Comment {
	trailing := p.trailingComments
	p.trailingComments = nil
	return trailing
}

func (p *Parser) Errors() []string {
	return p.errors
}

func (p *Parser) Parse() ast.Query {
	query := ast.Query{}

	for p.currentToken.Type != lexer.TEndOfFile {
		startPosition := p.currentToken.Start
		stmt, err := p.parseStatement()
		endPosition := p.currentToken.End

		if err != nil {
			var errMsg string
			if p.errorToken == ETCurrent {
				errMsg = fmt.Sprintf("[Error Line: %d Col: %d]: %s", p.currentToken.End.Line,
					p.currentToken.End.Col+1, err.Error())
			} else {
				errMsg = fmt.Sprintf("[Error Line: %d Col: %d]: %s", p.peekToken.End.Line,
					p.peekToken.End.Col+1, err.Error())
			}
			p.errors = append(p.errors, errMsg)
			p.nextToken()
			continue
		}
		if stmt != nil {
			stmt.SetSpan(ast.NewSpanFromLexerPosition(startPosition, endPosition))
			query.Statements = append(query.Statements, stmt)
		}

		p.nextToken()
	}
	return query
}
