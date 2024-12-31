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
	logger    *zap.SugaredLogger
	l         *lexer.Lexer
	peekToken lexer.Token
	errors    []string
	Comments  []ast.Comment
}

func NewParser(logger *zap.SugaredLogger, lexer *lexer.Lexer) *Parser {
	parser := &Parser{logger: logger, l: lexer}

	parser.nextToken()

	return parser
}

func (p *Parser) nextToken() {
	p.peekToken = p.l.NextToken()

	for p.peekTokenIs(lexer.TCommentLine) {
		p.Comments = append(p.Comments, ast.NewComment(p.peekToken))
		p.peekToken = p.l.NextToken()
	}
}

func (p *Parser) peekTokenIs(t lexer.TokenType) bool {
	return p.peekToken.Type == t
}

func (p *Parser) peekTokenIsAny(t []lexer.TokenType) bool {
	for _, token := range t {
		if p.peekToken.Type == token {
			return true
		}
	}

	return false
}

func (p *Parser) peekPrecedence() Precedence {
	return checkPrecedence(p.peekToken.Type)
}

func (p *Parser) consumeKeyword(t lexer.TokenType) (*ast.Keyword, error) {
	if p.peekToken.Type != t {
		return nil, fmt.Errorf("expected keyword, %s", t.String())
	}

	kw, err := ast.NewKeywordFromTokenNew(p.peekToken)
	if err != nil {
		return nil, err
	}
	p.nextToken()

	return kw, nil
}

func (p *Parser) consumeKeywordAny(tokens []lexer.TokenType) (*ast.Keyword, error) {
	for _, t := range tokens {
		if p.peekTokenIs(t) {
			kw, err := ast.NewKeywordFromTokenNew(p.peekToken)
			if err != nil {
				return nil, err
			}
			p.nextToken()
			return kw, nil
		}
	}

	errorString := "expected either keywords, "
	for i, t := range tokens {
		if i > 0 {
			errorString += ", "
		}
		errorString += fmt.Sprintf("%s", t.String())
	}
	return nil, fmt.Errorf(errorString)
}

func (p *Parser) consumeToken(t lexer.TokenType) (*lexer.Token, error) {
	if p.peekToken.Type != t {
		return nil, fmt.Errorf("expected token, %s", t.String())
	}

	token := p.peekToken
	p.nextToken()

	return &token, nil
}

func (p *Parser) consumeTokenAny(tokens []lexer.TokenType) (*lexer.Token, error) {
	for _, t := range tokens {
		if p.peekTokenIs(t) {
			token := p.peekToken
			p.nextToken()
			return &token, nil
		}
	}

	errorString := "expected either tokens, "
	for i, t := range tokens {
		if i > 0 {
			errorString += ", "
		}
		errorString += fmt.Sprintf("%s", t.String())
	}
	return nil, fmt.Errorf(errorString)
}

func (p *Parser) maybeKeyword(t lexer.TokenType) *ast.Keyword {
	if p.peekToken.Type != t {
		return nil
	}

	kw, err := ast.NewKeywordFromTokenNew(p.peekToken)
	if err != nil {
		return nil
	}
	p.nextToken()

	return kw
}

func (p *Parser) maybeToken(t lexer.TokenType) *lexer.Token {
	if p.peekToken.Type != t {
		return nil
	}

	token := p.peekToken
	p.nextToken()

	return &token
}

func (p *Parser) expectPeek(t lexer.TokenType) error {
	if p.peekToken.Type == t {
		return nil
	}

	return p.peekErrorString(t.String())
}

func (p *Parser) expectPeekMany(ts []lexer.TokenType) error {
	for _, t := range ts {
		if p.peekToken.Type == t {
			return nil
		}
	}

	return p.peekErrorMany(ts)
}

func (p *Parser) expectTokenMany(ts []lexer.TokenType) (lexer.Token, error) {
	for _, t := range ts {
		if p.peekToken.Type == t {
			token := p.peekToken
			return token, nil
		}
	}

	return lexer.Token{}, p.peekErrorMany(ts)
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

	arrows := ""
	if p.peekToken.Start.Col > 0 {
		for i := uint(0); i < p.peekToken.Start.Col-1; i++ {
			arrows += " "
		}
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

func (p *Parser) peekErrorString(expected string) error {
	arrows := ""
	if p.peekToken.Start.Col > 0 {
		for i := uint(0); i < p.peekToken.Start.Col-1; i++ {
			arrows += " "
		}
	}
	for i := p.peekToken.Start.Col; i <= p.peekToken.End.Col; i++ {
		arrows += "^"
	}
	return fmt.Errorf(
		"expected (%s) got (%s) instead\n%s\n%s",
		expected,
        p.peekToken.Type.String(),
		p.l.CurrentLine(),
		arrows,
	)
}

func (p *Parser) Errors() []string {
	return p.errors
}

func (p *Parser) Parse() ast.Query {
	query := ast.Query{}
	queryStartPosition := p.peekToken.Start

	for !p.peekTokenIs(lexer.TEndOfFile) {
		startPosition := p.peekToken.Start
		stmt, err := p.parseStatement()
		endPosition := p.peekToken.End

		if err != nil {
			errMsg := fmt.Sprintf("[Error Line: %d Col: %d]: %s", p.peekToken.End.Line,
				p.peekToken.End.Col+1, err.Error())
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
	queryEndPosition := p.peekToken.End
	query.SetSpan(ast.NewSpanFromLexerPosition(queryStartPosition, queryEndPosition))
	return query
}

var select_item_type_start = []lexer.TokenType{
	lexer.TIdentifier,
	lexer.TQuotedIdentifier,
	lexer.TNumericLiteral,
	lexer.TStringLiteral,
	lexer.TLocalVariable,
	lexer.TLeftParen,
	lexer.TCase,
	lexer.TAsterisk,
	lexer.TMinus,
	lexer.TPlus,
}

func (p *Parser) expectSelectItemStart() error {
	if p.peekToken.Type.IsBuiltinFunction() {
		return nil
	}

	for _, t := range select_item_type_start {
		if p.peekToken.Type == t {
			return nil
		}
	}

    return p.peekErrorMany(select_item_type_start)
}

var table_source_start = []lexer.TokenType{
	lexer.TIdentifier,
	lexer.TQuotedIdentifier,
	lexer.TLocalVariable,
	lexer.TLeftParen,
}

func (p *Parser) expectTableSourceStart() error {
	for _, t := range table_source_start {
		if p.peekToken.Type == t {
			return nil
		}
	}

    return p.peekErrorMany(table_source_start)
}

var group_by_start = []lexer.TokenType{
	lexer.TIdentifier,
	lexer.TQuotedIdentifier,
	lexer.TLocalVariable,
	lexer.TNumericLiteral,
}

func (p *Parser) expectGroupByStart() error {
	for _, t := range group_by_start {
		if p.peekToken.Type == t {
			return nil
		}
	}

    return p.peekErrorMany(group_by_start)
}

var expression_list_start = []lexer.TokenType{
	lexer.TIdentifier,
	lexer.TQuotedIdentifier,
	lexer.TLocalVariable,
	lexer.TNumericLiteral,
	lexer.TStringLiteral,
}

func (p *Parser) expectExpressionListStart() error {
	for _, t := range expression_list_start {
		if p.peekToken.Type == t {
			return nil
		}
	}

    return p.peekErrorMany(expression_list_start)
}

var function_args_start = append([]lexer.TokenType{
	lexer.TIdentifier,
	lexer.TQuotedIdentifier,
	lexer.TLocalVariable,
	lexer.TNumericLiteral,
	lexer.TStringLiteral,
}, ast.BuiltinFunctionsTokenType...)

func (p *Parser) expectFunctionArgsStart() error {
	for _, t := range function_args_start {
		if p.peekToken.Type == t {
			return nil
		}
	}

    return p.peekErrorMany(function_args_start)
}

var compound_identifier_start = []lexer.TokenType{
	lexer.TAsterisk,
	lexer.TQuotedIdentifier,
	lexer.TIdentifier,
}

func (p *Parser) expectCompoundIdentifierStart() error {
	for _, t := range compound_identifier_start {
		if p.peekToken.Type == t {
			return nil
		}
	}

    return p.peekErrorMany(compound_identifier_start)
}
