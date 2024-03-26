package lexer

import (
	"strings"
	"testing"
)

func TestBasic(t *testing.T) {
	expected := []Token{
		{Type: TSelect, Value: "select"},
		{Type: TDistinct, Value: "distinct"},
		{Type: TAsterisk, Value: "*"},
		{Type: TComma, Value: ","},
		{Type: TLocalVariable, Value: "hello"},
		{Type: TComma, Value: ","},
		{Type: TQuotedIdentifier, Value: "yes"},
		{Type: TComma, Value: ","},
		{Type: TNumericLiteral, Value: "3.555"},
		{Type: TComma, Value: ","},
		{Type: TStringLiteral, Value: "literal"},
		{Type: TFrom, Value: "from"},
		{Type: TIdentifier, Value: "testtable"},
		{Type: TWhere, Value: "where"},
	}
	_ = expected

	lexer := NewLexer(" SELect     distinct *, @hello, [yes], 3.555, 'literal' FROM testtable where ")

	lexed := []Token{}
	current := lexer.NextToken()
	t.Logf("current: %v\n", current.Value)

	for current.Type != TEndOfFile {
		lexed = append(lexed, current)

		current = lexer.NextToken()
	}

	if len(lexed) != len(expected) {
		t.Fatalf("expected %d tokens, got %d", len(expected), len(lexed))
	}

	for i, token := range lexed {
		if token.Type != expected[i].Type {
			t.Fatalf("expected %v, got %v", expected[i].Type, token.Type)
		}
        lowercase := strings.ToLower(token.Value)
		if lowercase != expected[i].Value {
			t.Fatalf("expected %s, got %s", expected[i].Value, lowercase)
		}
	}

}
