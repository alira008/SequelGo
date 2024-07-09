package lexer

import (
	"strings"
	"testing"
)

func TestBasic(t *testing.T) {
	expected := []Token{
		{Type: TSelect, Value: "select", Start: Position{Line: 0, Col: 2}, End: Position{Line: 0, Col: 7}},
		{Type: TDistinct, Value: "distinct", Start: Position{Line: 0, Col: 13}, End: Position{Line: 0, Col: 20}},
		{Type: TAsterisk, Value: "*", Start: Position{Line: 0, Col: 22}, End: Position{Line: 0, Col: 22}},
		{Type: TComma, Value: ",", Start: Position{Line: 0, Col: 23}, End: Position{Line: 0, Col: 23}},
		{Type: TLocalVariable, Value: "hello", Start: Position{Line: 0, Col: 25}, End: Position{Line: 0, Col: 30}},
		{Type: TComma, Value: ",", Start: Position{Line: 0, Col: 31}, End: Position{Line: 0, Col: 31}},
		{Type: TQuotedIdentifier, Value: "yes", Start: Position{Line: 0, Col: 33}, End: Position{Line: 0, Col: 37}},
		{Type: TComma, Value: ",", Start: Position{Line: 0, Col: 38}, End: Position{Line: 0, Col: 38}},
		{Type: TNumericLiteral, Value: "3.555", Start: Position{Line: 0, Col: 40}, End: Position{Line: 0, Col: 44}},
		{Type: TComma, Value: ",", Start: Position{Line: 0, Col: 45}, End: Position{Line: 0, Col: 45}},
		{Type: TStringLiteral, Value: "literal", Start: Position{Line: 0, Col: 47}, End: Position{Line: 0, Col: 55}},
		{Type: TFrom, Value: "from", Start: Position{Line: 0, Col: 57}, End: Position{Line: 0, Col: 60}},
		{Type: TIdentifier, Value: "testtable", Start: Position{Line: 0, Col: 62}, End: Position{Line: 0, Col: 70}},
		{Type: TWhere, Value: "where", Start: Position{Line: 0, Col: 72}, End: Position{Line: 0, Col: 76}},
		{Type: TAnd, Value: "and", Start: Position{Line: 0, Col: 78}, End: Position{Line: 0, Col: 80}},
		{Type: TStringLiteral, Value: "no", Start: Position{Line: 0, Col: 82}, End: Position{Line: 0, Col: 85}},
	}
	_ = expected

	lexer := NewLexer(" SELect     distinct *, @hello, [yes], 3.555, 'literal' FROM testtable where aND 'no'")

	lexed := []Token{}
	current := lexer.NextToken()
	t.Logf("current: %s\n", current.Value)

	for current.Type != TEndOfFile {
		lexed = append(lexed, current)

		current = lexer.NextToken()
	}

	if len(lexed) != len(expected) {
		t.Fatalf("expected %d tokens, got %d", len(expected), len(lexed))
	}

	for i, token := range lexed {
		if token.Type != expected[i].Type {
			t.Fatalf("expected %s, got %s", expected[i].Type.String(), token.Type.String())
		}
		lowercase := strings.ToLower(token.Value)
		if lowercase != expected[i].Value {
			t.Fatalf("expected %s, got %s", expected[i].Value, lowercase)
		}
		if token.String() != expected[i].String() {
			t.Fatalf("expected %s, got %s", expected[i].String(), token.String())
		}
	}

}
