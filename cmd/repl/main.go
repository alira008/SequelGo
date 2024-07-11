package main

import (
	"SequelGo/internal/lexer"
	"SequelGo/internal/parser"
	"fmt"
)

func main() {
	input := "select 'hello', yes, 1 from hello"
	l := lexer.NewLexer(input)
	p := parser.NewParser(l)

	query := p.Parse()
    fmt.Println(query.TokenLiteral())
}
