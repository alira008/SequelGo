package main

import (
	"SequelGo/internal/lexer"
	"SequelGo/internal/parser"
	"fmt"
)

func main() {
	input := "select 'hello'"
	l := lexer.NewLexer(input)
	p := parser.NewParser(l)
	// reader := bufio.NewReader(os.Stdin)
	//    reader.ReadString('\n')

	q := p.Parse()
	for _, s := range q.Statements {
		fmt.Printf("%v", s.TokenLiteral())
	}
}
