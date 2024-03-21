package main

import (
	"SequelGo/internal/lexer"
	"SequelGo/internal/parser"
)

func main() {
	input := "select 'hello', yes, 1"
	l := lexer.NewLexer(input)
	p := parser.NewParser(l)
	// reader := bufio.NewReader(os.Stdin)
	//    reader.ReadString('\n')

	q := p.Parse()
	q.TokenLiteral()
}
