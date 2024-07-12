package main

import (
	"SequelGo/internal/lexer"
	"SequelGo/internal/parser"
	"bufio"
	"fmt"
	"os"
	"strings"

	"go.uber.org/zap"
)

func readLines() string {
	reader := bufio.NewReader(os.Stdin)
	fullInput := ""
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}
		fullInput += line
	}

	return fullInput
}

func main() {
	// input := "select 'hello', yes, 1 from hello"
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()
	sugar := logger.Sugar()

    str := readLines()
	// fmt.Println(str)
	l := lexer.NewLexer(str)
	p := parser.NewParser(sugar, l)

	query := p.Parse()
	if len(query.Statements) > 0 {
		fmt.Fprintln(os.Stdout, query.TokenLiteral())
	}
	if len(p.Errors()) > 0 {
		fmt.Fprintf(os.Stderr, strings.Join(p.Errors(), "\n"))
	}
}
