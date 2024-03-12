package main

import (
	"SequelGo/internal/lexer"
	"bufio"
	"fmt"
	"os"
)

func main() {
	input := "select * from 'helloworld' where"
	l := lexer.New(input)
	reader := bufio.NewReader(os.Stdin)
    reader.ReadString('\n')

	for {
		token := l.NextToken()
		if token.Type == lexer.TEndOfFile {
			break
		}

		if token.Type == lexer.TSyntaxError {
			fmt.Printf("Syntax Error: %s\n", token.Value)
		} else {
			fmt.Printf("Token: %s\n", token.Value)
		}
	}
}
