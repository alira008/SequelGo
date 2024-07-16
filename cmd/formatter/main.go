package main

import (
	"fmt"
)

func main() {
	err := rootCmd.Execute()
	if err != nil {
		fmt.Printf("could not parse cmd ")
	}
}
