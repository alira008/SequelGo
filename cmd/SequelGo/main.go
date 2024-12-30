package main

import (
	"fmt"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "SequelGo [command]",
	Short: "Command line tool for working with T-SQL",
	Long: `SequelGo is a tool that has a command for an opionated formatter that formats T-SQL
    code into a more readable format. It also provides a command to start a language server
    that will make it easier to develop T-SQL queries`,
}

func init() {
	rootCmd.AddCommand(formatCmd)
	rootCmd.CompletionOptions.HiddenDefaultCmd = true
}

func main() {
	err := rootCmd.Execute()
	if err != nil {
		fmt.Printf("could not parse cmd ")
	}
}
