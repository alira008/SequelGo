package main

import (
	"SequelGo/internal/language_server/database"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var runLanguageServerCmd = &cobra.Command{
	Use:   "lsp",
	Short: "language server for T-SQL",
	Long:  `SequelGo lsp is a language server that provides completion completion capabilities`,
	Run:   runLanguageServer,
}

func runLanguageServer(cmd *cobra.Command, args []string) {
	logger, _ := zap.NewProduction()
	defer logger.Sync()
	_ = logger.Sugar()

	_, err := database.GetConfig()
	if err != nil {
		fmt.Fprintln(os.Stderr, fmt.Sprintf("Unable to get database config: %s", err.Error()))
	}
}
