package main

import (
	"SequelGo/internal/language_server"
	"SequelGo/internal/language_server/database"
	"fmt"
	"os"

	"github.com/sourcegraph/jsonrpc2"
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

	dbConn, err := database.NewConnection()
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		return
	}
	err = dbConn.Connect()
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		return
	}

	server := language_server.NewServer(logger, dbConn)

	h := jsonrpc2.HandlerWithError(server.Handle)
	logger.Debug("SequelGo: reading on stdin, writing on stdout")
	<-jsonrpc2.NewConn(
		cmd.Context(),
		jsonrpc2.NewBufferedStream(StdReadWrite{}, jsonrpc2.VSCodeObjectCodec{}),
		h,
	).DisconnectNotify()
	logger.Debug("SequelGo: connections closed")
}

type StdReadWrite struct{}

func (StdReadWrite) Read(p []byte) (int, error) {
	return os.Stdin.Read(p)
}

func (StdReadWrite) Write(p []byte) (int, error) {
	return os.Stdout.Write(p)
}

func (StdReadWrite) Close() error {
	if err := os.Stdin.Close(); err != nil {
		return err
	}
	return os.Stdout.Close()
}
