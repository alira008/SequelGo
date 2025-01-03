package language_server

import (
	"SequelGo/internal/ast"
	"SequelGo/internal/language_server/database"
	"SequelGo/internal/language_server/lsp"
	"SequelGo/internal/lexer"
	"SequelGo/internal/parser"
	"context"
	"encoding/json"
	"fmt"

	"github.com/sourcegraph/jsonrpc2"
	"go.uber.org/zap"
)

type Server struct {
	logger      *zap.SugaredLogger
	dbConn      *database.Connection
	astMap      map[string]ast.Query
	documentMap map[string]string
}

func NewServer(logger *zap.SugaredLogger, conn *database.Connection) *Server {
	return &Server{
		logger:      logger,
		dbConn:      conn,
		astMap:      make(map[string]ast.Query),
		documentMap: make(map[string]string),
	}
}

func (s *Server) openFile(uri, text string) {
	s.documentMap[uri] = text
	lexer := lexer.NewLexer(text)
	parser := parser.NewParser(s.logger, lexer)
	query := parser.Parse()
	s.astMap[uri] = query
}

func (s *Server) updateFile(uri, text string) {
	s.documentMap[uri] = text
	lexer := lexer.NewLexer(text)
	parser := parser.NewParser(s.logger, lexer)
	query := parser.Parse()
	s.astMap[uri] = query
}

func (s *Server) Handle(ctx context.Context, conn *jsonrpc2.Conn, req *jsonrpc2.Request) (result interface{}, err error) {
	return s.handle(ctx, conn, req)
}

func (s *Server) handle(ctx context.Context, conn *jsonrpc2.Conn, req *jsonrpc2.Request) (result interface{}, err error) {
	switch req.Method {
	case "initialize":
		return s.handleInitialize(ctx, conn, req)
	case "initialized":
		return
	case "textDocument/didOpen":
		return s.handleTextDocumentDidOpen(ctx, conn, req)
	case "textDocument/didChange":
		return s.handleTextDocumentDidChange(ctx, conn, req)
	case "textDocument/hover":
		return s.handleTextDocumentHover(ctx, conn, req)
	}
	return nil, &jsonrpc2.Error{Code: jsonrpc2.CodeMethodNotFound, Message: fmt.Sprintf("method not supported %s", req.Method)}
}

func (s *Server) handleInitialize(_ context.Context, _ *jsonrpc2.Conn, req *jsonrpc2.Request) (result interface{}, err error) {
	if req.Params == nil {
		return nil, &jsonrpc2.Error{Code: jsonrpc2.CodeInvalidParams}
	}

	var params lsp.InitializeParams
	if err := json.Unmarshal(*req.Params, &params); err != nil {
		return nil, err
	}

	result = lsp.InitializeResult{
		Capabilities: lsp.ServerCapabilities{
			TextDocumentSync: lsp.TextDocumentSyncOptions{
				OpenClose: true,
				Change:    lsp.TextDocumentSyncKindFull,
			},
			HoverProvider: true,
			CompletionProvider: &lsp.CompletionOptions{
				TriggerCharacters: []string{"select", ".", ",", "from"},
			},
		},
	}

	return result, nil
}

func (s *Server) handleTextDocumentDidOpen(_ context.Context, _ *jsonrpc2.Conn, req *jsonrpc2.Request) (result interface{}, err error) {
	if req.Params == nil {
		return nil, &jsonrpc2.Error{Code: jsonrpc2.CodeInvalidParams}
	}

	var params lsp.DidOpenTextDocumentParams
	if err := json.Unmarshal(*req.Params, &params); err != nil {
		return nil, err
	}

	s.openFile(params.TextDocument.Uri, params.TextDocument.Text)

	return nil, nil
}

func (s *Server) handleTextDocumentDidChange(_ context.Context, _ *jsonrpc2.Conn, req *jsonrpc2.Request) (result interface{}, err error) {
	if req.Params == nil {
		return nil, &jsonrpc2.Error{Code: jsonrpc2.CodeInvalidParams}
	}

	var params lsp.DidChangeTextDocumentParams
	if err := json.Unmarshal(*req.Params, &params); err != nil {
		return nil, err
	}

	s.updateFile(params.TextDocument.Uri, params.ContentChanges[0].Text)

	return nil, nil
}
