package language_server

import (
	"SequelGo/internal/language_server/database"
	"SequelGo/internal/language_server/lsp"
	"context"
	"encoding/json"
	"fmt"

	"github.com/sourcegraph/jsonrpc2"
)

func (s *Server) handleTextDocumentHover(ctx context.Context, conn *jsonrpc2.Conn, req *jsonrpc2.Request) (result interface{}, err error) {
	if req.Params == nil {
		return nil, &jsonrpc2.Error{Code: jsonrpc2.CodeInvalidParams}
	}

	var params lsp.HoverParams
	if err := json.Unmarshal(*req.Params, &params); err != nil {
		return nil, err
	}

    ast, ok := s.astMap[params.TextDocument.Uri]
    if !ok {
        return nil, fmt.Errorf("document not found: %s", params.TextDocument.Uri)
    }



	return
}

func hover(text string, params lsp.HoverParams, dbConn *database.Connection){

}
