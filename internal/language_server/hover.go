package language_server

import (
	"SequelGo/internal/ast"
	"SequelGo/internal/language_server/database"
	"SequelGo/internal/language_server/lsp"
	"context"
	"encoding/json"
	"fmt"
	"math"

	"github.com/sourcegraph/jsonrpc2"
)

func (s *Server) handleTextDocumentHover(_ context.Context, _ *jsonrpc2.Conn, req *jsonrpc2.Request) (result interface{}, err error) {
	if req.Params == nil {
		return nil, &jsonrpc2.Error{Code: jsonrpc2.CodeInvalidParams}
	}
	logger := s.logger

	var params lsp.HoverParams
	if err := json.Unmarshal(*req.Params, &params); err != nil {
		return nil, err
	}

	query, ok := s.astMap[params.TextDocument.Uri]
	if !ok {
		logger.Errorf("ast not found: %s", params.TextDocument.Uri)
		return nil, fmt.Errorf("ast not found: %s", params.TextDocument.Uri)
	}

	logger.Infof("bsearch")
	smallestNodeWidth := uint(math.MaxUint)
	var hoveredNode ast.Node
	ast.Inspect(&query, func(n ast.Node) bool {
		if n == nil {
			return false
		}
		nodeSpan := n.GetSpan()
		if params.Position.Line != nodeSpan.StartPosition.Line ||
			params.Position.Line != nodeSpan.EndPosition.Line ||
			params.Position.Column < nodeSpan.StartPosition.Col ||
			params.Position.Column > nodeSpan.EndPosition.Col {
			return true
		}

		nodeWidth := nodeSpan.EndPosition.Col - nodeSpan.StartPosition.Col
		if nodeWidth <= smallestNodeWidth {
			smallestNodeWidth = nodeWidth
			hoveredNode = n
		}
		return true
	})
	logger.Infof("params %v", params)
	if hoveredNode == nil {
		return
	}

	content := fmt.Sprintf("node: %s", hoveredNode.TokenLiteral())

	return lsp.HoverResult{
		Contents: lsp.MarkupContent{
			Kind:  lsp.MarkUpKindPlainText,
			Value: content,
		},
	}, nil
}

func hover(text string, params lsp.HoverParams, dbConn *database.Connection) {

}
