package lsp

type DidOpenTextDocumentParams struct {
	TextDocument TextDocumentItem
}

type TextDocumentItem struct {
	Uri        string `json:"uri"`
	LanguageId string `json:"languageId"`
	Version    string `json:"version"`
	Text       string `json:"text"`
}

type TextDocumentContentChangeEvent struct {
	Text string `json:"text"`
}

type TextDocumentIdentifier struct {
	Uri string `json:"uri"`
}

type VersionedTextDocumentIdentifier struct {
	TextDocumentIdentifier
	Version int `json:"version"`
}

type DidChangeTextDocumentParams struct {
	TextDocument   VersionedTextDocumentIdentifier  `json:"textDocument"`
	ContentChanges []TextDocumentContentChangeEvent `json:"contentChanges"`
}

type Position struct {
	Line   uint `json:"line"`
	Column uint `json:"character"`
}

type TextDocumentPositionParams struct {
	TextDocument TextDocumentIdentifier
	Position     Position
}

type HoverParams struct {
	TextDocumentPositionParams
}

type MarkUpKind int

const (
	MarkUpKindPlainText MarkUpKind = iota
	MarkUpKindMarkdown
)

type MarkupContent struct {
	Kind  MarkUpKind `json:"kind"`
	Value string     `json:"value"`
}

type Range struct {
	Start Position `json:"start"`
	End   Position `json:"end"`
}

type HoverResult struct {
	Contents MarkupContent `json:"contents"`
	Range    *Range        `json:"range"`
}
