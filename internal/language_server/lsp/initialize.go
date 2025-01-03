package lsp

type ClientInfo struct {
	Name    string  `json:"name"`
	Version *string `json:"version"`
}

type InitializeParams struct {
	ClientInfo *ClientInfo `json:"clientInfo"`
	RootUri    *string
}

type CompletionOptions struct {
	TriggerCharacters []string `json:"triggerCharacters"`
}

type TextDocumentSyncKind int

const (
	TextDocumentSyncKindNone TextDocumentSyncKind = iota
	TextDocumentSyncKindFull
	TextDocumentSyncKindIncremental
)

type TextDocumentSyncOptions struct {
	OpenClose bool                 `json:"openClose"`
	Change    TextDocumentSyncKind `json:"change"`
}

type ServerCapabilities struct {
	HoverProvider      bool               `json:"hoverProvider"`
	CompletionProvider *CompletionOptions `json:"completionProvider"`
	TextDocumentSync   TextDocumentSyncOptions `json:"textDocumentSync"`
}

type ServerInfo struct {
	Name    string  `json:"name"`
	Version *string `json:"version"`
}

type InitializeResult struct {
	Capabilities ServerCapabilities `json:"capabilities"`
	ServerInfo   *ServerInfo        `json:"serverInfo"`
}
