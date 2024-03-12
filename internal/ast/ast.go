package ast

type Node interface {
	TokenLiteral() string
}

type Statement interface {
	Node
	statementNode()
}

type Expression interface {
	Node
	expressionNode()
}

type Query struct {
	Statements []Statement
}

func (q *Query) TokenLiteral() string {
	if len(q.Statements) > 0 {
		return q.Statements[0].TokenLiteral()
	} else {
		return ""
	}
}

type SelectStatement struct {
	Distinct bool
    Top *TopStatement
}

type TopStatement struct {
    WithTies bool
    Percent bool
    Quantity int
}

func (ss *SelectStatement) statementNode()       {}
func (ss *SelectStatement) TokenLiteral() string { return "SELECT" }
