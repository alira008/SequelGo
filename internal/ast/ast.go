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

type DeclareStatement struct {
}

func (ds *DeclareStatement) statementNode() {}
func (ds *DeclareStatement) TokenLiteral() string {
	return ""
}

type ExecuteStatement struct {
}

type SetLocalVariableStatement struct {
}

type SelectStatement struct {
	SelectItems *[]*Expr
	whereClause *Expr
}

func (ss *SelectStatement) statementNode() {}
func (ss *SelectStatement) TokenLiteral() string {
	return ""
}

type InsertleteStatement struct {
}

type UpdateStatement struct {
}

type DeleteStatement struct {
}

type CTESelectStatement struct {
}

type CTEInsertleteStatement struct {
}

type CTEUpdateStatement struct {
}

type CTEDeleteStatement struct {
}
