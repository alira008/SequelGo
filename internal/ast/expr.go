package ast

import (
	"SequelGo/internal/lexer"
	"fmt"
	"strings"
)

func expressionListToString[N Expression](list []N, separator string) string {
	var str strings.Builder
	var items []string
	for _, s := range list {
		items = append(items, s.TokenLiteral())
	}
	str.WriteString(strings.Join(items, separator))
	return str.String()
}

var BuiltinFunctionsTokenType = []lexer.TokenType{
	lexer.TDenseRank, lexer.TRank, lexer.TRowNumber, lexer.TAbs, lexer.TAcos, lexer.TAsin,
	lexer.TAtan, lexer.TCeiling, lexer.TCos, lexer.TCot, lexer.TDegrees, lexer.TExp, lexer.TFloor,
	lexer.TLog, lexer.TLog10, lexer.TPi, lexer.TPower, lexer.TRadians, lexer.TRands, lexer.TRound,
	lexer.TSign, lexer.TSin, lexer.TSqrt, lexer.TSquare, lexer.TTan, lexer.TFirstValue,
	lexer.TLastValue, lexer.TLag, lexer.TLead, lexer.TAvg, lexer.TCount, lexer.TMax, lexer.TMin,
	lexer.TStdev, lexer.TStdevp, lexer.TSum, lexer.TVar, lexer.TVarp, lexer.TGetdate,
	lexer.TChecksum, lexer.TNewId,
}

type ExprStringLiteral struct {
	Span
	LeadingComments  *[]Comment
	TrailingComments *[]Comment
	Value            string
}

type ExprNumberLiteral struct {
	Span
	LeadingComments  *[]Comment
	TrailingComments *[]Comment
	Value            string
}

type ExprLocalVariable struct {
	Span
	LeadingComments  *[]Comment
	TrailingComments *[]Comment
	Value            string
}

type ExprIdentifier struct {
	Span
	LeadingComments  *[]Comment
	TrailingComments *[]Comment
	Value            string
}

type ExprQuotedIdentifier struct {
	Span
	LeadingComments  *[]Comment
	TrailingComments *[]Comment
	Value            string
}

type ExprStar struct {
	LeadingComments  *[]Comment
	TrailingComments *[]Comment
	Span
}

type ExprWithAlias struct {
	Span
	LeadingComments  *[]Comment
	TrailingComments *[]Comment
	Expression       Expression
	AsKeyword        *Keyword
	Alias            Expression
}

type ExprCompoundIdentifier struct {
	Span
	LeadingComments  *[]Comment
	TrailingComments *[]Comment
	Identifiers      []Expression
}

type SelectItems struct {
	Span
	LeadingComments  *[]Comment
	TrailingComments *[]Comment
	Items            []Expression
}

type WhereClause struct {
	Span
	LeadingComments  *[]Comment
	TrailingComments *[]Comment
	WhereKeyword     Keyword
	Clause           Expression
}

type HavingClause struct {
	Span
	LeadingComments  *[]Comment
	TrailingComments *[]Comment
	HavingKeyword    Keyword
	Clause           Expression
}

type GroupByClause struct {
	Span
	LeadingComments  *[]Comment
	TrailingComments *[]Comment
	GroupByKeyword   [2]Keyword
	Items            []Expression
}

type ExprSubquery struct {
	SelectBody
	LeadingComments  *[]Comment
	TrailingComments *[]Comment
}

type TopArg struct {
	Span
	LeadingComments  *[]Comment
	TrailingComments *[]Comment
	TopKeyword       Keyword
	WithTiesKeyword  *[2]Keyword
	PercentKeyword   *Keyword
	Quantity         Expression
}

type TableArg struct {
	Span
	LeadingComments  *[]Comment
	TrailingComments *[]Comment
	FromKeyword      Keyword
	Table            *TableSource
	Joins            []Join
}

type TableSource struct {
	Span
	LeadingComments  *[]Comment
	TrailingComments *[]Comment
	Type             TableSourceType
	Source           Expression
}

type Join struct {
	Span
	LeadingComments  *[]Comment
	TrailingComments *[]Comment
	JoinTypeKeyword  []Keyword
	JoinKeyword      Keyword
	Type             JoinType
	Table            *TableSource
	OnKeyword        *Keyword
	Condition        Expression
}

type OrderByClause struct {
	Span
	LeadingComments  *[]Comment
	TrailingComments *[]Comment
	OrderByKeyword   [2]Keyword
	Expressions      []OrderByArg
	OffsetFetch      *OffsetFetchClause
}

type OffsetFetchClause struct {
	Span
	LeadingComments  *[]Comment
	TrailingComments *[]Comment
	Offset           OffsetArg
	Fetch            *FetchArg
}

type OrderByArg struct {
	Span
	LeadingComments  *[]Comment
	TrailingComments *[]Comment
	Column           Expression
	OrderKeyword     *Keyword
	Type             OrderByType
}

type OffsetArg struct {
	Span
	LeadingComments  *[]Comment
	TrailingComments *[]Comment
	OffsetKeyword    Keyword
	RowOrRowsKeyword Keyword
	Value            Expression
	RowOrRows        RowOrRows
}

type FetchArg struct {
	Span
	LeadingComments    *[]Comment
	TrailingComments   *[]Comment
	FetchKeyword       Keyword
	Value              Expression
	NextOrFirst        NextOrFirst
	RowOrRows          RowOrRows
	NextOrFirstKeyword Keyword
	RowOrRowsKeyword   Keyword
	OnlyKeyword        Keyword
}

type ExprExpressionList struct {
	Span
	LeadingComments  *[]Comment
	TrailingComments *[]Comment
	List             []Expression
}

type FunctionOverClause struct {
	Span
	LeadingComments    *[]Comment
	TrailingComments   *[]Comment
	OverKeyword        Keyword
	PartitionByKeyword *[2]Keyword
	PartitionByClause  []Expression
	OrderByKeyword     *[2]Keyword
	OrderByClause      []OrderByArg
	WindowFrameClause  *WindowFrameClause
}

type WindowFrameClause struct {
	Span
	LeadingComments    *[]Comment
	TrailingComments   *[]Comment
	RowsOrRangeKeyword Keyword
	RowsOrRange        RowsOrRangeType
	BetweenKeyword     *Keyword
	Start              *WindowFrameBound
	AndKeyword         *Keyword
	End                *WindowFrameBound
}

type WindowFrameBound struct {
	Span
	LeadingComments  *[]Comment
	TrailingComments *[]Comment
	BoundKeyword     []Keyword
	Type             WindowFrameBoundType
	Expression       Expression
}

type ExprFunction struct {
	Span
	LeadingComments  *[]Comment
	TrailingComments *[]Comment
	Type             FuncType
	Name             Expression
}

type ExprFunctionCall struct {
	Span
	LeadingComments  *[]Comment
	TrailingComments *[]Comment
	Name             *ExprFunction
	Args             []Expression
	OverClause       *FunctionOverClause
}

type ExprCast struct {
	Span
	LeadingComments  *[]Comment
	TrailingComments *[]Comment
	CastKeyword      Keyword
	Expression       Expression
	AsKeyword        Keyword
	DataType         DataType
}

type CommonTableExpression struct {
	Span
	LeadingComments  *[]Comment
	TrailingComments *[]Comment
	Name             string
	Columns          *ExprExpressionList
	AsKeyword        Keyword
	Query            SelectBody
}

func (e ExprStringLiteral) expressionNode()       {}
func (e ExprNumberLiteral) expressionNode()       {}
func (e ExprLocalVariable) expressionNode()       {}
func (e ExprIdentifier) expressionNode()          {}
func (e ExprQuotedIdentifier) expressionNode()    {}
func (e ExprStar) expressionNode()                {}
func (e ExprWithAlias) expressionNode()           {}
func (e ExprCompoundIdentifier) expressionNode()  {}
func (si SelectItems) expressionNode()            {}
func (w WhereClause) expressionNode()             {}
func (h HavingClause) expressionNode()            {}
func (gb GroupByClause) expressionNode()          {}
func (ta TableArg) expressionNode()               {}
func (ts TableSource) expressionNode()            {}
func (j Join) expressionNode()                    {}
func (ta TopArg) expressionNode()                 {}
func (o OrderByArg) expressionNode()              {}
func (o OrderByClause) expressionNode()           {}
func (o OffsetArg) expressionNode()               {}
func (f FetchArg) expressionNode()                {}
func (o OffsetFetchClause) expressionNode()       {}
func (e ExprSubquery) expressionNode()            {}
func (e ExprExpressionList) expressionNode()      {}
func (e ExprFunction) expressionNode()            {}
func (w WindowFrameBound) expressionNode()        {}
func (w WindowFrameClause) expressionNode()       {}
func (e FunctionOverClause) expressionNode()      {}
func (e ExprFunctionCall) expressionNode()        {}
func (e ExprCast) expressionNode()                {}
func (cte CommonTableExpression) expressionNode() {}

func (e ExprStringLiteral) TokenLiteral() string {
	return fmt.Sprintf("'%s'", e.Value)
}
func (e ExprNumberLiteral) TokenLiteral() string {
	return e.Value
}
func (e ExprLocalVariable) TokenLiteral() string {
	return fmt.Sprintf("@%s", e.Value)
}
func (e ExprIdentifier) TokenLiteral() string {
	return e.Value
}
func (e ExprQuotedIdentifier) TokenLiteral() string {
	return fmt.Sprintf("[%s]", e.Value)
}
func (e ExprStar) TokenLiteral() string {
	return "*"
}
func (e ExprWithAlias) TokenLiteral() string {
	var str strings.Builder
	str.WriteString(e.Expression.TokenLiteral())
	if e.AsKeyword != nil {
		str.WriteString(fmt.Sprintf(" %s ", e.AsKeyword.TokenLiteral()))
	} else {
		str.WriteString(" ")
	}
	str.WriteString(e.Alias.TokenLiteral())
	return str.String()
}
func (e ExprCompoundIdentifier) TokenLiteral() string {
	var str strings.Builder
	for i, item := range e.Identifiers {
		if i > 0 {
			str.WriteString(".")
		}
		str.WriteString(item.TokenLiteral())
	}
	return str.String()
}
func (si SelectItems) TokenLiteral() string {
	return expressionListToString(si.Items, ", ")
}
func (w WhereClause) TokenLiteral() string {
	var str strings.Builder
	str.WriteString(fmt.Sprintf(" %s ", w.WhereKeyword.TokenLiteral()))
	str.WriteString(w.Clause.TokenLiteral())

	return str.String()
}
func (h HavingClause) TokenLiteral() string {
	var str strings.Builder
	str.WriteString(fmt.Sprintf(" %s ", h.HavingKeyword.TokenLiteral()))
	str.WriteString(h.Clause.TokenLiteral())

	return str.String()
}
func (gb GroupByClause) TokenLiteral() string {
	var str strings.Builder
	str.WriteString(" ")
	for _, k := range gb.GroupByKeyword {
		str.WriteString(fmt.Sprintf("%s ", k.TokenLiteral()))
	}
	str.WriteString(expressionListToString(gb.Items, ", "))

	return str.String()
}
func (ta TableArg) TokenLiteral() string {
	var str strings.Builder

	str.WriteString(fmt.Sprintf(" %s ", ta.FromKeyword.TokenLiteral()))

	str.WriteString(ta.Table.TokenLiteral())

	if len(ta.Joins) == 0 {
		return str.String()
	}

	var joins []string
	for _, j := range ta.Joins {
		joins = append(joins, j.TokenLiteral())
	}

	str.WriteString(strings.Join(joins, " "))

	return str.String()
}
func (ts TableSource) TokenLiteral() string {
	var str strings.Builder

	str.WriteString(ts.Source.TokenLiteral())

	return str.String()
}
func (j Join) TokenLiteral() string {
	var str strings.Builder

	str.WriteString(" ")
	for _, k := range j.JoinTypeKeyword {
		str.WriteString(fmt.Sprintf("%s ", k.TokenLiteral()))
	}

	str.WriteString(j.Table.TokenLiteral())

	if j.Condition != nil {
		if j.OnKeyword != nil {
			str.WriteString(fmt.Sprintf(" %s ", j.OnKeyword.TokenLiteral()))
		}
		// str.WriteString(" ON ")
		str.WriteString(j.Condition.TokenLiteral())
	}

	return str.String()
}
func (ta TopArg) TokenLiteral() string {
	var str strings.Builder
	str.WriteString(fmt.Sprintf("%s %s", ta.TopKeyword.TokenLiteral(), ta.Quantity.TokenLiteral()))

	if ta.PercentKeyword != nil {
		str.WriteString(fmt.Sprintf(" %s", ta.PercentKeyword.TokenLiteral()))
	}

	if ta.WithTiesKeyword != nil {
		for _, k := range ta.WithTiesKeyword {
			str.WriteString(fmt.Sprintf(" %s", k.TokenLiteral()))
		}
	}

	return str.String()
}
func (o OrderByClause) TokenLiteral() string {
	var str strings.Builder

	if len(o.Expressions) == 0 {
		return ""
	}

	var orderByArgs []string
	for _, o := range o.Expressions {
		orderByArgs = append(orderByArgs, o.TokenLiteral())
	}

	str.WriteString(" ")
	for _, k := range o.OrderByKeyword {
		str.WriteString(fmt.Sprintf("%s ", k.TokenLiteral()))
	}
	str.WriteString(strings.Join(orderByArgs, ", "))

	if o.OffsetFetch == nil {
		return str.String()
	}

	str.WriteString(o.OffsetFetch.TokenLiteral())

	return str.String()
}
func (o OffsetFetchClause) TokenLiteral() string {
	var str strings.Builder

	str.WriteString(o.Offset.TokenLiteral())

	if o.Fetch == nil {
		return str.String()
	}

	str.WriteString(o.Fetch.TokenLiteral())
	return str.String()
}
func (o OrderByArg) TokenLiteral() string {
	var str strings.Builder
	str.WriteString(o.Column.TokenLiteral())
	if o.OrderKeyword != nil {
		str.WriteString(fmt.Sprintf(" %s", o.OrderKeyword.TokenLiteral()))
	}

	return str.String()
}
func (o OffsetArg) TokenLiteral() string {
	var str strings.Builder
	str.WriteString(fmt.Sprintf(" %s ", o.OffsetKeyword.TokenLiteral()))
	str.WriteString(o.Value.TokenLiteral())
	str.WriteString(fmt.Sprintf(" %s", o.RowOrRowsKeyword.TokenLiteral()))

	return str.String()
}
func (f FetchArg) TokenLiteral() string {
	var str strings.Builder
	str.WriteString(fmt.Sprintf(" %s ", f.FetchKeyword.TokenLiteral()))
	str.WriteString(fmt.Sprintf("%s ", f.NextOrFirstKeyword.TokenLiteral()))

	str.WriteString(f.Value.TokenLiteral())
	str.WriteString(fmt.Sprintf(" %s %s", f.RowOrRowsKeyword.TokenLiteral(),
		f.OnlyKeyword.TokenLiteral()))

	return str.String()
}
func (e ExprSubquery) TokenLiteral() string {
	var str strings.Builder
	str.WriteString("(")

	str.WriteString(e.SelectBody.TokenLiteral())

	str.WriteString(")")
	return str.String()
}
func (e ExprExpressionList) TokenLiteral() string {
	var str strings.Builder
	for i, item := range e.List {
		if i > 0 {
			str.WriteString(", ")
		}

		str.WriteString(item.TokenLiteral())
	}
	return str.String()
}
func (e ExprFunction) TokenLiteral() string {
	switch e.Type {
	case FuncDenseRank:
		return "DENSE_RANK"
	case FuncRank:
		return "RANK"
	case FuncRowNumber:
		return "ROW_NUMBER"
	case FuncAbs:
		return "ABS"
	case FuncAcos:
		return "ACOS"
	case FuncAsin:
		return "ASIN"
	case FuncAtan:
		return "ATAN"
	case FuncCeiling:
		return "CEILING"
	case FuncCos:
		return "COS"
	case FuncCot:
		return "COT"
	case FuncDegrees:
		return "DEGREES"
	case FuncExp:
		return "EXP"
	case FuncFloor:
		return "FLOOR"
	case FuncLog:
		return "LOG"
	case FuncLog10:
		return "LOG10"
	case FuncPi:
		return "PI"
	case FuncPower:
		return "POWER"
	case FuncRadians:
		return "RADIANS"
	case FuncRands:
		return "RANDS"
	case FuncRound:
		return "ROUND"
	case FuncSign:
		return "SIGN"
	case FuncSin:
		return "SIN"
	case FuncSqrt:
		return "SQRT"
	case FuncSquare:
		return "SQUARE"
	case FuncTan:
		return "TAN"
	case FuncFirstValue:
		return "FIRST_VALUE"
	case FuncLastValue:
		return "LAST_VALUE"
	case FuncLag:
		return "LAG"
	case FuncLead:
		return "LEAD"
	case FuncAvg:
		return "AVG"
	case FuncCount:
		return "COUNT"
	case FuncMax:
		return "MAX"
	case FuncMin:
		return "MIN"
	case FuncStdev:
		return "STDEV"
	case FuncStdevp:
		return "STDEVP"
	case FuncSum:
		return "SUM"
	case FuncVar:
		return "VAR"
	case FuncVarp:
		return "VARP"
	case FuncGetdate:
		return "GETDATE"
	case FuncChecksum:
		return "CHECKSUM"
	case FuncNewId:
		return "NEWID"
	case FuncUserDefined:
		return e.Name.TokenLiteral()
	default:
		return "unimplemented function type"
	}
}
func (w WindowFrameBound) TokenLiteral() string {
	var str strings.Builder

	if w.Expression != nil {
		str.WriteString(w.Expression.TokenLiteral())
		str.WriteString(" ")
	}
	for i, k := range w.BoundKeyword {
		if i > 0 {
			str.WriteString(" ")
		}
		str.WriteString(fmt.Sprintf("%s", k.TokenLiteral()))
	}
	// switch w.Type {
	// case WFBTFollowing:
	// 	str.WriteString(w.Expression.TokenLiteral())
	// 	str.WriteString(" FOLLOWING")
	// 	break
	// case WFBTCurrentRow:
	// 	str.WriteString("CURRENT ROW")
	// 	break
	// case WFBTPreceding:
	// 	str.WriteString(w.Expression.TokenLiteral())
	// 	str.WriteString(" PRECEDING")
	// 	break
	// case WFBTUnboundedPreceding:
	// 	str.WriteString("UNBOUNDED PRECEDING")
	// 	break
	// case WFBTUnboundedFollowing:
	// 	str.WriteString("UNBOUNDED FOLLOWING")
	// 	break
	// }

	return str.String()
}
func (w WindowFrameClause) TokenLiteral() string {
	var str strings.Builder

	str.WriteString(fmt.Sprintf(" %s ", w.RowsOrRangeKeyword.TokenLiteral()))
	// switch w.RowsOrRange {
	// case RRTRows:
	// 	str.WriteString(" ROWS ")
	// 	break
	// case RRTRange:
	// 	str.WriteString(" RANGE ")
	// 	break
	// }

	if w.End != nil {
		str.WriteString(fmt.Sprintf("%s ", w.BetweenKeyword.TokenLiteral()))
	}

	str.WriteString(w.Start.TokenLiteral())

	if w.End != nil {
		str.WriteString(fmt.Sprintf(" %s ", w.AndKeyword.TokenLiteral()))
		str.WriteString(w.End.TokenLiteral())
	}

	return str.String()
}
func (e FunctionOverClause) TokenLiteral() string {
	var str strings.Builder

	str.WriteString(fmt.Sprintf(" %s (", e.OverKeyword.TokenLiteral()))

	if len(e.PartitionByClause) > 0 {
		if e.PartitionByKeyword != nil {
			for _, k := range e.PartitionByKeyword {
				str.WriteString(fmt.Sprintf("%s ", k.TokenLiteral()))
			}
		}
		var expressions []string
		for _, p := range e.PartitionByClause {
			expressions = append(expressions, p.TokenLiteral())
		}
		str.WriteString(strings.Join(expressions, ", "))
	}

	if len(e.PartitionByClause) > 0 && len(e.OrderByClause) > 0 {
		str.WriteString(" ")
	}

	if len(e.OrderByClause) > 0 {
		if e.OrderByKeyword != nil {
			for _, k := range e.OrderByKeyword {
				str.WriteString(fmt.Sprintf("%s ", k.TokenLiteral()))
			}
		}
		var args []string
		for _, o := range e.OrderByClause {
			args = append(args, o.TokenLiteral())
		}
		str.WriteString(strings.Join(args, ", "))
	}

	if e.WindowFrameClause != nil {
		str.WriteString(e.WindowFrameClause.TokenLiteral())
	}

	str.WriteString(")")

	return str.String()
}
func (e ExprFunctionCall) TokenLiteral() string {
	var str strings.Builder
	str.WriteString(e.Name.TokenLiteral())
	str.WriteString("(")
	for i, item := range e.Args {
		if i > 0 {
			str.WriteString(", ")
		}
		str.WriteString(item.TokenLiteral())
	}
	str.WriteString(")")

	if e.OverClause != nil {
		str.WriteString(e.OverClause.TokenLiteral())
	}

	return str.String()
}
func (e ExprCast) TokenLiteral() string {
	var str strings.Builder

	str.WriteString(fmt.Sprintf("%s(", e.CastKeyword.TokenLiteral()))
	str.WriteString(e.Expression.TokenLiteral())
	str.WriteString(fmt.Sprintf(" %s ", e.AsKeyword.TokenLiteral()))
	str.WriteString(e.DataType.TokenLiteral())
	str.WriteString(")")

	return str.String()
}
func (cte *CommonTableExpression) TokenLiteral() string {
	var str strings.Builder
	str.WriteString(fmt.Sprintf("%s", cte.Name))
	if cte.Columns != nil {
		str.WriteString(" (")
		str.WriteString(cte.Columns.TokenLiteral())
		str.WriteString(")")
	}
	str.WriteString(fmt.Sprintf(" %s", cte.AsKeyword.TokenLiteral()))
	str.WriteString(" ( ")
	str.WriteString(cte.Query.TokenLiteral())
	str.WriteString(" )")
	return str.String()
}

func (e *ExprStringLiteral) SetSpan(span Span)       { e.Span = span }
func (e *ExprNumberLiteral) SetSpan(span Span)       { e.Span = span }
func (e *ExprLocalVariable) SetSpan(span Span)       { e.Span = span }
func (e *ExprIdentifier) SetSpan(span Span)          { e.Span = span }
func (e *ExprQuotedIdentifier) SetSpan(span Span)    { e.Span = span }
func (e *ExprStar) SetSpan(span Span)                { e.Span = span }
func (e *ExprWithAlias) SetSpan(span Span)           { e.Span = span }
func (e *ExprCompoundIdentifier) SetSpan(span Span)  { e.Span = span }
func (si *SelectItems) SetSpan(span Span)            { si.Span = span }
func (w *WhereClause) SetSpan(span Span)             { w.Span = span }
func (h *HavingClause) SetSpan(span Span)            { h.Span = span }
func (gb *GroupByClause) SetSpan(span Span)          { gb.Span = span }
func (ta *TableArg) SetSpan(span Span)               { ta.Span = span }
func (ts *TableSource) SetSpan(span Span)            { ts.Span = span }
func (j *Join) SetSpan(span Span)                    { j.Span = span }
func (ta *TopArg) SetSpan(span Span)                 { ta.Span = span }
func (o *OrderByClause) SetSpan(span Span)           { o.Span = span }
func (o *OffsetFetchClause) SetSpan(span Span)       { o.Span = span }
func (o *OrderByArg) SetSpan(span Span)              { o.Span = span }
func (o *OffsetArg) SetSpan(span Span)               { o.Span = span }
func (f *FetchArg) SetSpan(span Span)                { f.Span = span }
func (e *ExprSubquery) SetSpan(span Span)            { e.Span = span }
func (e *ExprExpressionList) SetSpan(span Span)      { e.Span = span }
func (e *ExprFunction) SetSpan(span Span)            { e.Span = span }
func (w *WindowFrameBound) SetSpan(span Span)        { w.Span = span }
func (w *WindowFrameClause) SetSpan(span Span)       { w.Span = span }
func (e *FunctionOverClause) SetSpan(span Span)      { e.Span = span }
func (e *ExprFunctionCall) SetSpan(span Span)        { e.Span = span }
func (e *ExprCast) SetSpan(span Span)                { e.Span = span }
func (cte *CommonTableExpression) SetSpan(span Span) { cte.Span = span }

func (e ExprStringLiteral) GetSpan() Span        { return e.Span }
func (e ExprNumberLiteral) GetSpan() Span        { return e.Span }
func (e ExprLocalVariable) GetSpan() Span        { return e.Span }
func (e ExprIdentifier) GetSpan() Span           { return e.Span }
func (e ExprQuotedIdentifier) GetSpan() Span     { return e.Span }
func (e ExprStar) GetSpan() Span                 { return e.Span }
func (e ExprWithAlias) GetSpan() Span            { return e.Span }
func (e ExprCompoundIdentifier) GetSpan() Span   { return e.Span }
func (si SelectItems) GetSpan() Span             { return si.Span }
func (w WhereClause) GetSpan() Span              { return w.Span }
func (h HavingClause) GetSpan() Span             { return h.Span }
func (gb GroupByClause) GetSpan() Span           { return gb.Span }
func (ta TableArg) GetSpan() Span                { return ta.Span }
func (ts TableSource) GetSpan() Span             { return ts.Span }
func (j Join) GetSpan() Span                     { return j.Span }
func (ta TopArg) GetSpan() Span                  { return ta.Span }
func (o OrderByClause) GetSpan() Span            { return o.Span }
func (o OffsetFetchClause) GetSpan() Span        { return o.Span }
func (o OrderByArg) GetSpan() Span               { return o.Span }
func (o OffsetArg) GetSpan() Span                { return o.Span }
func (f FetchArg) GetSpan() Span                 { return f.Span }
func (e ExprSubquery) GetSpan() Span             { return e.Span }
func (e ExprExpressionList) GetSpan() Span       { return e.Span }
func (e ExprFunction) GetSpan() Span             { return e.Span }
func (w WindowFrameBound) GetSpan() Span         { return w.Span }
func (w WindowFrameClause) GetSpan() Span        { return w.Span }
func (e FunctionOverClause) GetSpan() Span       { return e.Span }
func (e ExprFunctionCall) GetSpan() Span         { return e.Span }
func (e ExprCast) GetSpan() Span                 { return e.Span }
func (cte *CommonTableExpression) GetSpan() Span { return cte.Span }

func (e *ExprStringLiteral) SetLeadingComments(comments []Comment)    { e.LeadingComments = &comments }
func (e *ExprNumberLiteral) SetLeadingComments(comments []Comment)    { e.LeadingComments = &comments }
func (e *ExprLocalVariable) SetLeadingComments(comments []Comment)    { e.LeadingComments = &comments }
func (e *ExprIdentifier) SetLeadingComments(comments []Comment)       { e.LeadingComments = &comments }
func (e *ExprQuotedIdentifier) SetLeadingComments(comments []Comment) { e.LeadingComments = &comments }
func (e *ExprStar) SetLeadingComments(comments []Comment)             { e.LeadingComments = &comments }
func (e *ExprWithAlias) SetLeadingComments(comments []Comment)        { e.LeadingComments = &comments }
func (e *ExprCompoundIdentifier) SetLeadingComments(comments []Comment) {
	e.LeadingComments = &comments
}
func (si *SelectItems) SetLeadingComments(comments []Comment)       { si.LeadingComments = &comments }
func (w *WhereClause) SetLeadingComments(comments []Comment)        { w.LeadingComments = &comments }
func (h *HavingClause) SetLeadingComments(comments []Comment)       { h.LeadingComments = &comments }
func (gb *GroupByClause) SetLeadingComments(comments []Comment)     { gb.LeadingComments = &comments }
func (ta *TableArg) SetLeadingComments(comments []Comment)          { ta.LeadingComments = &comments }
func (ts *TableSource) SetLeadingComments(comments []Comment)       { ts.LeadingComments = &comments }
func (j *Join) SetLeadingComments(comments []Comment)               { j.LeadingComments = &comments }
func (ta *TopArg) SetLeadingComments(comments []Comment)            { ta.LeadingComments = &comments }
func (o *OrderByClause) SetLeadingComments(comments []Comment)      { o.LeadingComments = &comments }
func (o *OffsetFetchClause) SetLeadingComments(comments []Comment)  { o.LeadingComments = &comments }
func (o *OrderByArg) SetLeadingComments(comments []Comment)         { o.LeadingComments = &comments }
func (o *OffsetArg) SetLeadingComments(comments []Comment)          { o.LeadingComments = &comments }
func (f *FetchArg) SetLeadingComments(comments []Comment)           { f.LeadingComments = &comments }
func (e *ExprSubquery) SetLeadingComments(comments []Comment)       { e.LeadingComments = &comments }
func (e *ExprExpressionList) SetLeadingComments(comments []Comment) { e.LeadingComments = &comments }
func (e *ExprFunction) SetLeadingComments(comments []Comment)       { e.LeadingComments = &comments }
func (w *WindowFrameBound) SetLeadingComments(comments []Comment)   { w.LeadingComments = &comments }
func (w *WindowFrameClause) SetLeadingComments(comments []Comment)  { w.LeadingComments = &comments }
func (e *FunctionOverClause) SetLeadingComments(comments []Comment) { e.LeadingComments = &comments }
func (e *ExprFunctionCall) SetLeadingComments(comments []Comment)   { e.LeadingComments = &comments }
func (e *ExprCast) SetLeadingComments(comments []Comment)           { e.LeadingComments = &comments }
func (cte *CommonTableExpression) SetLeadingComments(comments []Comment) {
	cte.LeadingComments = &comments
}

func (e *ExprStringLiteral) SetTrailingComments(comments []Comment) { e.TrailingComments = &comments }
func (e *ExprNumberLiteral) SetTrailingComments(comments []Comment) { e.TrailingComments = &comments }
func (e *ExprLocalVariable) SetTrailingComments(comments []Comment) { e.TrailingComments = &comments }
func (e *ExprIdentifier) SetTrailingComments(comments []Comment)    { e.TrailingComments = &comments }
func (e *ExprQuotedIdentifier) SetTrailingComments(comments []Comment) {
	e.TrailingComments = &comments
}
func (e *ExprStar) SetTrailingComments(comments []Comment)      { e.TrailingComments = &comments }
func (e *ExprWithAlias) SetTrailingComments(comments []Comment) { e.TrailingComments = &comments }
func (e *ExprCompoundIdentifier) SetTrailingComments(comments []Comment) {
	e.TrailingComments = &comments
}
func (si *SelectItems) SetTrailingComments(comments []Comment)       { si.TrailingComments = &comments }
func (w *WhereClause) SetTrailingComments(comments []Comment)        { w.TrailingComments = &comments }
func (h *HavingClause) SetTrailingComments(comments []Comment)       { h.TrailingComments = &comments }
func (gb *GroupByClause) SetTrailingComments(comments []Comment)     { gb.TrailingComments = &comments }
func (ta *TableArg) SetTrailingComments(comments []Comment)          { ta.TrailingComments = &comments }
func (ts *TableSource) SetTrailingComments(comments []Comment)       { ts.TrailingComments = &comments }
func (j *Join) SetTrailingComments(comments []Comment)               { j.TrailingComments = &comments }
func (ta *TopArg) SetTrailingComments(comments []Comment)            { ta.TrailingComments = &comments }
func (o *OrderByClause) SetTrailingComments(comments []Comment)      { o.TrailingComments = &comments }
func (o *OffsetFetchClause) SetTrailingComments(comments []Comment)  { o.TrailingComments = &comments }
func (o *OrderByArg) SetTrailingComments(comments []Comment)         { o.TrailingComments = &comments }
func (o *OffsetArg) SetTrailingComments(comments []Comment)          { o.TrailingComments = &comments }
func (f *FetchArg) SetTrailingComments(comments []Comment)           { f.TrailingComments = &comments }
func (e *ExprSubquery) SetTrailingComments(comments []Comment)       { e.TrailingComments = &comments }
func (e *ExprExpressionList) SetTrailingComments(comments []Comment) { e.TrailingComments = &comments }
func (e *ExprFunction) SetTrailingComments(comments []Comment)       { e.TrailingComments = &comments }
func (w *WindowFrameBound) SetTrailingComments(comments []Comment)   { w.TrailingComments = &comments }
func (w *WindowFrameClause) SetTrailingComments(comments []Comment)  { w.TrailingComments = &comments }
func (e *FunctionOverClause) SetTrailingComments(comments []Comment) { e.TrailingComments = &comments }
func (e *ExprFunctionCall) SetTrailingComments(comments []Comment)   { e.TrailingComments = &comments }
func (e *ExprCast) SetTrailingComments(comments []Comment)           { e.TrailingComments = &comments }
func (cte *CommonTableExpression) SetTrailingComments(comments []Comment) {
	cte.TrailingComments = &comments
}

func (e *ExprStringLiteral) GetLeadingComments() *[]Comment       { return e.LeadingComments }
func (e *ExprNumberLiteral) GetLeadingComments() *[]Comment       { return e.LeadingComments }
func (e *ExprLocalVariable) GetLeadingComments() *[]Comment       { return e.LeadingComments }
func (e *ExprIdentifier) GetLeadingComments() *[]Comment          { return e.LeadingComments }
func (e *ExprQuotedIdentifier) GetLeadingComments() *[]Comment    { return e.LeadingComments }
func (e *ExprStar) GetLeadingComments() *[]Comment                { return e.LeadingComments }
func (e *ExprWithAlias) GetLeadingComments() *[]Comment           { return e.LeadingComments }
func (e *ExprCompoundIdentifier) GetLeadingComments() *[]Comment  { return e.LeadingComments }
func (si *SelectItems) GetLeadingComments() *[]Comment            { return si.LeadingComments }
func (w *WhereClause) GetLeadingComments() *[]Comment             { return w.LeadingComments }
func (h *HavingClause) GetLeadingComments() *[]Comment            { return h.LeadingComments }
func (gb *GroupByClause) GetLeadingComments() *[]Comment          { return gb.LeadingComments }
func (ta *TableArg) GetLeadingComments() *[]Comment               { return ta.LeadingComments }
func (ts *TableSource) GetLeadingComments() *[]Comment            { return ts.LeadingComments }
func (j *Join) GetLeadingComments() *[]Comment                    { return j.LeadingComments }
func (ta *TopArg) GetLeadingComments() *[]Comment                 { return ta.LeadingComments }
func (o *OrderByClause) GetLeadingComments() *[]Comment           { return o.LeadingComments }
func (o *OffsetFetchClause) GetLeadingComments() *[]Comment       { return o.LeadingComments }
func (o *OrderByArg) GetLeadingComments() *[]Comment              { return o.LeadingComments }
func (o *OffsetArg) GetLeadingComments() *[]Comment               { return o.LeadingComments }
func (f *FetchArg) GetLeadingComments() *[]Comment                { return f.LeadingComments }
func (e *ExprSubquery) GetLeadingComments() *[]Comment            { return e.LeadingComments }
func (e *ExprExpressionList) GetLeadingComments() *[]Comment      { return e.LeadingComments }
func (e *ExprFunction) GetLeadingComments() *[]Comment            { return e.LeadingComments }
func (w *WindowFrameBound) GetLeadingComments() *[]Comment        { return w.LeadingComments }
func (w *WindowFrameClause) GetLeadingComments() *[]Comment       { return w.LeadingComments }
func (e *FunctionOverClause) GetLeadingComments() *[]Comment      { return e.LeadingComments }
func (e *ExprFunctionCall) GetLeadingComments() *[]Comment        { return e.LeadingComments }
func (e *ExprCast) GetLeadingComments() *[]Comment                { return e.LeadingComments }
func (cte *CommonTableExpression) GetLeadingComments() *[]Comment { return cte.LeadingComments }

func (e *ExprStringLiteral) GetTrailingComments() *[]Comment       { return e.TrailingComments }
func (e *ExprNumberLiteral) GetTrailingComments() *[]Comment       { return e.TrailingComments }
func (e *ExprLocalVariable) GetTrailingComments() *[]Comment       { return e.TrailingComments }
func (e *ExprIdentifier) GetTrailingComments() *[]Comment          { return e.TrailingComments }
func (e *ExprQuotedIdentifier) GetTrailingComments() *[]Comment    { return e.TrailingComments }
func (e *ExprStar) GetTrailingComments() *[]Comment                { return e.TrailingComments }
func (e *ExprWithAlias) GetTrailingComments() *[]Comment           { return e.TrailingComments }
func (e *ExprCompoundIdentifier) GetTrailingComments() *[]Comment  { return e.TrailingComments }
func (si *SelectItems) GetTrailingComments() *[]Comment            { return si.TrailingComments }
func (w *WhereClause) GetTrailingComments() *[]Comment             { return w.TrailingComments }
func (h *HavingClause) GetTrailingComments() *[]Comment            { return h.TrailingComments }
func (gb *GroupByClause) GetTrailingComments() *[]Comment          { return gb.TrailingComments }
func (ta *TableArg) GetTrailingComments() *[]Comment               { return ta.TrailingComments }
func (ts *TableSource) GetTrailingComments() *[]Comment            { return ts.TrailingComments }
func (j *Join) GetTrailingComments() *[]Comment                    { return j.TrailingComments }
func (ta *TopArg) GetTrailingComments() *[]Comment                 { return ta.TrailingComments }
func (o *OrderByClause) GetTrailingComments() *[]Comment           { return o.TrailingComments }
func (o *OffsetFetchClause) GetTrailingComments() *[]Comment       { return o.TrailingComments }
func (o *OrderByArg) GetTrailingComments() *[]Comment              { return o.TrailingComments }
func (o *OffsetArg) GetTrailingComments() *[]Comment               { return o.TrailingComments }
func (f *FetchArg) GetTrailingComments() *[]Comment                { return f.TrailingComments }
func (e *ExprSubquery) GetTrailingComments() *[]Comment            { return e.TrailingComments }
func (e *ExprExpressionList) GetTrailingComments() *[]Comment      { return e.TrailingComments }
func (e *ExprFunction) GetTrailingComments() *[]Comment            { return e.TrailingComments }
func (w *WindowFrameBound) GetTrailingComments() *[]Comment        { return w.TrailingComments }
func (w *WindowFrameClause) GetTrailingComments() *[]Comment       { return w.TrailingComments }
func (e *FunctionOverClause) GetTrailingComments() *[]Comment      { return e.TrailingComments }
func (e *ExprFunctionCall) GetTrailingComments() *[]Comment        { return e.TrailingComments }
func (e *ExprCast) GetTrailingComments() *[]Comment                { return e.TrailingComments }
func (cte *CommonTableExpression) GetTrailingComments() *[]Comment { return cte.TrailingComments }

type TableSourceType uint8

const (
	TSTTable TableSourceType = iota
	TSTDerived
	TSTTableValuedFunction
)

type JoinType uint8

const (
	JTInner JoinType = iota
	JTLeft
	JTLeftOuter
	JTRight
	JTRightOuter
	JTFull
	JTFullOuter
)

type OrderByType uint8

const (
	OBNone OrderByType = iota
	OBAsc
	OBDesc
)

type RowOrRows uint8

const (
	RRRow RowOrRows = iota
	RRRows
)

type NextOrFirst uint8

const (
	NFNext NextOrFirst = iota
	NFFirst
)

type FuncType uint8

const (
	FuncDenseRank FuncType = iota
	FuncRank
	FuncRowNumber
	FuncAbs
	FuncAcos
	FuncAsin
	FuncAtan
	FuncCeiling
	FuncCos
	FuncCot
	FuncDegrees
	FuncExp
	FuncFloor
	FuncLog
	FuncLog10
	FuncPi
	FuncPower
	FuncRadians
	FuncRands
	FuncRound
	FuncSign
	FuncSin
	FuncSqrt
	FuncSquare
	FuncTan
	FuncFirstValue
	FuncLastValue
	FuncLag
	FuncLead
	FuncAvg
	FuncCount
	FuncMax
	FuncMin
	FuncStdev
	FuncStdevp
	FuncSum
	FuncVar
	FuncVarp
	FuncGetdate
	FuncChecksum
	FuncNewId
	FuncUserDefined
)

type WindowFrameBoundType uint8

const (
	WFBTCurrentRow WindowFrameBoundType = iota
	WFBTPreceding
	WFBTFollowing
	WFBTUnboundedPreceding
	WFBTUnboundedFollowing
)

type RowsOrRangeType uint8

const (
	RRTRows RowsOrRangeType = iota
	RRTRange
)
