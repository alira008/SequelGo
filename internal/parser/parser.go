package parser

import (
	"SequelGo/internal/ast"
	"SequelGo/internal/lexer"
	"fmt"
	"strconv"
	"strings"

	"go.uber.org/zap"
)

type ErrorToken int

const (
	ETCurrent ErrorToken = iota
	ETPeek
	ETNone
)

type Parser struct {
	logger       *zap.SugaredLogger
	l            *lexer.Lexer
	currentToken lexer.Token
	peekToken    lexer.Token
	peekToken2   lexer.Token
	errorToken   ErrorToken
	errors       []string
}

func NewParser(logger *zap.SugaredLogger, lexer *lexer.Lexer) *Parser {
	parser := &Parser{logger: logger, l: lexer}

	parser.nextToken()
	parser.nextToken()
	parser.nextToken()

	return parser
}

func (p *Parser) nextToken() {
	p.currentToken = p.peekToken
	p.peekToken = p.peekToken2
	p.peekToken2 = p.l.NextToken()
	p.errorToken = ETNone
}

func (p *Parser) currentTokenIs(t lexer.TokenType) bool {
	return p.currentToken.Type == t
}

func (p *Parser) peekTokenIs(t lexer.TokenType) bool {
	return p.peekToken.Type == t
}

func (p *Parser) peekToken2Is(t lexer.TokenType) bool {
	return p.peekToken2.Type == t
}

func (p *Parser) peekTokenIsAny(t []lexer.TokenType) bool {
	for _, token := range t {
		if p.peekToken.Type == token {
			return true
		}
	}

	return false
}

func (p *Parser) peekToken2IsAny(t []lexer.TokenType) bool {
	for _, token := range t {
		if p.peekToken2.Type == token {
			return true
		}
	}

	return false
}

func (p *Parser) expectPeek(t lexer.TokenType) error {
	if p.peekToken.Type == t {
		p.nextToken()
		return nil
	}

	p.errorToken = ETPeek
	return p.peekError(t)
}

func (p *Parser) expectPeekMany(ts []lexer.TokenType) error {
	for _, t := range ts {
		if p.peekToken.Type == t {
			p.nextToken()
			return nil
		}
	}

	p.errorToken = ETPeek
	return p.peekErrorMany(ts)
}

func (p *Parser) peekError(t lexer.TokenType) error {
	p.errorToken = ETPeek
	return fmt.Errorf(
		"expected (%s) got (%s) instead\n%s",
		t.String(),
		p.peekToken.Type.String(),
		p.l.CurrentLine(),
	)
}

func (p *Parser) peekErrorMany(ts []lexer.TokenType) error {
	var expectedTokenTypes []string
	builtinFuncPresent := false
	for _, t := range ts {
		if t.IsBuiltinFunction() && !builtinFuncPresent {
			builtinFuncPresent = true
			expectedTokenTypes = append(expectedTokenTypes, fmt.Sprintf("Builtin Functions"))
			continue
		} else if t.IsBuiltinFunction() && builtinFuncPresent {
			continue
		}

		expectedTokenTypes = append(expectedTokenTypes, fmt.Sprintf("%s", t.String()))
	}

	p.errorToken = ETPeek
	return fmt.Errorf(
		"expected (%s) got (%s) instead\n%s",
		strings.Join(expectedTokenTypes, " or "),
		p.peekToken.Type.String(),
		p.l.CurrentLine(),
	)
}

func (p *Parser) currentErrorString(expected string) error {
	p.errorToken = ETCurrent
	return fmt.Errorf(
		"expected %s\n%s",
		expected,
		p.l.CurrentLine(),
	)
}

func (p *Parser) Errors() []string {
	return p.errors
}

func (p *Parser) Parse() ast.Query {
	query := ast.Query{Statements: []ast.Statement{}}

	for p.currentToken.Type != lexer.TEndOfFile {
		stmt, err := p.parseStatement()

		if err != nil {
			if p.errorToken == ETCurrent {
				p.logger.Debugf("[Error Line: %d Col: %d]: %s", p.currentToken.End.Line,
					p.currentToken.End.Col+1, err.Error())
			} else {
				p.logger.Debugf("[Error Line: %d Col: %d]: %s", p.peekToken.End.Line,
					p.peekToken.End.Col+1, err.Error())
			}
			p.nextToken()
			continue
		}

		query.Statements = append(query.Statements, stmt)

		p.nextToken()
	}
	return query
}

func (p *Parser) parseStatement() (ast.Statement, error) {
	switch p.currentToken.Type {
	case lexer.TSelect:
		body, err := p.parseSelectBody()
		if err != nil {
			return nil, err
		}

		return &ast.SelectStatement{SelectBody: &body}, nil
	default:
		return nil, nil
		// return nil, fmt.Errorf("unknown statement type %s", p.currentToken.Value)
	}
}

func (p *Parser) parseSelectStatement() *ast.SelectStatement {

	return &ast.SelectStatement{}
}

func (p *Parser) parseTopArg() (*ast.TopArg, error) {
	p.nextToken()
	p.nextToken()

	expr, err := p.parseExpression(PrecedenceLowest)
	if err != nil {
		return nil, err
	}

	topArg := ast.TopArg{Quantity: expr}
	if p.peekTokenIs(lexer.TPercent) {
		topArg.Percent = true
		p.nextToken()
	}

	if p.peekTokenIs(lexer.TWith) {
		p.nextToken()

		err = p.expectPeek(lexer.TTies)
		if err != nil {
			return nil, err
		}
		topArg.Percent = true
		p.nextToken()
	}

	return &topArg, nil
}

func (p *Parser) parseSelectBody() (ast.SelectBody, error) {
	stmt := ast.SelectBody{}
	if p.peekTokenIs(lexer.TDistinct) {
		stmt.Distinct = true
		p.nextToken()
	}

	// check for optional all keyword
	if p.peekTokenIs(lexer.TAll) {
		p.nextToken()
	}

	if p.peekTokenIs(lexer.TTop) {
		topArg, err := p.parseTopArg()
		if err != nil {
			return stmt, err
		}
		stmt.Top = topArg
	}

	selectItems, err := p.parseSelectItems()
	if err != nil {
		return stmt, err
	}
	stmt.SelectItems = selectItems

	table, err := p.parseTableArg()
	if err != nil {
		return stmt, err
	}
	stmt.Table = table

	whereExpression, err := p.parseWhereExpression()
	if err != nil {
		return stmt, err
	}
	stmt.WhereClause = whereExpression

	groupByClause, err := p.parseGroupByClause()
	if err != nil {
		return stmt, err
	}
	stmt.GroupByClause = groupByClause

	havingExpression, err := p.parseHavingExpression()
	if err != nil {
		return stmt, err
	}
	stmt.HavingClause = havingExpression

	orderByClause, err := p.parseOrderByClause()
	if err != nil {
		return stmt, err
	}
	stmt.OrderByClause = orderByClause

	return stmt, nil
}

func (p *Parser) parseSelectSubquery() (ast.ExprSubquery, error) {
	stmt := ast.ExprSubquery{}
	p.logger.Debug("parsing subquery")
	if p.peekTokenIs(lexer.TDistinct) {
		stmt.Distinct = true
		p.nextToken()
	}

	// check for optional all keyword
	if p.peekTokenIs(lexer.TAll) {
		p.nextToken()
	}

	if p.peekTokenIs(lexer.TTop) {
		topArg, err := p.parseTopArg()
		if err != nil {
			return stmt, err
		}
		stmt.Top = topArg
	}

	selectItems, err := p.parseSelectItems()
	if err != nil {
		return stmt, err
	}

	stmt.SelectItems = selectItems

	table, err := p.parseTableArg()
	if err != nil {
		return stmt, err
	}
	stmt.Table = table

	whereExpression, err := p.parseWhereExpression()
	if err != nil {
		return stmt, err
	}
	stmt.WhereClause = whereExpression

	groupByClause, err := p.parseGroupByClause()
	if err != nil {
		return stmt, err
	}
	stmt.GroupByClause = groupByClause

	havingExpression, err := p.parseHavingExpression()
	if err != nil {
		return stmt, err
	}
	stmt.HavingClause = havingExpression

	orderByClause, err := p.parseOrderByClause()
	if err != nil {
		return stmt, err
	}
	stmt.OrderByClause = orderByClause

	return stmt, nil
}

func (p *Parser) parseSelectItems() ([]ast.Expression, error) {
	items := []ast.Expression{}

	for {
		err := p.expectPeekMany(append([]lexer.TokenType{lexer.TIdentifier,
			lexer.TNumericLiteral,
			lexer.TStringLiteral,
			lexer.TAsterisk,
			lexer.TLocalVariable,
			lexer.TLeftParen,
			lexer.TMinus,
			lexer.TPlus,
			// rework checking keywords
			lexer.TSum,
			lexer.TQuotedIdentifier}, ast.BuiltinFunctionsTokenType...))
		if err != nil {
			return items, err
		}

		expr, err := p.parseExpression(PrecedenceLowest)
		if err != nil {
			return items, err
		}
		switch v := expr.(type) {
		case *ast.ExprSubquery:
			if len(v.SelectItems) > 1 {
				return items, p.currentErrorString("Subquery must contain only one column")
			}
			if len(v.GroupByClause) > 1 && v.Distinct {
				return items, p.currentErrorString("The 'DISTINCT' keyword can't be used with subqueries that include 'GROUP BY'")
			}
			if v.OrderByClause != nil && len(v.OrderByClause.Expressions) > 1 && v.Top == nil {
				return items, p.currentErrorString("'ORDER BY' can only be specified when 'TOP' is also specified")
			}
			break
		}
		if (p.peekToken.Type == lexer.TAs ||
			p.peekToken.Type == lexer.TIdentifier ||
			p.peekToken.Type == lexer.TStringLiteral ||
			p.peekToken.Type == lexer.TQuotedIdentifier) && !p.peekToken2IsAny(ast.DataTypeTokenTypes) {
			exprAlias := &ast.ExprWithAlias{AsTokenPresent: false, Expression: expr}

			if p.peekToken.Type == lexer.TAs {
				exprAlias.AsTokenPresent = true
				p.nextToken()
			}

			// needed in case we just parsed AS keyword
			err := p.expectPeekMany([]lexer.TokenType{lexer.TIdentifier, lexer.TStringLiteral, lexer.TQuotedIdentifier})
			if err != nil {
				return nil, err
			}

			alias, err := p.parseExpression(PrecedenceLowest)
			if err != nil {
				return nil, err
			}

			switch alias.(type) {
			case *ast.ExprIdentifier, *ast.ExprStringLiteral, *ast.ExprQuotedIdentifier:
				break
			default:
				err = fmt.Errorf("Expected (Identifier or StringLiteral or QuotedIdentifier) for Alias")
				return nil, err
			}
			exprAlias.Alias = alias
			items = append(items, exprAlias)
		} else {
			items = append(items, expr)
		}

		if p.peekToken.Type != lexer.TComma {
			break
		}

		p.nextToken()
	}

	return items, nil
}

func (p *Parser) parseTableArg() (*ast.TableArg, error) {
	err := p.expectPeek(lexer.TFrom)
	if err != nil {
		p.logger.Debug("from err")
		return nil, err
	}
	p.logger.Debug("parsing table arg")

	tableSource, err := p.parseTableSource()
	if err != nil {
		return nil, err
	}

	if !p.peekTokenIs(lexer.TInner) ||
		p.peekTokenIs(lexer.TLeft) ||
		p.peekTokenIs(lexer.TRight) ||
		p.peekTokenIs(lexer.TFull) {
		return &ast.TableArg{Table: tableSource}, nil
	}

	joins, err := p.parseJoins()
	if err != nil {
		return nil, err
	}

	return &ast.TableArg{Table: tableSource, Joins: joins}, nil
}

func (p *Parser) parseTableSource() (*ast.TableSource, error) {
	err := p.expectPeekMany([]lexer.TokenType{lexer.TIdentifier, lexer.TLocalVariable, lexer.TLeftParen})
	if err != nil {
		return nil, err
	}

	source, err := p.parseExpression(PrecedenceLowest)
	if err != nil {
		return nil, err
	}
	switch v := source.(type) {
	case *ast.ExprIdentifier, *ast.ExprCompoundIdentifier, *ast.ExprLocalVariable:
		return &ast.TableSource{
			Type:   ast.TSTTable,
			Source: source,
		}, err
	case *ast.ExprFunctionCall:
		return &ast.TableSource{
			Type:   ast.TSTTableValuedFunction,
			Source: source,
		}, err
	case *ast.ExprSubquery:
		return &ast.TableSource{
			Type:   ast.TSTDerived,
			Source: source,
		}, err
	case *ast.ExprWithAlias:
		var tableType ast.TableSourceType
		switch v.Expression.(type) {
		case *ast.ExprIdentifier, *ast.ExprCompoundIdentifier, *ast.ExprLocalVariable:
			tableType = ast.TSTTable
			break
		case *ast.ExprFunctionCall:
			tableType = ast.TSTTableValuedFunction
			break
		case *ast.ExprSubquery:
			tableType = ast.TSTDerived
			break
		}
		return &ast.TableSource{
			Type:   tableType,
			Source: v,
		}, err
	default:
		return nil, p.currentErrorString("expected Table Name or Function or Subquery")
	}

}

func (p *Parser) parseJoins() ([]ast.Join, error) {
	joins := []ast.Join{}

	for {
		var joinType ast.JoinType
		if p.peekTokenIs(lexer.TInner) {
			p.nextToken()
			if err := p.expectPeek(lexer.TJoin); err != nil {
				return nil, err
			}
			joinType = ast.JTInner

		} else if p.peekTokenIs(lexer.TLeft) {
			p.nextToken()
			if p.peekTokenIs(lexer.TOuter) {
				joinType = ast.JTLeftOuter
				p.nextToken()
			} else if p.peekTokenIs(lexer.TJoin) {
				joinType = ast.JTLeft
			}

			if err := p.expectPeek(lexer.TJoin); err != nil {
				return nil, err
			}
		} else if p.peekTokenIs(lexer.TRight) {
			p.nextToken()
			if p.peekTokenIs(lexer.TOuter) {
				joinType = ast.JTRightOuter
				p.nextToken()
			} else if p.peekTokenIs(lexer.TJoin) {
				joinType = ast.JTRight
			}

			if err := p.expectPeek(lexer.TJoin); err != nil {
				return nil, err
			}
		} else if p.peekTokenIs(lexer.TFull) {
			p.nextToken()
			if p.peekTokenIs(lexer.TOuter) {
				joinType = ast.JTFullOuter
				p.nextToken()
			} else if p.peekTokenIs(lexer.TJoin) {
				joinType = ast.JTFull
			}

			if err := p.expectPeek(lexer.TJoin); err != nil {
				return nil, err
			}
		} else {
			break
		}

		tableSource, err := p.parseTableSource()
		if err != nil {
			return nil, err
		}

		if err := p.expectPeek(lexer.TOn); err != nil {
			return nil, err
		}
		p.nextToken()

		searchCondition, err := p.parseExpression(PrecedenceLowest)
		if err != nil {
			return nil, err
		}

		joins = append(joins, ast.Join{Type: joinType, Table: tableSource, Condition: searchCondition})
	}

	return joins, nil
}

func (p *Parser) parseWhereExpression() (ast.Expression, error) {
	if !p.peekTokenIs(lexer.TWhere) {
		return nil, nil
	}
	p.logger.Debug("parsing where")

	// go to where token
	p.nextToken()
	p.nextToken()
	expr, err := p.parseExpression(PrecedenceLowest)
	if err != nil {
		return expr, err
	}

	switch expr.(type) {
	case *ast.ExprComparisonOperator,
		*ast.ExprAndLogicalOperator,
		*ast.ExprAllLogicalOperator,
		*ast.ExprBetweenLogicalOperator,
		*ast.ExprExistsLogicalOperator,
		*ast.ExprInSubqueryLogicalOperator,
		*ast.ExprInLogicalOperator,
		*ast.ExprLikeLogicalOperator,
		*ast.ExprNotLogicalOperator,
		*ast.ExprOrLogicalOperator,
		*ast.ExprSomeLogicalOperator,
		*ast.ExprAnyLogicalOperator:
		break
	default:
		p.errorToken = ETCurrent
		return nil, p.currentErrorString("expected expression after 'WHERE' keyword")
	}
	return expr, nil
}

func (p *Parser) parseGroupByClause() ([]ast.Expression, error) {
	items := []ast.Expression{}
	if !p.peekTokenIs(lexer.TGroup) {
		return items, nil
	}
	p.nextToken()
	err := p.expectPeek(lexer.TBy)
	if err != nil {
		return items, err
	}

	p.logger.Debug("parsing group by clause")

	for {
		err := p.expectPeekMany([]lexer.TokenType{
			lexer.TIdentifier,
			lexer.TNumericLiteral,
			lexer.TLocalVariable,
			lexer.TQuotedIdentifier,
		})
		if err != nil {
			return items, err
		}

		expr, err := p.parseExpression(PrecedenceLowest)
		if err != nil {
			return items, err
		}
		items = append(items, expr)

		if p.peekToken.Type != lexer.TComma {
			break
		}

		p.nextToken()
	}
	p.nextToken()

	return items, nil
}

func (p *Parser) parseHavingExpression() (ast.Expression, error) {
	if !p.peekTokenIs(lexer.THaving) {
		return nil, nil
	}
	p.logger.Debug("parsing having")

	// go to where token
	p.nextToken()
	p.nextToken()
	expr, err := p.parseExpression(PrecedenceLowest)
	if err != nil {
		return expr, err
	}

	switch expr.(type) {
	case *ast.ExprComparisonOperator,
		*ast.ExprAndLogicalOperator,
		*ast.ExprAllLogicalOperator,
		*ast.ExprBetweenLogicalOperator,
		*ast.ExprExistsLogicalOperator,
		*ast.ExprInSubqueryLogicalOperator,
		*ast.ExprInLogicalOperator,
		*ast.ExprLikeLogicalOperator,
		*ast.ExprNotLogicalOperator,
		*ast.ExprOrLogicalOperator,
		*ast.ExprSomeLogicalOperator,
		*ast.ExprAnyLogicalOperator:
		break
	default:
		return nil, p.currentErrorString("expected expression after 'HAVING' keyword")
	}
	return expr, nil
}

func (p *Parser) parseOrderByClause() (*ast.OrderByClause, error) {
	if !p.peekTokenIs(lexer.TOrder) {
		return nil, nil
	}
	p.nextToken()
	err := p.expectPeek(lexer.TBy)
	if err != nil {
		return nil, err
	}
	p.logger.Debug("parsing order by clause")
	args, err := p.parseOrderByArgs()
	if err != nil {
		return nil, err
	}
	orderByClause := &ast.OrderByClause{Expressions: args}

	if !p.peekTokenIs(lexer.TOffset) {
		return orderByClause, nil
	}

	offsetFetchClause, err := p.parseOffsetFetchClause()
	if err != nil {
		return nil, err
	}

	orderByClause.OffsetFetch = offsetFetchClause

	return orderByClause, nil
}

func (p *Parser) parseOrderByArgs() ([]ast.OrderByArg, error) {
	items := []ast.OrderByArg{}

	for {
		err := p.expectPeekMany([]lexer.TokenType{
			lexer.TIdentifier,
			lexer.TNumericLiteral,
			lexer.TLocalVariable,
			lexer.TQuotedIdentifier,
		})
		if err != nil {
			return items, err
		}

		expr, err := p.parseExpression(PrecedenceLowest)
		if err != nil {
			return items, err
		}
		if p.peekTokenIs(lexer.TAsc) {
			p.nextToken()
			items = append(items, ast.OrderByArg{Column: expr, Type: ast.OBAsc})
		} else if p.peekTokenIs(lexer.TDesc) {
			p.nextToken()
			items = append(items, ast.OrderByArg{Column: expr, Type: ast.OBDesc})
		} else {
			items = append(items, ast.OrderByArg{Column: expr, Type: ast.OBNone})
		}

		if p.peekToken.Type != lexer.TComma {
			break
		}

		p.nextToken()
	}

	return items, nil
}

func (p *Parser) parseOffsetFetchClause() (*ast.OffsetFetchClause, error) {
	p.logger.Debug("parsing offset fetch clause")
	p.nextToken()

	offset, err := p.parseOffset()
	if err != nil {
		return nil, err
	}
	offsetFetchClause := ast.OffsetFetchClause{Offset: offset}

	if !p.peekTokenIs(lexer.TFetch) {
		return &offsetFetchClause, nil
	}

	p.nextToken()
	fetch, err := p.parseFetch()
	if err != nil {
		return nil, err
	}

	offsetFetchClause.Fetch = &fetch

	return &offsetFetchClause, nil
}

func (p *Parser) parseOffset() (ast.OffsetArg, error) {
	p.nextToken()
	offsetArg := ast.OffsetArg{}
	offset, err := p.parseExpression(PrecedenceLowest)
	if err != nil {
		return offsetArg, err
	}

	err = p.expectPeekMany([]lexer.TokenType{lexer.TRow, lexer.TRows})
	if err != nil {
		return offsetArg, err
	}

	offsetArg.Value = offset
	switch p.currentToken.Type {
	case lexer.TRow:
		offsetArg.RowOrRows = ast.RRRow
		return offsetArg, nil
	case lexer.TRows:
		offsetArg.RowOrRows = ast.RRRows
		return offsetArg, nil
	default:
		return offsetArg, p.currentErrorString("Expected 'ROW' or 'ROWS' after offset expression")
	}
}

func (p *Parser) parseFetch() (ast.FetchArg, error) {
	fetchArg := ast.FetchArg{}
	if err := p.expectPeekMany([]lexer.TokenType{lexer.TFirst, lexer.TNext}); err != nil {
		return fetchArg, err
	}

	switch p.currentToken.Type {
	case lexer.TFirst:
		fetchArg.NextOrFirst = ast.NFFirst
		break
	case lexer.TNext:
		fetchArg.NextOrFirst = ast.NFNext
		break
	}

	p.nextToken()
	fetch, err := p.parseExpression(PrecedenceLowest)
	if err != nil {
		return fetchArg, err
	}

	fetchArg.Value = fetch
	err = p.expectPeekMany([]lexer.TokenType{lexer.TRow, lexer.TRows})
	if err != nil {
		return fetchArg, err
	}

	switch p.currentToken.Type {
	case lexer.TRow:
		fetchArg.RowOrRows = ast.RRRow
		break
	case lexer.TRows:
		fetchArg.RowOrRows = ast.RRRows
		break
	}

	if err = p.expectPeek(lexer.TOnly); err != nil {
		return fetchArg, err
	}

	return fetchArg, nil
}

func (p *Parser) parseOverClause() (*ast.FunctionOverClause, error) {
	p.nextToken()
	if err := p.expectPeek(lexer.TLeftParen); err != nil {
		return nil, err
	}

	functionOverClause := ast.FunctionOverClause{}

	if p.peekTokenIs(lexer.TPartition) {
		expressions, err := p.parsePartitionClause()
		if err != nil {
			return nil, err
		}
		functionOverClause.PartitionByClause = expressions
	}

	if p.peekTokenIs(lexer.TOrder) {
		p.nextToken()
		if err := p.expectPeek(lexer.TBy); err != nil {
			return nil, err
		}
		args, err := p.parseOrderByArgs()
		if err != nil {
			return nil, err
		}
		functionOverClause.OrderByClause = args
	}

	if p.peekTokenIs(lexer.TRows) || p.peekTokenIs(lexer.TRange) {
		clause, err := p.parseWindowFrameClause()
		if err != nil {
			return nil, err
		}
		functionOverClause.WindowFrameClause = clause
	}

	err := p.expectPeek(lexer.TRightParen)
	if err != nil {
		return nil, err
	}

	return &functionOverClause, nil
}

func (p *Parser) parsePartitionClause() ([]ast.Expression, error) {
	p.nextToken()
	if err := p.expectPeek(lexer.TBy); err != nil {
		return nil, err
	}
	args := []ast.Expression{}

	for {
		if !p.peekTokenIs(lexer.TIdentifier) && !p.peekTokenIs(lexer.TQuotedIdentifier) {
			break
		}
		p.nextToken()

		expr, err := p.parseExpression(PrecedenceLowest)
		if err != nil {
			return nil, err
		}

		args = append(args, expr)

		if !p.peekTokenIs(lexer.TComma) {
			break
		}
		p.nextToken()
	}

	if len(args) == 0 {
		return nil, p.currentErrorString("PARTITION BY items in PARTITION BY expression")
	}

	return args, nil
}

func (p *Parser) parseWindowFrameClause() (*ast.WindowFrameClause, error) {
	var rowsOrRangeType ast.RowsOrRangeType
	if p.peekTokenIs(lexer.TRows) {
		rowsOrRangeType = ast.RRTRows
	} else if p.peekTokenIs(lexer.TRange) {
		rowsOrRangeType = ast.RRTRange
	}
	p.nextToken()

	var windowFrameStart ast.WindowFrameBound
	var windowFrameEnd ast.WindowFrameBound
	followingNeeded := false
	// parse between
	p.logger.Debug("p.currentToken.Value: ", p.currentToken.Value)
	p.logger.Debug("p.peekToken.Value: ", p.peekToken.Value)
	if p.peekTokenIs(lexer.TBetween) {
		followingNeeded = true
		p.nextToken()
	}
	p.logger.Debug("p.currentToken.Value: ", p.currentToken.Value)
	p.logger.Debug("p.peekToken.Value: ", p.peekToken.Value)
	if p.peekTokenIs(lexer.TUnbounded) {
		p.nextToken()
		if err := p.expectPeek(lexer.TPreceding); err != nil {
			return nil, err
		}
		windowFrameStart = ast.WindowFrameBound{Type: ast.WFBTUnboundedPreceding}
	} else if p.peekTokenIs(lexer.TCurrent) {
		p.nextToken()
		if err := p.expectPeek(lexer.TRow); err != nil {
			return nil, err
		}
		windowFrameStart = ast.WindowFrameBound{Type: ast.WFBTCurrentRow}
	} else if p.peekTokenIs(lexer.TNumericLiteral) {
		p.nextToken()
		expr, err := p.parseExpression(PrecedenceLowest)
		p.logger.Debug("test")
		if err != nil {
			return nil, err
		}
		if err := p.expectPeek(lexer.TPreceding); err != nil {
			return nil, err
		}
		windowFrameStart = ast.WindowFrameBound{Type: ast.WFBTPreceding, Expression: expr}
	} else {
		return nil, p.currentErrorString("Expected UNBOUNDED PRECEDING or CURRENT ROW or <NUMBER> PRECEDING")
	}

	if !followingNeeded {
		return &ast.WindowFrameClause{RowsOrRange: rowsOrRangeType, Start: &windowFrameStart}, nil
	}

	if err := p.expectPeek(lexer.TAnd); err != nil {
		return nil, err
	}

	if p.peekTokenIs(lexer.TUnbounded) {
		p.nextToken()
		if err := p.expectPeek(lexer.TFollowing); err != nil {
			return nil, err
		}
		windowFrameEnd = ast.WindowFrameBound{Type: ast.WFBTUnboundedFollowing}
	} else if p.peekTokenIs(lexer.TCurrent) {
		p.nextToken()
		if err := p.expectPeek(lexer.TRow); err != nil {
			return nil, err
		}
		windowFrameEnd = ast.WindowFrameBound{Type: ast.WFBTCurrentRow}
	} else if p.peekTokenIs(lexer.TNumericLiteral) {
		p.nextToken()
		expr, err := p.parseExpression(PrecedenceLowest)
		if err != nil {
			return nil, err
		}
		if err := p.expectPeek(lexer.TFollowing); err != nil {
			return nil, err
		}
		windowFrameEnd = ast.WindowFrameBound{Type: ast.WFBTFollowing, Expression: expr}
	} else {
		return nil, p.currentErrorString("Expected UNBOUNDED FOLLOWING or CURRENT ROW or <NUMBER> FOLLOWING")
	}

	return &ast.WindowFrameClause{RowsOrRange: rowsOrRangeType, Start: &windowFrameStart, End: &windowFrameEnd}, nil
}

func (p *Parser) parseExpressionList() (ast.ExprExpressionList, error) {
	expressionList := ast.ExprExpressionList{}

	for {
		err := p.expectPeekMany([]lexer.TokenType{lexer.TIdentifier,
			lexer.TQuotedIdentifier,
			lexer.TNumericLiteral,
			lexer.TStringLiteral,
			lexer.TLocalVariable})
		if err != nil {
			return expressionList, err
		}

		expr, err := p.parseExpression(PrecedenceLowest)
		if err != nil {
			return expressionList, err
		}
		expressionList.List = append(expressionList.List, expr)

		if p.peekToken.Type != lexer.TComma {
			break
		}

		p.nextToken()
	}

	return expressionList, nil
}

func (p *Parser) parseNumericSize() (*ast.NumericSize, error) {
	if err := p.expectPeek(lexer.TLeftParen); err != nil {
		return nil, err
	}

	if err := p.expectPeek(lexer.TNumericLiteral); err != nil {
		return nil, p.currentErrorString("Expected a numeric literal for casting expression")
	}

	// parse precision
	precision, err := strconv.ParseUint(p.currentToken.Value, 10, 32)
	if err != nil {
		return nil, p.currentErrorString("could not convert numeric literal to uint32")
	}
	precision32 := uint32(precision)
	if p.peekTokenIs(lexer.TRightParen) {
        p.nextToken()
		return &ast.NumericSize{Precision: precision32}, nil
	}

	// parse scale
	if err := p.expectPeek(lexer.TComma); err != nil {
		return nil, p.currentErrorString("Expected a comma before scale")
	}

	p.nextToken()

	if err := p.expectPeek(lexer.TNumericLiteral); err != nil {
		return nil, p.currentErrorString("Expected a numeric literal for scale when casting expression")
	}

	scale, err := strconv.ParseUint(p.currentToken.Value, 10, 32)
	if err != nil {
		return nil, p.currentErrorString("could not convert numeric literal to uint32")
	}
	scale32 := uint32(scale)

	if err := p.expectPeek(lexer.TRightParen); err != nil {
		return nil, p.currentErrorString("Expected a right parenthesis after scale")
	}

	return &ast.NumericSize{Precision: precision32, Scale: &scale32}, nil
}

func (p *Parser) parseDataType() (*ast.DataType, error) {
	if err := p.expectPeekMany(ast.DataTypeTokenTypes); err != nil {
		return nil, err
	}

	var dataType ast.DataType
	switch p.currentToken.Type {
	case lexer.TInt:
		dataType = ast.DataType{Kind: ast.DTInt}
		break
	case lexer.TBigint:
		dataType = ast.DataType{Kind: ast.DTBigInt}
		break
	case lexer.TTinyint:
		dataType = ast.DataType{Kind: ast.DTTinyInt}
		break
	case lexer.TSmallint:
		dataType = ast.DataType{Kind: ast.DTSmallInt}
		break
	case lexer.TBit:
		dataType = ast.DataType{Kind: ast.DTBit}
		break
	case lexer.TFloat:
		dataType = ast.DataType{Kind: ast.DTFloat}
		if !p.peekTokenIs(lexer.TLeftParen) {
			break
		}

		if err := p.expectPeek(lexer.TLeftParen); err != nil {
			return nil, err
		}
		if err := p.expectPeek(lexer.TNumericLiteral); err != nil {
			return nil, p.currentErrorString("Expected a numeric literal for casting expression")
		}
		size, err := strconv.ParseUint(p.currentToken.Value, 10, 32)
		if err != nil {
			return nil, p.currentErrorString("could not convert numeric literal to uint32")
		}
		size32 := uint32(size)
		dataType.FloatPrecision = &size32
		if err := p.expectPeek(lexer.TRightParen); err != nil {
			return nil, p.currentErrorString("Expected a numeric literal for casting expression")
		}
		break
	case lexer.TReal:
		dataType = ast.DataType{Kind: ast.DTReal}
		break
	case lexer.TDate:
		dataType = ast.DataType{Kind: ast.DTDate}
		break
	case lexer.TDatetime:
		dataType = ast.DataType{Kind: ast.DTDatetime}
		break
	case lexer.TTime:
		dataType = ast.DataType{Kind: ast.DTTime}
		break
	case lexer.TDecimal:
		dataType = ast.DataType{Kind: ast.DTDecimal}
		if !p.peekTokenIs(lexer.TLeftParen) {
			break
		}
		numericSize, err := p.parseNumericSize()
		if err != nil {
			return nil, err
		}
		dataType.DecimalNumericSize = numericSize
		break
	case lexer.TNumeric:
		dataType = ast.DataType{Kind: ast.DTNumeric}
		if !p.peekTokenIs(lexer.TLeftParen) {
			break
		}
		numericSize, err := p.parseNumericSize()
		if err != nil {
			return nil, err
		}
		dataType.DecimalNumericSize = numericSize
		break
	case lexer.TVarchar:
		dataType = ast.DataType{Kind: ast.DTVarchar}
		if !p.peekTokenIs(lexer.TLeftParen) {
			break
		}

		if err := p.expectPeek(lexer.TLeftParen); err != nil {
			return nil, err
		}
		if err := p.expectPeek(lexer.TNumericLiteral); err != nil {
			return nil, p.currentErrorString("Expected a numeric literal for casting expression")
		}
		size, err := strconv.ParseUint(p.currentToken.Value, 10, 32)
		if err != nil {
			return nil, p.currentErrorString("could not convert numeric literal to uint32")
		}
		size32 := uint32(size)
		dataType.FloatPrecision = &size32
		if err := p.expectPeek(lexer.TRightParen); err != nil {
			return nil, p.currentErrorString("Expected a numeric literal for casting expression")
		}
		break
	default:
		return nil, p.currentErrorString("Expected a Builtin Datatype")
	}

	return &dataType, nil
}

func (p *Parser) parseCast() (*ast.ExprCast, error) {
	if err := p.expectPeek(lexer.TLeftParen); err != nil {
		return nil, err
	}
	p.logger.Debug("parsing cast expression")
	p.nextToken()

	expr, err := p.parseExpression(PrecedenceLowest)
	if err != nil {
		return nil, err
	}

	p.logger.Debug(expr.TokenLiteral())
	if err := p.expectPeek(lexer.TAs); err != nil {
		return nil, err
	}

	dt, err := p.parseDataType()
	if err != nil {
		return nil, err
	}
	p.logger.Debug(dt.TokenLiteral())
	p.logger.Debug(p.currentToken)
	p.logger.Debug(p.peekToken)
	if err := p.expectPeek(lexer.TRightParen); err != nil {
		return nil, err
	}

	return &ast.ExprCast{Expression: expr, DataType: *dt}, nil
}

func (p *Parser) parseExpression(precedence Precedence) (ast.Expression, error) {
	leftExpr, err := p.parsePrefixExpression()
	if err != nil {
		return nil, err
	}

	// parse infix sql expressions using stacks to keep track of precedence
	for precedence < checkPrecedence(p.peekToken.Type) {
		p.nextToken()

		leftExpr, err = p.parseInfixExpression(leftExpr)
		if err != nil {
			return nil, err
		}
	}

	return leftExpr, nil
}

func (p *Parser) parsePrefixExpression() (ast.Expression, error) {
	// p.logger.Debugf("parsing prefix expression\n")
	var newExpr ast.Expression
	switch p.currentToken.Type {
	case lexer.TIdentifier, lexer.TNumericLiteral, lexer.TStringLiteral, lexer.TAsterisk, lexer.TLocalVariable, lexer.TQuotedIdentifier:
		switch p.currentToken.Type {
		case lexer.TLocalVariable:
			newExpr = &ast.ExprLocalVariable{Value: p.currentToken.Value}
		case lexer.TQuotedIdentifier:
			newExpr = &ast.ExprQuotedIdentifier{Value: p.currentToken.Value}
		case lexer.TStringLiteral:
			newExpr = &ast.ExprStringLiteral{Value: p.currentToken.Value}
		case lexer.TNumericLiteral:
			newExpr = &ast.ExprNumberLiteral{Value: p.currentToken.Value}
		case lexer.TIdentifier:
			newExpr = &ast.ExprIdentifier{Value: p.currentToken.Value}
		case lexer.TAsterisk:
			newExpr = &ast.ExprStar{}
		}

		if p.peekToken.Type == lexer.TPeriod {
			// we are dealing with a qualified identifier
			compound := &[]ast.Expression{newExpr}
			p.logger.Debug("parsing compound identifier")

			// go to period token
			p.nextToken()
			p.logger.Debug("current token: ", p.currentToken)

			for {
				err := p.expectPeekMany([]lexer.TokenType{lexer.TIdentifier, lexer.TQuotedIdentifier, lexer.TAsterisk})
				if err != nil {
					return nil, err
				}
				p.logger.Debug("current token: ", p.currentToken)

				if p.currentToken.Type == lexer.TAsterisk {
					expr := &ast.ExprStar{}
					*compound = append(*compound, expr)
					break
				} else if p.currentToken.Type == lexer.TQuotedIdentifier {
					expr := &ast.ExprQuotedIdentifier{Value: p.currentToken.Value}
					*compound = append(*compound, expr)
				} else {
					expr := &ast.ExprIdentifier{Value: p.currentToken.Value}
					*compound = append(*compound, expr)
				}

				if p.peekToken.Type != lexer.TPeriod {
					break
				}

				p.nextToken()
			}

			newExpr = &ast.ExprCompoundIdentifier{Identifiers: *compound}
		}

		if (p.peekTokenIs(lexer.TAs) || p.peekTokenIs(lexer.TIdentifier) ||
			p.peekTokenIs(lexer.TStringLiteral) || p.peekTokenIs(lexer.TQuotedIdentifier)) &&
			!p.peekToken2IsAny(ast.DataTypeTokenTypes) {
			expr := &ast.ExprWithAlias{AsTokenPresent: false, Expression: newExpr}

			if p.peekToken.Type == lexer.TAs {
				expr.AsTokenPresent = true
				p.nextToken()
			}

			// needed in case we just parsed AS keyword
			err := p.expectPeekMany([]lexer.TokenType{lexer.TIdentifier, lexer.TStringLiteral, lexer.TQuotedIdentifier})
			if err != nil {
				return nil, err
			}

			alias, err := p.parseExpression(PrecedenceLowest)
			if err != nil {
				return nil, err
			}

			switch alias.(type) {
			case *ast.ExprIdentifier, *ast.ExprStringLiteral, *ast.ExprQuotedIdentifier:
				break
			default:
				err = fmt.Errorf("Expected (Identifier or StringLiteral or QuotedIdentifier) for Alias")
				return nil, err
			}
			expr.Alias = alias
			newExpr = expr
		}
	case lexer.TDenseRank,
		lexer.TRank,
		lexer.TRowNumber,
		lexer.TAbs,
		lexer.TAcos,
		lexer.TAsin,
		lexer.TAtan,
		lexer.TCeiling,
		lexer.TCos,
		lexer.TCot,
		lexer.TDegrees,
		lexer.TExp,
		lexer.TFloor,
		lexer.TLog,
		lexer.TLog10,
		lexer.TPi,
		lexer.TPower,
		lexer.TRadians,
		lexer.TRands,
		lexer.TRound,
		lexer.TSign,
		lexer.TSin,
		lexer.TSqrt,
		lexer.TSquare,
		lexer.TTan,
		lexer.TFirstValue,
		lexer.TLastValue,
		lexer.TLag,
		lexer.TLead,
		lexer.TAvg,
		lexer.TCount,
		lexer.TMax,
		lexer.TMin,
		lexer.TStdev,
		lexer.TStdevp,
		lexer.TSum,
		lexer.TVar,
		lexer.TVarp,
		lexer.TGetdate:
		var funcType ast.FuncType
		switch p.currentToken.Type {
		case lexer.TDenseRank:
			funcType = ast.FuncDenseRank
		case lexer.TRank:
			funcType = ast.FuncRank
		case lexer.TRowNumber:
			funcType = ast.FuncRowNumber
		case lexer.TAbs:
			funcType = ast.FuncAbs
		case lexer.TAcos:
			funcType = ast.FuncAcos
		case lexer.TAsin:
			funcType = ast.FuncAsin
		case lexer.TAtan:
			funcType = ast.FuncAtan
		case lexer.TCeiling:
			funcType = ast.FuncCeiling
		case lexer.TCos:
			funcType = ast.FuncCos
		case lexer.TCot:
			funcType = ast.FuncCot
		case lexer.TDegrees:
			funcType = ast.FuncDegrees
		case lexer.TExp:
			funcType = ast.FuncExp
		case lexer.TFloor:
			funcType = ast.FuncFloor
		case lexer.TLog:
			funcType = ast.FuncLog
		case lexer.TLog10:
			funcType = ast.FuncLog10
		case lexer.TPi:
			funcType = ast.FuncPi
		case lexer.TPower:
			funcType = ast.FuncPower
		case lexer.TRadians:
			funcType = ast.FuncRadians
		case lexer.TRands:
			funcType = ast.FuncRands
		case lexer.TRound:
			funcType = ast.FuncRound
		case lexer.TSign:
			funcType = ast.FuncSign
		case lexer.TSin:
			funcType = ast.FuncSin
		case lexer.TSqrt:
			funcType = ast.FuncSqrt
		case lexer.TSquare:
			funcType = ast.FuncSquare
		case lexer.TTan:
			funcType = ast.FuncTan
		case lexer.TFirstValue:
			funcType = ast.FuncFirstValue
		case lexer.TLastValue:
			funcType = ast.FuncLastValue
		case lexer.TLag:
			funcType = ast.FuncLag
		case lexer.TLead:
			funcType = ast.FuncLead
		case lexer.TAvg:
			funcType = ast.FuncAvg
		case lexer.TCount:
			funcType = ast.FuncCount
		case lexer.TMax:
			funcType = ast.FuncMax
		case lexer.TMin:
			funcType = ast.FuncMin
		case lexer.TStdev:
			funcType = ast.FuncStdev
		case lexer.TStdevp:
			funcType = ast.FuncStdevp
		case lexer.TSum:
			funcType = ast.FuncSum
		case lexer.TVar:
			funcType = ast.FuncVar
		case lexer.TVarp:
			funcType = ast.FuncVarp
		case lexer.TGetdate:
			funcType = ast.FuncGetdate
		}
		p.logger.Debug("in function parse")
		function := &ast.ExprFunction{Type: funcType, Name: &ast.ExprIdentifier{Value: p.currentToken.Value}}
		// parse function arguments
		err := p.expectPeek(lexer.TLeftParen)
		if err != nil {
			return nil, err
		}
		args := []ast.Expression{}
		if !p.peekTokenIs(lexer.TRightParen) {

			p.logger.Debug("parsing function args")
			for {
				err = p.expectPeekMany([]lexer.TokenType{lexer.TIdentifier,
					lexer.TNumericLiteral,
					lexer.TStringLiteral,
					lexer.TLocalVariable,
					lexer.TQuotedIdentifier,
				})
				if err != nil {
					return nil, err
				}

				if p.currentToken.Type == lexer.TLocalVariable {
					args = append(args, &ast.ExprLocalVariable{Value: p.currentToken.Value})
				} else if p.currentToken.Type == lexer.TQuotedIdentifier {
					args = append(args, &ast.ExprQuotedIdentifier{Value: p.currentToken.Value})
				} else if p.currentToken.Type == lexer.TStringLiteral {
					args = append(args, &ast.ExprStringLiteral{Value: p.currentToken.Value})
				} else if p.currentToken.Type == lexer.TNumericLiteral {
					args = append(args, &ast.ExprNumberLiteral{Value: p.currentToken.Value})
				} else {
					args = append(args, &ast.ExprIdentifier{Value: p.currentToken.Value})
				}

				if p.peekTokenIs(lexer.TRightParen) || p.peekTokenIs(lexer.TComma) {
					break
				}
				p.nextToken()
			}
		}

		err = p.expectPeek(lexer.TRightParen)
		if err != nil {
			p.logger.Debug("expected right parenthesis, got ", p.peekToken.Value)
			return nil, err
		}

		// check for over clause
		if !p.peekTokenIs(lexer.TOver) {
			return &ast.ExprFunctionCall{
				Name: function,
				Args: args,
			}, nil
		}

		overClause, err := p.parseOverClause()
		if err != nil {
			return nil, err
		}

		p.logger.Debug(overClause.TokenLiteral())

		return &ast.ExprFunctionCall{
			Name:       function,
			Args:       args,
			OverClause: overClause,
		}, nil
	case lexer.TLeftParen:
		// start of subquery
		if p.peekTokenIs(lexer.TSelect) {
			p.nextToken()
			subquery, err := p.parseSelectSubquery()
			if err != nil {
				return nil, err
			}
			p.expectPeek(lexer.TRightParen)

			p.logger.Debug("parsing subquery alias")
			p.logger.Debug(p.peekToken)
			if (p.peekTokenIs(lexer.TAs) || p.peekTokenIs(lexer.TIdentifier) ||
				p.peekTokenIs(lexer.TStringLiteral) || p.peekTokenIs(lexer.TQuotedIdentifier)) &&
				!p.peekToken2IsAny(ast.DataTypeTokenTypes) {
				p.logger.Debug("parsing subquery alias")
				exprWithAlias := &ast.ExprWithAlias{AsTokenPresent: false, Expression: &subquery}

				p.logger.Debug("parsing subquery alias")
				if p.peekToken.Type == lexer.TAs {
					exprWithAlias.AsTokenPresent = true
					p.nextToken()
				}

				// needed in case we just parsed AS keyword
				err := p.expectPeekMany([]lexer.TokenType{lexer.TIdentifier, lexer.TStringLiteral, lexer.TQuotedIdentifier})
				if err != nil {
					return nil, err
				}

				alias, err := p.parseExpression(PrecedenceLowest)
				if err != nil {
					return nil, err
				}

				switch alias.(type) {
				case *ast.ExprIdentifier, *ast.ExprStringLiteral, *ast.ExprQuotedIdentifier:
					break
				default:
					err = fmt.Errorf("Expected (Identifier or StringLiteral or QuotedIdentifier) for Alias")
					return nil, err
				}
				exprWithAlias.Alias = alias
				return exprWithAlias, nil
			}

			return &subquery, nil
		} else if p.peekTokenIs(lexer.TIdentifier) ||
			p.peekTokenIs(lexer.TLocalVariable) ||
			p.peekTokenIs(lexer.TQuotedIdentifier) ||
			p.peekTokenIs(lexer.TStringLiteral) ||
			p.peekTokenIs(lexer.TNumericLiteral) {
			stmt, err := p.parseExpressionList()
			if err != nil {
				return nil, err
			}
			p.expectPeek(lexer.TRightParen)
			p.logger.Debug("stmt: ", stmt)
			return &stmt, nil
		}
	case lexer.TPlus, lexer.TMinus:
		var operator ast.UnaryOperatorType
		switch p.currentToken.Type {
		case lexer.TPlus:
			operator = ast.UnaryOpPlus
			break
		case lexer.TMinus:
			operator = ast.UnaryOpMinus
			break
		}

		p.nextToken()

		right, err := p.parseExpression(PrecedenceLowest)
		if err != nil {
			return nil, err
		}
		newExpr = &ast.ExprUnaryOperator{
			Operator: operator,
			Right:    right,
		}
		break
	case lexer.TExists:
		p.nextToken()

		expr, err := p.parseExpression(PrecedenceLowest)
		if err != nil {
			return nil, err
		}
		// check if it is a subquery
		switch v := expr.(type) {
		case *ast.ExprSubquery:
			newExpr = &ast.ExprExistsLogicalOperator{
				Subquery: v,
			}
			break
		default:
			p.errorToken = ETCurrent
			return nil, fmt.Errorf("Expected subquery after 'EXISTS' keyword")
		}
		break
	case lexer.TNot:
		p.nextToken()

		expr, err := p.parseExpression(PrecedenceLowest)
		if err != nil {
			return nil, err
		}
		// check if it is a subquery
		newExpr = &ast.ExprNotLogicalOperator{
			Expression: expr,
		}
		break
	case lexer.TCast:
		expr, err := p.parseCast()
		if err != nil {
			return nil, err
		}
		p.logger.Debug(expr.TokenLiteral())
		newExpr = expr
	default:
		p.errorToken = ETCurrent
		return nil, fmt.Errorf("Unimplemented expression %s", p.currentToken.Type.String())
	}

	return newExpr, nil

}

func (p *Parser) parseInfixExpression(left ast.Expression) (ast.Expression, error) {
	p.logger.Debug("parsing infix expression, ", p.currentToken.String())
	switch p.currentToken.Type {
	case lexer.TAnd:
		precedence := checkPrecedence(p.currentToken.Type)
		p.nextToken()

		right, err := p.parseExpression(precedence)
		if err != nil {
			return nil, err
		}
		return &ast.ExprAndLogicalOperator{
			Left:  left,
			Right: right,
		}, nil
	case lexer.TOr:
		precedence := checkPrecedence(p.currentToken.Type)
		p.nextToken()

		right, err := p.parseExpression(precedence)
		if err != nil {
			return nil, err
		}
		return &ast.ExprOrLogicalOperator{
			Left:  left,
			Right: right,
		}, nil
	case lexer.TPlus,
		lexer.TMinus,
		lexer.TAsterisk,
		lexer.TDivide,
		lexer.TMod:
		var operator ast.ArithmeticOperatorType
		switch p.currentToken.Type {
		case lexer.TPlus:
			operator = ast.ArithmeticOpPlus
		case lexer.TMinus:
			operator = ast.ArithmeticOpMinus
		case lexer.TAsterisk:
			operator = ast.ArithmeticOpMult
		case lexer.TDivide:
			operator = ast.ArithmeticOpDiv
		case lexer.TMod:
			operator = ast.ArithmeticOpMod
		}
		precedence := checkPrecedence(p.currentToken.Type)
		p.nextToken()

		right, err := p.parseExpression(precedence)
		if err != nil {
			return nil, err
		}
		return &ast.ExprArithmeticOperator{
			Left:     left,
			Operator: operator,
			Right:    right,
		}, nil
	case lexer.TEqual,
		lexer.TNotEqualBang,
		lexer.TNotEqualArrow,
		lexer.TGreaterThan,
		lexer.TLessThan,
		lexer.TGreaterThanEqual,
		lexer.TLessThanEqual:

		var operator ast.ComparisonOperatorType
		switch p.currentToken.Type {
		case lexer.TEqual:
			operator = ast.ComparisonOpEqual
		case lexer.TNotEqualBang:
			operator = ast.ComparisonOpNotEqualBang
		case lexer.TNotEqualArrow:
			operator = ast.ComparisonOpNotEqualArrow
		case lexer.TGreaterThan:
			operator = ast.ComparisonOpGreater
		case lexer.TLessThan:
			operator = ast.ComparisonOpLess
		case lexer.TGreaterThanEqual:
			operator = ast.ComparisonOpGreaterEqual
		case lexer.TLessThanEqual:
			operator = ast.ComparisonOpLessEqual
		}
		precedence := checkPrecedence(p.currentToken.Type)
		p.nextToken()

		if p.currentTokenIs(lexer.TAll) {
			p.nextToken()
			right, err := p.parseExpression(precedence)
			if err != nil {
				return nil, err
			}
			switch v := right.(type) {
			case *ast.ExprSubquery:
				return &ast.ExprAllLogicalOperator{
					ScalarExpression:   left,
					ComparisonOperator: operator,
					Subquery:           v,
				}, nil
			}
			p.errorToken = ETCurrent
			return nil, fmt.Errorf("sub query was not provided for All Expression")
		} else if p.currentTokenIs(lexer.TSome) {
			p.nextToken()
			right, err := p.parseExpression(precedence)
			if err != nil {
				return nil, err
			}
			switch v := right.(type) {
			case *ast.ExprSubquery:
				return &ast.ExprSomeLogicalOperator{
					ScalarExpression:   left,
					ComparisonOperator: operator,
					Subquery:           v,
				}, nil
			}
			p.errorToken = ETCurrent
			return nil, p.currentErrorString("(Subquery) was not provided for Some Expression")
		} else if p.currentTokenIs(lexer.TAny) {
			p.nextToken()
			right, err := p.parseExpression(precedence)
			if err != nil {
				return nil, err
			}
			switch v := right.(type) {
			case *ast.ExprSubquery:
				return &ast.ExprAnyLogicalOperator{
					ScalarExpression:   left,
					ComparisonOperator: operator,
					Subquery:           v,
				}, nil
			}
			p.errorToken = ETCurrent
			return nil, p.currentErrorString("(Subquery) was not provided for Any Expression")
		}

		// parse the expression of the operator
		right, err := p.parseExpression(precedence)
		if err != nil {
			return nil, err
		}
		return &ast.ExprComparisonOperator{
			Left:     left,
			Operator: operator,
			Right:    right,
		}, nil
	case lexer.TBetween:
		precedence := checkPrecedence(p.currentToken.Type)
		p.nextToken()

		right, err := p.parseExpression(precedence)
		if err != nil {
			return nil, err
		}

		// check if we have and operator
		switch v := right.(type) {
		case *ast.ExprAndLogicalOperator:
			return &ast.ExprBetweenLogicalOperator{
				TestExpression: left,
				Begin:          v.Left,
				End:            v.Right,
			}, nil
		default:
			p.errorToken = ETCurrent
			return nil, p.currentErrorString("Expected 'AND' logical operator after 'BETWEEN' keyword")
		}
	case lexer.TIn:
		p.expectPeek(lexer.TLeftParen)
		if p.peekTokenIs(lexer.TSelect) {
			p.nextToken()
			statement, err := p.parseSelectSubquery()
			if err != nil {
				return nil, err
			}
			p.expectPeek(lexer.TRightParen)

			return &ast.ExprInSubqueryLogicalOperator{
				TestExpression: left,
				Subquery:       &statement,
			}, nil
		} else if p.peekTokenIs(lexer.TIdentifier) ||
			p.peekTokenIs(lexer.TLocalVariable) ||
			p.peekTokenIs(lexer.TQuotedIdentifier) ||
			p.peekTokenIs(lexer.TStringLiteral) ||
			p.peekTokenIs(lexer.TNumericLiteral) {
			stmt, err := p.parseExpressionList()
			if err != nil {
				return nil, err
			}
			p.expectPeek(lexer.TRightParen)

			return &ast.ExprInLogicalOperator{
				TestExpression: left,
				Expressions:    stmt.List,
			}, nil
		}
		p.errorToken = ETCurrent
		return nil, p.currentErrorString("Expected (Subquery or Expression List) after 'IN' keyword")
	case lexer.TLike:
		precedence := checkPrecedence(p.currentToken.Type)
		p.nextToken()

		right, err := p.parseExpression(precedence)
		if err != nil {
			return nil, err
		}

		return &ast.ExprLikeLogicalOperator{
			MatchExpression: left,
			Pattern:         right,
		}, nil
	case lexer.TNot:
		p.nextToken()

		if p.currentTokenIs(lexer.TBetween) {
			precedence := checkPrecedence(p.currentToken.Type)
			p.nextToken()

			right, err := p.parseExpression(precedence)
			if err != nil {
				return nil, err
			}

			// check if we have and operator
			switch v := right.(type) {
			case *ast.ExprAndLogicalOperator:
				return &ast.ExprBetweenLogicalOperator{
					TestExpression: left,
					Not:            true,
					Begin:          v.Left,
					End:            v.Right,
				}, nil
			default:
				p.errorToken = ETCurrent
				return nil, p.currentErrorString("Expected (Subquery) after 'NOT BETWEEN' keyword")
			}
		} else if p.currentTokenIs(lexer.TIn) {
			p.expectPeek(lexer.TLeftParen)
			if p.peekTokenIs(lexer.TSelect) {
				p.nextToken()
				statement, err := p.parseSelectSubquery()
				if err != nil {
					return nil, err
				}
				p.expectPeek(lexer.TRightParen)

				return &ast.ExprInSubqueryLogicalOperator{
					TestExpression: left,
					Not:            true,
					Subquery:       &statement,
				}, nil
			} else if p.peekTokenIs(lexer.TIdentifier) ||
				p.peekTokenIs(lexer.TLocalVariable) ||
				p.peekTokenIs(lexer.TQuotedIdentifier) ||
				p.peekTokenIs(lexer.TStringLiteral) ||
				p.peekTokenIs(lexer.TNumericLiteral) {
				stmt, err := p.parseExpressionList()
				if err != nil {
					return nil, err
				}
				p.expectPeek(lexer.TRightParen)

				return &ast.ExprInLogicalOperator{
					TestExpression: left,
					Not:            true,
					Expressions:    stmt.List,
				}, nil
			}
			p.errorToken = ETCurrent
			return nil, p.currentErrorString("(Subquery or Expression List) after 'NOT IN' keyword")
		} else if p.currentTokenIs(lexer.TLike) {
			precedence := checkPrecedence(p.currentToken.Type)
			p.nextToken()

			right, err := p.parseExpression(precedence)
			if err != nil {
				return nil, err
			}

			return &ast.ExprLikeLogicalOperator{
				MatchExpression: left,
				Not:             true,
				Pattern:         right,
			}, nil
		} else {
			p.errorToken = ETCurrent
			return nil, p.currentErrorString("(BETWEEN Expression or IN Expression or LIKE Expression) after 'Test Expression NOT' Expression")
		}
	}
	p.errorToken = ETCurrent
	return nil, p.currentErrorString("Unimplemented expression")
}
