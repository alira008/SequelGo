package lexer

import (
	"fmt"
	"strings"
)

type TokenType uint8

const (
	TEndOfFile TokenType = iota
	TSyntaxError

	TLocalVariable

	// Literals
	TIdentifier
	TNumericLiteral
	TStringLiteral
	TQuotedIdentifier

	TComma
	TLeftParen
	TRightParen
	TDoubleEqual
	TEqual
	TNotEqual
	TLessThan
	TLessThanEqual
	TGreaterThan
	TGreaterThanEqual
	TPlus
	TMinus
	TDivide
	TAsterisk
	TMod
	TPlusEqual
	TMinusEqual
	TDivideEqual
	TMultiplyEqual
	TPercentEqual
	TAndEqual
	TOrEqual
	TCaretEqual
	TPeriod
	TSemiColon
	TLeftBracket
	TRightBracket
	TLeftBrace
	TRightBrace
	TTilde
	TExclamationMark

	// Keywords
	TAbs
	TAcos
	TAll
	TAlter
	TAnd
	TAny
	TAs
	TAsc
	TAsin
	TAtan
	TAutoincrement
	TAvg
	TBegin
	TBetween
	TBigint
	TBit
	TBy
	TCascade
	TCase
	TCast
	TCeil
	TCeiling
	TChar
	TColumn
	TColumns
	TCommit
	TCommited
	TConstraint
	TCos
	TCot
	TCount
	TCreate
	TCurrent
	TDate
	TDatetime
	TDay
	TDayofweek
	TDayofyear
	TDecimal
	TDeclare
	TDegrees
	TDefault
	TDelete
	TDenseRank
	TDesc
	TDescribe
	TDistinct
	TDo
	TDrop
	TElse
	TEnd
	TEngine
	TExec
	TExecute
	TExists
	TExp
	TFalse
	TFetch
	TFirst
	TFirstValue
	TFloat
	TFloor
	TFollowing
	TForeign
	TFrom
	TFull
	TFunction
	TGetdate
	TGroup
	THaving
	THour
	THours
	TIdentity
	TIf
	TIn
	TIncrement
	TIndex
	TInner
	TInsert
	TInteger
	TIntersect
	TInt
	TInto
	TIs
	TJoin
	TKey
	TLag
	TLast
	TLastValue
	TLead
	TLeft
	TLike
	TLimit
	TLog
	TLog10
	TMax
	TMicrosecond
	TMicroseconds
	TMillisecond
	TMilliseconds
	TMin
	TMinute
	TMonth
	TNanosecond
	TNanoseconds
	TNchar
	TNext
	TNot
	TNull
	TNullif
	TNumeric
	TNvarchar
	TOffset
	TOn
	TOnly
	TOr
	TOrder
	TOuter
	TOver
	TPartition
	TPassword
	TPercent
	TPi
	TPower
	TPreceding
	TProcedure
	TRadians
	TRands
	TRange
	TRank
	TReal
	TReturn
	TReturns
	TRevoke
	TRight
	TRole
	TRollback
	TRound
	TRow
	TRowid
	TRows
	TRowNumber
	TSecond
	TSelect
	TSet
	TSign
	TSin
	TSmallint
	TSnapshot
	TSome
	TSqrt
	TSquare
	TStage
	TStart
	TStatistics
	TStdev
	TStdevp
	TSum
	TTable
	TTan
	TTemp
	TThen
	TTies
	TTime
	TTinyint
	TTop
	TTransaction
	TTrigger
	TTrue
	TTruncate
	TUnbounded
	TUncommitted
	TUnion
	TUnique
	TUnlock
	TUpdate
	TUpper
	TUse
	TUser
	TUuid
	TValue
	TValues
	TVarbinary
	TVarchar
	TVar
	TVarp
	TWeek
	TWhen
	TWhere
	TWindow
	TWith
	TYear
)

var Keywords = map[string]TokenType{
	// Reserved Keywords
	"abs":           TAbs,
	"acos":          TAcos,
	"all":           TAll,
	"alter":         TAlter,
	"and":           TAnd,
	"any":           TAny,
	"as":            TAs,
	"asc":           TAsc,
	"asin":          TAsin,
	"atan":          TAtan,
	"autoincrement": TAutoincrement,
	"avg":           TAvg,
	"begin":         TBegin,
	"between":       TBetween,
	"bigint":        TBigint,
	"bit":           TBit,
	"by":            TBy,
	"cascade":       TCascade,
	"case":          TCase,
	"cast":          TCast,
	"ceil":          TCeil,
	"ceiling":       TCeiling,
	"char":          TChar,
	"column":        TColumn,
	"columns":       TColumns,
	"commit":        TCommit,
	"commited":      TCommited,
	"constraint":    TConstraint,
	"cos":           TCos,
	"cot":           TCot,
	"count":         TCount,
	"create":        TCreate,
	"current":       TCurrent,
	"date":          TDate,
	"datetime":      TDatetime,
	"day":           TDay,
	"dayofweek":     TDayofweek,
	"dayofyear":     TDayofyear,
	"decimal":       TDecimal,
	"declare":       TDeclare,
	"degrees":       TDegrees,
	"default":       TDefault,
	"delete":        TDelete,
	"dense_rank":    TDenseRank,
	"desc":          TDesc,
	"describe":      TDescribe,
	"distinct":      TDistinct,
	"do":            TDo,
	"drop":          TDrop,
	"else":          TElse,
	"end":           TEnd,
	"engine":        TEngine,
	"exec":          TExec,
	"execute":       TExecute,
	"exists":        TExists,
	"exp":           TExp,
	"false":         TFalse,
	"fetch":         TFetch,
	"first":         TFirst,
	"first_value":   TFirstValue,
	"float":         TFloat,
	"floor":         TFloor,
	"following":     TFollowing,
	"foreign":       TForeign,
	"from":          TFrom,
	"full":          TFull,
	"function":      TFunction,
	"getdate":       TGetdate,
	"group":         TGroup,
	"having":        THaving,
	"hour":          THour,
	"hours":         THours,
	"identity":      TIdentity,
	"if":            TIf,
	"in":            TIn,
	"increment":     TIncrement,
	"index":         TIndex,
	"inner":         TInner,
	"insert":        TInsert,
	"integer":       TInteger,
	"intersect":     TIntersect,
	"int":           TInt,
	"into":          TInto,
	"is":            TIs,
	"join":          TJoin,
	"key":           TKey,
	"lag":           TLag,
	"last":          TLast,
	"last_value":    TLastValue,
	"lead":          TLead,
	"left":          TLeft,
	"like":          TLike,
	"limit":         TLimit,
	"log":           TLog,
	"log10":         TLog10,
	"max":           TMax,
	"microsecond":   TMicrosecond,
	"microseconds":  TMicroseconds,
	"millisecond":   TMillisecond,
	"milliseconds":  TMilliseconds,
	"min":           TMin,
	"minute":        TMinute,
	"month":         TMonth,
	"nanosecond":    TNanosecond,
	"nanoseconds":   TNanoseconds,
	"nchar":         TNchar,
	"next":          TNext,
	"not":           TNot,
	"null":          TNull,
	"nullif":        TNullif,
	"numeric":       TNumeric,
	"nvarchar":      TNvarchar,
	"offset":        TOffset,
	"on":            TOn,
	"only":          TOnly,
	"or":            TOr,
	"order":         TOrder,
	"outer":         TOuter,
	"over":          TOver,
	"partition":     TPartition,
	"password":      TPassword,
	"percent":       TPercent,
	"pi":            TPi,
	"power":         TPower,
	"preceding":     TPreceding,
	"procedure":     TProcedure,
	"radians":       TRadians,
	"rands":         TRands,
	"range":         TRange,
	"rank":          TRank,
	"real":          TReal,
	"return":        TReturn,
	"returns":       TReturns,
	"revoke":        TRevoke,
	"right":         TRight,
	"role":          TRole,
	"rollback":      TRollback,
	"round":         TRound,
	"row":           TRow,
	"rowid":         TRowid,
	"rows":          TRows,
	"row_number":    TRowNumber,
	"second":        TSecond,
	"select":        TSelect,
	"set":           TSet,
	"sign":          TSign,
	"sin":           TSin,
	"smallint":      TSmallint,
	"snapshot":      TSnapshot,
	"some":          TSome,
	"sqrt":          TSqrt,
	"square":        TSquare,
	"stage":         TStage,
	"start":         TStart,
	"tstatistics":   TStatistics,
	"stdev":         TStdev,
	"stdevp":        TStdevp,
	"sum":           TSum,
	"table":         TTable,
	"tan":           TTan,
	"temp":          TTemp,
	"then":          TThen,
	"ties":          TTies,
	"time":          TTime,
	"tinyint":       TTinyint,
	"top":           TTop,
	"transaction":   TTransaction,
	"trigger":       TTrigger,
	"true":          TTrue,
	"truncate":      TTruncate,
	"unbounded":     TUnbounded,
	"uncommitted":   TUncommitted,
	"union":         TUnion,
	"unique":        TUnique,
	"unlock":        TUnlock,
	"update":        TUpdate,
	"upper":         TUpper,
	"use":           TUse,
	"user":          TUser,
	"uuid":          TUuid,
	"value":         TValue,
	"values":        TValues,
	"varbinary":     TVarbinary,
	"varchar":       TVarchar,
	"var":           TVar,
	"varp":          TVarp,
	"week":          TWeek,
	"when":          TWhen,
	"where":         TWhere,
	"window":        TWindow,
	"with":          TWith,
	"year":          TYear,
}

func (t TokenType) String() string {
	switch t {
	case TEndOfFile:
		return "EndOfFile"
	case TSyntaxError:
		return "SyntaxError"
	case TLocalVariable:
		return "LocalVariable"
	case TIdentifier:
		return "Identifier"
	case TNumericLiteral:
		return "NumericLiteral"
	case TStringLiteral:
		return "StringLiteral"
	case TQuotedIdentifier:
		return "QuotedIdentifier"
	case TComma:
		return "Comma"
	case TLeftParen:
		return "LeftParen"
	case TRightParen:
		return "RightParen"
	case TDoubleEqual:
		return "DoubleEqual"
	case TEqual:
		return "Equal"
	case TNotEqual:
		return "NotEqual"
	case TLessThan:
		return "LessThan"
	case TLessThanEqual:
		return "LessThanEqual"
	case TGreaterThan:
		return "GreaterThan"
	case TGreaterThanEqual:
		return "GreaterThanEqual"
	case TPlus:
		return "Plus"
	case TMinus:
		return "Minus"
	case TDivide:
		return "Divide"
	case TAsterisk:
		return "Asterisk"
	case TMod:
		return "Mod"
	case TPlusEqual:
		return "PlusEqual"
	case TMinusEqual:
		return "MinusEqual"
	case TDivideEqual:
		return "DivideEqual"
	case TMultiplyEqual:
		return "MultiplyEqual"
	case TPercentEqual:
		return "PercentEqual"
	case TAndEqual:
		return "AndEqual"
	case TOrEqual:
		return "OrEqual"
	case TCaretEqual:
		return "CaretEqual"
	case TPeriod:
		return "Period"
	case TSemiColon:
		return "SemiColon"
	case TLeftBracket:
		return "LeftBracket"
	case TRightBracket:
		return "RightBracket"
	case TLeftBrace:
		return "LeftBrace"
	case TRightBrace:
		return "RightBrace"
	case TTilde:
		return "Tilde"
	case TExclamationMark:
		return "ExclamationMark"
	case TAbs:
		return "Abs"
	case TAcos:
		return "Acos"
	case TAll:
		return "All"
	case TAlter:
		return "Alter"
	case TAnd:
		return "And"
	case TAny:
		return "Any"
	case TAs:
		return "As"
	case TAsc:
		return "Asc"
	case TAsin:
		return "Asin"
	case TAtan:
		return "Atan"
	case TAutoincrement:
		return "Autoincrement"
	case TAvg:
		return "Avg"
	case TBegin:
		return "Begin"
	case TBetween:
		return "Between"
	case TBigint:
		return "Bigint"
	case TBit:
		return "Bit"
	case TBy:
		return "By"
	case TCascade:
		return "Cascade"
	case TCase:
		return "Case"
	case TCast:
		return "Cast"
	case TCeil:
		return "Ceil"
	case TCeiling:
		return "Ceiling"
	case TChar:
		return "Char"
	case TColumn:
		return "Column"
	case TColumns:
		return "Columns"
	case TCommit:
		return "Commit"
	case TCommited:
		return "Commited"
	case TConstraint:
		return "Constraint"
	case TCos:
		return "Cos"
	case TCot:
		return "Cot"
	case TCount:
		return "Count"
	case TCreate:
		return "Create"
	case TCurrent:
		return "Current"
	case TDate:
		return "Date"
	case TDatetime:
		return "Datetime"
	case TDay:
		return "Day"
	case TDayofweek:
		return "Dayofweek"
	case TDayofyear:
		return "Dayofyear"
	case TDecimal:
		return "Decimal"
	case TDeclare:
		return "Declare"
	case TDegrees:
		return "Degrees"
	case TDefault:
		return "Default"
	case TDelete:
		return "Delete"
	case TDenseRank:
		return "DenseRank"
	case TDesc:
		return "Desc"
	case TDescribe:
		return "Describe"
	case TDistinct:
		return "Distinct"
	case TDo:
		return "Do"
	case TDrop:
		return "Drop"
	case TElse:
		return "Else"
	case TEnd:
		return "End"
	case TEngine:
		return "Engine"
	case TExec:
		return "Exec"
	case TExecute:
		return "Execute"
	case TExists:
		return "Exists"
	case TExp:
		return "Exp"
	case TFalse:
		return "False"
	case TFetch:
		return "Fetch"
	case TFirst:
		return "First"
	case TFirstValue:
		return "FirstValue"
	case TFloat:
		return "Float"
	case TFloor:
		return "Floor"
	case TFollowing:
		return "Following"
	case TForeign:
		return "Foreign"
	case TFrom:
		return "From"
	case TFull:
		return "Full"
	case TFunction:
		return "Function"
	case TGetdate:
		return "Getdate"
	case TGroup:
		return "Group"
	case THaving:
		return "Having"
	case THour:
		return "Hour"
	case THours:
		return "Hours"
	case TIdentity:
		return "Identity"
	case TIf:
		return "If"
	case TIn:
		return "In"
	case TIncrement:
		return "Increment"
	case TIndex:
		return "Index"
	case TInner:
		return "Inner"
	case TInsert:
		return "Insert"
	case TInteger:
		return "Integer"
	case TIntersect:
		return "Intersect"
	case TInt:
		return "Int"
	case TInto:
		return "Into"
	case TIs:
		return "Is"
	case TJoin:
		return "Join"
	case TKey:
		return "Key"
	case TLag:
		return "Lag"
	case TLast:
		return "Last"
	case TLastValue:
		return "LastValue"
	case TLead:
		return "Lead"
	case TLeft:
		return "Left"
	case TLike:
		return "Like"
	case TLimit:
		return "Limit"
	case TLog:
		return "Log"
	case TLog10:
		return "Log10"
	case TMax:
		return "Max"
	case TMicrosecond:
		return "Microsecond"
	case TMicroseconds:
		return "Microseconds"
	case TMillisecond:
		return "Millisecond"
	case TMilliseconds:
		return "Milliseconds"
	case TMin:
		return "Min"
	case TMinute:
		return "Minute"
	case TMonth:
		return "Month"
	case TNanosecond:
		return "Nanosecond"
	case TNanoseconds:
		return "Nanoseconds"
	case TNchar:
		return "Nchar"
	case TNext:
		return "Next"
	case TNot:
		return "Not"
	case TNull:
		return "Null"
	case TNullif:
		return "Nullif"
	case TNumeric:
		return "Numeric"
	case TNvarchar:
		return "Nvarchar"
	case TOffset:
		return "Offset"
	case TOn:
		return "On"
	case TOnly:
		return "Only"
	case TOr:
		return "Or"
	case TOrder:
		return "Order"
	case TOuter:
		return "Outer"
	case TOver:
		return "Over"
	case TPartition:
		return "Partition"
	case TPassword:
		return "Password"
	case TPercent:
		return "Percent"
	case TPi:
		return "Pi"
	case TPower:
		return "Power"
	case TPreceding:
		return "Preceding"
	case TProcedure:
		return "Procedure"
	case TRadians:
		return "Radians"
	case TRands:
		return "Rands"
	case TRange:
		return "Range"
	case TRank:
		return "Rank"
	case TReal:
		return "Real"
	case TReturn:
		return "Return"
	case TReturns:
		return "Returns"
	case TRevoke:
		return "Revoke"
	case TRight:
		return "Right"
	case TRole:
		return "Role"
	case TRollback:
		return "Rollback"
	case TRound:
		return "Round"
	case TRow:
		return "Row"
	case TRowid:
		return "Rowid"
	case TRows:
		return "Rows"
	case TRowNumber:
		return "RowNumber"
	case TSecond:
		return "Second"
	case TSelect:
		return "Select"
	case TSet:
		return "Set"
	case TSign:
		return "Sign"
	case TSin:
		return "Sin"
	case TSmallint:
		return "Smallint"
	case TSnapshot:
		return "Snapshot"
	case TSome:
		return "Some"
	case TSqrt:
		return "Sqrt"
	case TSquare:
		return "Square"
	case TStage:
		return "Stage"
	case TStart:
		return "Start"
	case TStatistics:
		return "Statistics"
	case TStdev:
		return "Stdev"
	case TStdevp:
		return "Stdevp"
	case TSum:
		return "Sum"
	case TTable:
		return "Table"
	case TTan:
		return "Tan"
	case TTemp:
		return "Temp"
	case TThen:
		return "Then"
	case TTies:
		return "Ties"
	case TTime:
		return "Time"
	case TTinyint:
		return "Tinyint"
	case TTop:
		return "Top"
	case TTransaction:
		return "Transaction"
	case TTrigger:
		return "Trigger"
	case TTrue:
		return "True"
	case TTruncate:
		return "Truncate"
	case TUnbounded:
		return "Unbounded"
	case TUncommitted:
		return "Uncommitted"
	case TUnion:
		return "Union"
	case TUnique:
		return "Unique"
	case TUnlock:
		return "Unlock"
	case TUpdate:
		return "Update"
	case TUpper:
		return "Upper"
	case TUse:
		return "Use"
	case TUser:
		return "User"
	case TUuid:
		return "Uuid"
	case TValue:
		return "Value"
	case TValues:
		return "Values"
	case TVarbinary:
		return "Varbinary"
	case TVarchar:
		return "Varchar"
	case TVar:
		return "Var"
	case TVarp:
		return "Varp"
	case TWeek:
		return "Week"
	case TWhen:
		return "When"
	case TWhere:
		return "Where"
	case TWindow:
		return "Window"
	case TWith:
		return "With"
	case TYear:
		return "Year"
	}
	return "Unimplemented"
}

type Lexer struct {
	input   string
	read    int
	current int
	ch      byte
	line    int
	col     int
}

type Token struct {
	Type  TokenType
	Value string
	Start Position
	End   Position
}

type Position struct {
	Line int
	Col  int
}

func NewPosition(line, col int) Position {
	return Position{Line: line, Col: col}
}

func (p *Position) String() string {
	return fmt.Sprintf("{Line: %d Col: %d}", p.Line, p.Col)
}

func NewLexer(input string) *Lexer {
	lexer := &Lexer{input: input}
	lexer.readChar()
	return lexer
}

func (l *Lexer) readChar() {
	if l.read >= len(l.input) {
		l.ch = 0
	} else {
		l.ch = l.input[l.read]
	}

	if l.ch == '\n' || l.ch == '\r' {
		l.line++
		l.col = 0
	} else if l.ch == '\t' {
		l.col += 4
	} else {
		l.col++
	}

	l.current = l.read
	l.read += 1
}

func (l *Lexer) peekChar() byte {
	if l.read >= len(l.input) {
		return 0
	}
	return l.input[l.read]
}

func (l *Lexer) skipWhitespace() {
	for l.ch == ' ' || l.ch == '\t' || l.ch == '\n' || l.ch == '\r' {
		l.readChar()
	}
}

func (l *Lexer) isLetter(ch byte) bool {
	return 'a' <= ch && ch <= 'z' || 'A' <= ch && ch <= 'Z' || ch == '_'
}

func (l *Lexer) isDigit(ch byte) bool {
	return '0' <= ch && ch <= '9'
}

func (l *Lexer) isAlphaNumeric(ch byte) bool {
	return l.isLetter(ch) || l.isDigit(ch)
}

func (l *Lexer) readQuotedIdentifier() string {
	// skip the quote character
	l.readChar()

	// read the identifier until next quote
	start := l.current

	for {
		peekChar := l.peekChar()
		if peekChar == ']' || peekChar == 0 {
			break
		}
		l.readChar()
	}

	// go to the quote character
	l.readChar()

	return l.input[start:l.current]
}

func (l *Lexer) readQuotedString() string {
	// skip the quote character
	l.readChar()

	// read the identifier until next quote
	start := l.current

	for {
		peekChar := l.peekChar()
		if peekChar == '\'' || peekChar == 0 {
			break
		}
		l.readChar()
	}

	// go to the quote character
	l.readChar()

	return l.input[start:l.current]
}

func (l *Lexer) readNumber() string {
	start := l.current
	for l.isDigit(l.peekChar()) {
		l.readChar()
	}

	// check for floating point
	if l.peekChar() == '.' {
		l.readChar()

		for l.isDigit(l.peekChar()) {
			l.readChar()
		}
	}

	if l.current+1 >= len(l.input) {
		return l.input[start:]
	}
	return l.input[start : l.current+1]
}

func (l *Lexer) readIdentifier() string {
	start := l.current
	peekChar := l.peekChar()
	for l.isAlphaNumeric(peekChar) || peekChar == '_' {
		l.readChar()
		peekChar = l.peekChar()
	}

	if l.current+1 >= len(l.input) {
		return l.input[start:]
	}
	return l.input[start : l.current+1]
}

func (l *Lexer) NextToken() Token {
	l.skipWhitespace()
	token := Token{}
	token.Start.Col = l.col
	token.Start.Line = l.line
	switch l.ch {
	case ',':
		token.Type = TComma
		token.Value = ","
	case '(':
		token.Type = TLeftParen
		token.Value = "("
	case ')':
		token.Type = TRightParen
		token.Value = ")"
	case '=':
		if l.peekChar() == '=' {
			l.readChar()
			token.Type = TDoubleEqual
			token.Value = "=="
		} else {
			token.Type = TEqual
			token.Value = "="
		}
	case '!':
		if l.peekChar() == '=' {
			l.readChar()
			token.Type = TNotEqual
			token.Value = "!="
		} else {
			token.Type = TExclamationMark
			token.Value = "!"
		}
	case '<':
		if l.peekChar() == '=' {
			l.readChar()
			token.Type = TLessThanEqual
			token.Value = "<="
		} else {
			token.Type = TLessThan
			token.Value = "<"
		}
	case '>':
		if l.peekChar() == '=' {
			l.readChar()
			token.Type = TGreaterThanEqual
			token.Value = ">="
		} else {
			token.Type = TGreaterThan
			token.Value = ">"
		}
	case '+':
		if l.peekChar() == '=' {
			l.readChar()
			token.Type = TPlusEqual
			token.Value = "+="
		} else {
			token.Type = TPlus
			token.Value = "+"
		}
	case '-':
		if l.peekChar() == '=' {
			l.readChar()
			token.Type = TMinusEqual
			token.Value = "-="
		} else {
			token.Type = TPlus
			token.Value = "-"
		}
	case '/':
		if l.peekChar() == '=' {
			l.readChar()
			token.Type = TDivideEqual
			token.Value = "/="
		} else {
			token.Type = TDivide
			token.Value = "/"
		}
	case '*':
		if l.peekChar() == '=' {
			l.readChar()
			token.Type = TMultiplyEqual
			token.Value = "*="
		} else {
			token.Type = TAsterisk
			token.Value = "*"
		}
	case '%':
		if l.peekChar() == '=' {
			l.readChar()
			token.Type = TPercentEqual
			token.Value = "%="
		} else {
			token.Type = TPercent
			token.Value = "*"
		}
	case '^':
		if l.peekChar() == '=' {
			l.readChar()
			token.Type = TCaretEqual
			token.Value = "^="
		} else {
			token.Type = TSyntaxError
			token.Value = "^"
		}
	case '|':
		if l.peekChar() == '=' {
			l.readChar()
			token.Type = TOrEqual
			token.Value = "|="
		} else {
			token.Type = TOr
			token.Value = "|"
		}
	case '&':
		if l.peekChar() == '=' {
			l.readChar()
			token.Type = TAndEqual
			token.Value = "&="
		} else {
			token.Type = TAnd
			token.Value = "&"
		}
	case '.':
		token.Type = TPeriod
		token.Value = "."
	case ';':
		token.Type = TSemiColon
		token.Value = ";"
	case '[':
		peekChar := l.peekChar()
		if l.isAlphaNumeric(peekChar) {
			// Read identifier until ']'
			quotedIdentifier := l.readQuotedIdentifier()
			// if the last character is not ']', then it's a syntax error
			if l.ch == 0 {
				token.Type = TSyntaxError
				token.Value = quotedIdentifier
			} else {
				token.Type = TQuotedIdentifier
				token.Value = quotedIdentifier
			}
		} else {
			token.Type = TLeftBracket
			token.Value = "["
		}
	case ']':
		token.Type = TRightBracket
		token.Value = "]"
	case '\'':
		peekChar := l.peekChar()
		if l.isAlphaNumeric(peekChar) {
			// Read identifier until '\''
			stringLiteral := l.readQuotedString()
			// if the last character is not '\'', then it's a syntax error
			if l.ch == 0 {
				token.Type = TSyntaxError
				token.Value = stringLiteral
			} else {
				token.Type = TStringLiteral
				token.Value = stringLiteral
			}
		} else {
			token.Type = TSyntaxError
			token.Value = "'"
		}
	case '{':
		token.Type = TLeftBrace
		token.Value = "{"
	case '}':
		token.Type = TRightBrace
		token.Value = "}"
	case '~':
		token.Type = TTilde
		token.Value = "~"
	case '@':
		// skip the @ character
		l.readChar()
		localVariable := l.readIdentifier()
		token.Type = TLocalVariable
		token.Value = localVariable
	case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
		number := l.readNumber()
		token.Type = TNumericLiteral
		token.Value = number
	case 0:
		token.Type = TEndOfFile
		token.Value = ""
	default:
		if l.isLetter(l.ch) || l.ch == '_' {
			identifier := l.readIdentifier()
			lowerIdentifier := strings.ToLower(identifier)
			keyword, ok := Keywords[lowerIdentifier]
			if ok {
				token.Type = keyword
				token.Value = identifier
			} else {

				token.Type = TIdentifier
				token.Value = identifier
			}
		} else {
			token.Type = TSyntaxError
			token.Value = string(l.ch)
		}
	}

	token.End.Col = l.col
	token.End.Line = l.line

	l.readChar()

	return token
}

func (t *Token) String() string {
	return fmt.Sprintf("{Value: %s, Start line: %d, Start col: %d,  End line: %d, End col: %d}", strings.ToLower(t.Value), t.Start.Line, t.Start.Col, t.End.Line, t.End.Col)
}
