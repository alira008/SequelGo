package lexer

type TokenType uint8

const (
	TEndOfFile TokenType = iota
	TSyntaxError

	TLocalVariable
	TKeyword

	// Literals
	TNumericLiteral
	TStringLiteral
	TQuotedStringLiteral

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

type Lexer struct {
	input   string
	current int
	start   int
	end     int
	ch      byte
}

type Token struct {
	Type  TokenType
	Value string
}

func NewLexer(input string) *Lexer {
	lexer := &Lexer{input: input}
	lexer.readChar()
	return lexer
}

func (l *Lexer) readChar() {
	if l.current >= len(l.input) {
		return
	}

	l.ch = l.input[l.current]
	l.current++
}

func (l *Lexer) peekChar() byte {
	if l.current >= len(l.input) {
		return 0
	}
	return l.input[l.current]
}

func (l *Lexer) skipWhitespace() {
	for l.ch == ' ' || l.ch == '\t' || l.ch == '\n' || l.ch == '\r' {
		l.readChar()
	}
}

func (l *Lexer) NextToken() Token {
	l.skipWhitespace()
	token := Token{}
	switch l.ch {
	case '=':
		if l.peekChar() == '=' {
			l.readChar()
			token.Type = TDoubleEqual
			token.Value = "=="
		} else {
			token.Type = TDoubleEqual
			token.Value = "="
		}
	default:
		token.Type = TSyntaxError
		token.Value = string(l.ch)
	}

	return token
}
