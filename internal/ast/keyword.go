package ast

import "SequelGo/internal/lexer"

type Keyword struct {
	Span
	Type KeywordType
}

func NewKeywordFromToken(token lexer.Token) Keyword {
	keywordType, ok := Keywords[token.Value]
	if !ok {
		keywordType = KAll
	}
	return Keyword{
		Span: NewSpanFromLexerPosition(token.Start, token.End),
		Type: keywordType,
	}
}

func (k Keyword) expressionNode()    {}
func (k *Keyword) SetSpan(span Span) { k.Span = span }
func (k Keyword) GetSpan() Span      { return k.Span }

type KeywordType uint8

const (
	KAll KeywordType = iota
	KAlter
	KAnd
	KAny
	KAs
	KAsc
	KAutoincrement
	KBegin
	KBetween
	KBy
	KCascade
	KCase
	KChar
	KColumn
	KColumns
	KCommit
	KCommited
	KConstraint
	KCreate
	KCurrent
	KDay
	KDayofweek
	KDayofyear
	KDeclare
	KDegrees
	KDefault
	KDelete
	KDesc
	KDescribe
	KDistinct
	KDo
	KDrop
	KElse
	KEnd
	KEngine
	KExec
	KExecute
	KExists
	KFalse
	KFetch
	KFirst
	KFloat
	KFloor
	KFollowing
	KForeign
	KFrom
	KFull
	KFunction
	KGroup
	KHaving
	KHour
	KHours
	KIdentity
	KIf
	KIn
	KIncrement
	KIndex
	KInner
	KInsert
	KInteger
	KIntersect
	KInt
	KInto
	KIs
	KJoin
	KKey
	KLast
	KLead
	KLeft
	KLike
	KLimit
	KMicrosecond
	KMicroseconds
	KMillisecond
	KMilliseconds
	KMin
	KMinute
	KMonth
	KNanosecond
	KNanoseconds
	KNchar
	KNext
	KNot
	KOffset
	KOn
	KOnly
	KOr
	KOrder
	KOuter
	KOver
	KPartition
	KPassword
	KPercent
	KPi
	KPower
	KPreceding
	KProcedure
	KRadians
	KRands
	KReturn
	KReturns
	KRevoke
	KRight
	KRole
	KRollback
	KRound
	KRow
	KRowId
	KRows
	KRowNumber
	KSecond
	KSelect
	KSet
	KSign
	KSnapshot
	KSome
	KStage
	KStart
	KStatistics
	KTable
	KTemp
	KThen
	KTies
	KTop
	KTransaction
	KTrigger
	KTrue
	KTruncate
	KUnbounded
	KUncommitted
	KUnion
	KUnique
	KUnlock
	KUpdate
	KUpper
	KUse
	KUser
	KUuid
	KValue
	KValues
	KWeek
	KWhen
	KWhere
	KWindow
	KWith
	KYear
)

var Keywords = map[string]KeywordType{
	"all":           KAll,
	"alter":         KAlter,
	"and":           KAnd,
	"any":           KAny,
	"as":            KAs,
	"asc":           KAsc,
	"autoincrement": KAutoincrement,
	"begin":         KBegin,
	"between":       KBetween,
	"by":            KBy,
	"cascade":       KCascade,
	"case":          KCase,
	"char":          KChar,
	"column":        KColumn,
	"columns":       KColumns,
	"commit":        KCommit,
	"commited":      KCommited,
	"constraint":    KConstraint,
	"create":        KCreate,
	"current":       KCurrent,
	"day":           KDay,
	"dayofweek":     KDayofweek,
	"dayofyear":     KDayofyear,
	"declare":       KDeclare,
	"degrees":       KDegrees,
	"default":       KDefault,
	"delete":        KDelete,
	"desc":          KDesc,
	"describe":      KDescribe,
	"distinct":      KDistinct,
	"do":            KDo,
	"drop":          KDrop,
	"else":          KElse,
	"end":           KEnd,
	"engine":        KEngine,
	"exec":          KExec,
	"execute":       KExecute,
	"exists":        KExists,
	"false":         KFalse,
	"fetch":         KFetch,
	"first":         KFirst,
	"float":         KFloat,
	"floor":         KFloor,
	"following":     KFollowing,
	"foreign":       KForeign,
	"from":          KFrom,
	"full":          KFull,
	"function":      KFunction,
	"group":         KGroup,
	"having":        KHaving,
	"hour":          KHour,
	"hours":         KHours,
	"identity":      KIdentity,
	"if":            KIf,
	"in":            KIn,
	"increment":     KIncrement,
	"index":         KIndex,
	"inner":         KInner,
	"insert":        KInsert,
	"integer":       KInteger,
	"intersect":     KIntersect,
	"int":           KInt,
	"into":          KInto,
	"is":            KIs,
	"join":          KJoin,
	"key":           KKey,
	"last":          KLast,
	"lead":          KLead,
	"left":          KLeft,
	"like":          KLike,
	"limit":         KLimit,
	"microsecond":   KMicrosecond,
	"microseconds":  KMicroseconds,
	"millisecond":   KMillisecond,
	"milliseconds":  KMilliseconds,
	"min":           KMin,
	"minute":        KMinute,
	"month":         KMonth,
	"nanosecond":    KNanosecond,
	"nanoseconds":   KNanoseconds,
	"nchar":         KNchar,
	"next":          KNext,
	"not":           KNot,
	"offset":        KOffset,
	"on":            KOn,
	"only":          KOnly,
	"or":            KOr,
	"order":         KOrder,
	"outer":         KOuter,
	"over":          KOver,
	"partition":     KPartition,
	"password":      KPassword,
	"percent":       KPercent,
	"pi":            KPi,
	"power":         KPower,
	"preceding":     KPreceding,
	"procedure":     KProcedure,
	"radians":       KRadians,
	"rands":         KRands,
	"return":        KReturn,
	"returns":       KReturns,
	"revoke":        KRevoke,
	"right":         KRight,
	"role":          KRole,
	"rollback":      KRollback,
	"round":         KRound,
	"row":           KRow,
	"rowid":         KRowId,
	"rows":          KRows,
	"row_number":    KRowNumber,
	"second":        KSecond,
	"select":        KSelect,
	"set":           KSet,
	"sign":          KSign,
	"snapshot":      KSnapshot,
	"some":          KSome,
	"stage":         KStage,
	"start":         KStart,
	"tstatistics":   KStatistics,
	"table":         KTable,
	"temp":          KTemp,
	"then":          KThen,
	"ties":          KTies,
	"top":           KTop,
	"transaction":   KTransaction,
	"trigger":       KTrigger,
	"true":          KTrue,
	"truncate":      KTruncate,
	"unbounded":     KUnbounded,
	"uncommitted":   KUncommitted,
	"union":         KUnion,
	"unique":        KUnique,
	"unlock":        KUnlock,
	"update":        KUpdate,
	"upper":         KUpper,
	"use":           KUse,
	"user":          KUser,
	"uuid":          KUuid,
	"value":         KValue,
	"values":        KValues,
	"week":          KWeek,
	"when":          KWhen,
	"where":         KWhere,
	"window":        KWindow,
	"with":          KWith,
	"year":          KYear,
}
