package ast

import (
	"SequelGo/internal/lexer"
	"strings"
)

type Keyword struct {
	Span
	Type KeywordType
}

func NewKeywordFromToken(token lexer.Token) Keyword {
	keywordType, ok := Keywords[strings.ToLower(token.Value)]
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
func (k Keyword) TokenLiteral() string      { return k.Type.String() }

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

func (k KeywordType) String() string {
	switch k {
	case KAll:
		return "All"
	case KAlter:
		return "Alter"
	case KAnd:
		return "And"
	case KAny:
		return "Any"
	case KAs:
		return "As"
	case KAsc:
		return "Asc"
	case KAutoincrement:
		return "Autoincrement"
	case KBegin:
		return "Begin"
	case KBetween:
		return "Between"
	case KBy:
		return "By"
	case KCascade:
		return "Cascade"
	case KCase:
		return "Case"
	case KChar:
		return "Char"
	case KColumn:
		return "Column"
	case KColumns:
		return "Columns"
	case KCommit:
		return "Commit"
	case KCommited:
		return "Commited"
	case KConstraint:
		return "Constraint"
	case KCreate:
		return "Create"
	case KCurrent:
		return "Current"
	case KDay:
		return "Day"
	case KDayofweek:
		return "Dayofweek"
	case KDayofyear:
		return "Dayofyear"
	case KDeclare:
		return "Declare"
	case KDegrees:
		return "Degrees"
	case KDefault:
		return "Default"
	case KDelete:
		return "Delete"
	case KDesc:
		return "Desc"
	case KDescribe:
		return "Describe"
	case KDistinct:
		return "Distinct"
	case KDo:
		return "Do"
	case KDrop:
		return "Drop"
	case KElse:
		return "Else"
	case KEnd:
		return "End"
	case KEngine:
		return "Engine"
	case KExec:
		return "Exec"
	case KExecute:
		return "Execute"
	case KExists:
		return "Exists"
	case KFalse:
		return "False"
	case KFetch:
		return "Fetch"
	case KFirst:
		return "First"
	case KFloat:
		return "Float"
	case KFloor:
		return "Floor"
	case KFollowing:
		return "Following"
	case KForeign:
		return "Foreign"
	case KFrom:
		return "From"
	case KFull:
		return "Full"
	case KFunction:
		return "Function"
	case KGroup:
		return "Group"
	case KHaving:
		return "Having"
	case KHour:
		return "Hour"
	case KHours:
		return "Hours"
	case KIdentity:
		return "Identity"
	case KIf:
		return "If"
	case KIn:
		return "In"
	case KIncrement:
		return "Increment"
	case KIndex:
		return "Index"
	case KInner:
		return "Inner"
	case KInsert:
		return "Insert"
	case KInteger:
		return "Integer"
	case KIntersect:
		return "Intersect"
	case KInt:
		return "Int"
	case KInto:
		return "Into"
	case KIs:
		return "Is"
	case KJoin:
		return "Join"
	case KKey:
		return "Key"
	case KLast:
		return "Last"
	case KLead:
		return "Lead"
	case KLeft:
		return "Left"
	case KLike:
		return "Like"
	case KLimit:
		return "Limit"
	case KMicrosecond:
		return "Microsecond"
	case KMicroseconds:
		return "Microseconds"
	case KMillisecond:
		return "Millisecond"
	case KMilliseconds:
		return "Milliseconds"
	case KMin:
		return "Min"
	case KMinute:
		return "Minute"
	case KMonth:
		return "Month"
	case KNanosecond:
		return "Nanosecond"
	case KNanoseconds:
		return "Nanoseconds"
	case KNchar:
		return "Nchar"
	case KNext:
		return "Next"
	case KNot:
		return "Not"
	case KOffset:
		return "Offset"
	case KOn:
		return "On"
	case KOnly:
		return "Only"
	case KOr:
		return "Or"
	case KOrder:
		return "Order"
	case KOuter:
		return "Outer"
	case KOver:
		return "Over"
	case KPartition:
		return "Partition"
	case KPassword:
		return "Password"
	case KPercent:
		return "Percent"
	case KPi:
		return "Pi"
	case KPower:
		return "Power"
	case KPreceding:
		return "Preceding"
	case KProcedure:
		return "Procedure"
	case KRadians:
		return "Radians"
	case KRands:
		return "Rands"
	case KReturn:
		return "Return"
	case KReturns:
		return "Returns"
	case KRevoke:
		return "Revoke"
	case KRight:
		return "Right"
	case KRole:
		return "Role"
	case KRollback:
		return "Rollback"
	case KRound:
		return "Round"
	case KRow:
		return "Row"
	case KRowId:
		return "Rowid"
	case KRows:
		return "Rows"
	case KRowNumber:
		return "RowNumber"
	case KSecond:
		return "Second"
	case KSelect:
		return "Select"
	case KSet:
		return "Set"
	case KSign:
		return "Sign"
	case KSnapshot:
		return "Snapshot"
	case KSome:
		return "Some"
	case KStage:
		return "Stage"
	case KStart:
		return "Start"
	case KStatistics:
		return "Statistics"
	case KTable:
		return "Table"
	case KTemp:
		return "Temp"
	case KThen:
		return "Then"
	case KTies:
		return "Ties"
	case KTop:
		return "Top"
	case KTransaction:
		return "Transaction"
	case KTrigger:
		return "Trigger"
	case KTrue:
		return "True"
	case KTruncate:
		return "Truncate"
	case KUnbounded:
		return "Unbounded"
	case KUncommitted:
		return "Uncommitted"
	case KUnion:
		return "Union"
	case KUnique:
		return "Unique"
	case KUnlock:
		return "Unlock"
	case KUpdate:
		return "Update"
	case KUpper:
		return "Upper"
	case KUse:
		return "Use"
	case KUser:
		return "User"
	case KUuid:
		return "Uuid"
	case KValue:
		return "Value"
	case KValues:
		return "Values"
	case KWeek:
		return "Week"
	case KWhen:
		return "When"
	case KWhere:
		return "Where"
	case KWindow:
		return "Window"
	case KWith:
		return "With"
	case KYear:
		return "Year"
	}
	return "Unimplemented"
}
