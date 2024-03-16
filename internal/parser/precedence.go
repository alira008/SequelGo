package parser

import "SequelGo/internal/lexer"

type Precedence uint8

const (
	PrecedenceLowest Precedence = iota
	PrecedenceAssignment
	PrecedenceOtherLogicals
	PrecedenceAnd
	PrecedenceNot
	PrecedenceComparison
	PrecedenceSum
	PrecedenceProduct
	PrecedenceHighest
)

var PrecedenceMap = map[lexer.TokenType]Precedence{
	lexer.TTilde: PrecedenceHighest,
    lexer.TAsterisk: PrecedenceProduct,
    lexer.TDivide: PrecedenceProduct,
    lexer.TPlus: PrecedenceSum,
    lexer.TMinus: PrecedenceSum,
    lexer.TEqual: PrecedenceComparison,
    lexer.TNotEqual: PrecedenceComparison,
    lexer.TLessThan: PrecedenceComparison,
    lexer.TLessThanEqual: PrecedenceComparison,
    lexer.TGreaterThan: PrecedenceComparison,
    lexer.TGreaterThanEqual: PrecedenceComparison,
    lexer.TNot: PrecedenceNot,
    lexer.TAnd: PrecedenceAnd,
    lexer.TAll: PrecedenceOtherLogicals,
    lexer.TAny: PrecedenceOtherLogicals,
    lexer.TBetween: PrecedenceOtherLogicals,
    lexer.TIn: PrecedenceOtherLogicals,
    lexer.TLike: PrecedenceOtherLogicals,
    lexer.TOr: PrecedenceOtherLogicals,
    lexer.TSome: PrecedenceOtherLogicals,
    lexer.TPlusEqual: PrecedenceOtherLogicals,
    lexer.TMinusEqual: PrecedenceOtherLogicals,
    lexer.TMultiplyEqual: PrecedenceOtherLogicals,
    lexer.TDivideEqual: PrecedenceOtherLogicals,
    lexer.TPercentEqual: PrecedenceOtherLogicals,
    lexer.TAndEqual: PrecedenceOtherLogicals,
    lexer.TOrEqual: PrecedenceOtherLogicals,
    lexer.TCaretEqual: PrecedenceOtherLogicals,
}

func checkPrecedence(t lexer.TokenType) Precedence {
    if p, ok := PrecedenceMap[t]; ok {
        return p
    }
    return PrecedenceLowest
}
