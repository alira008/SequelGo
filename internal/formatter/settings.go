package formatter

type IndentCommaLists uint8

const (
	ICLNoSpaceAfterComma IndentCommaLists = iota
	ICLSpaceAfterComma
	ICLTrailingComma
)

type KeywordCase uint8

const (
	KCUpper KeywordCase = iota
	KCLower
)

type Settings struct {
    IndentCommaLists IndentCommaLists
    IndentInLists bool
    IndentBetweenConditions bool
    KeywordCase KeywordCase
    MaxWidth uint32
    IndentWidth uint32
    UseTab bool
}
