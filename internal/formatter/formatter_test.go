package formatter

import (
	"strings"
	"testing"

	"go.uber.org/zap"
)

func TestParseBasicSelectQuery(t *testing.T) {
	expected := "SELECT TOP 30 PERCENT WITH TIES\n    LastPrice\n    ,HighPrice\n    ,LowPrice\n    ,QuoteTime "
	expected += "'QuoTime'\n    ,*\nFROM MarketTable mkt\nWHERE QuoTime < '6:30'\n    AND lastPrice NOT "
	expected += "BETWEEN 2 AND 4\n    AND Symbol IN (\n        'aal'\n        ,'amzn'\n        ,'googl'"
	expected += "\n    )\n    AND InsertDate = CAST(GETDATE() AS DATE)\nORDER BY Symbol"

	input := "Select top 30 percent with ties LastPrice, HighPrice , LowPrice,"
	input += " QuoteTime 'QuoTime', * from MarketTable mkt where QuoTime < '6:30' and"
	input += " lastPrice not between 2 and 4 and Symbol in ('aal', 'amzn', 'googl') and"
	input += " InsertDate = cast(getdate() as date) oRDer By Symbol"

	test(t, expected, input)
}

func TestParseDetailedSelectQuery(t *testing.T) {
	expected := "SELECT TOP 30 PERCENT\n    LastPrice\n    ,[Time]\n    ,@Hello\n    ,PC AS "
	expected += "'PercentChange'\n    ,143245\nFROM MarketTable mkt\nINNER JOIN IndexTable it ON "
	expected += "mkt.[Time] = it.QuoteTime\nWHERE QuoteTime BETWEEN '6:30'"
	expected += " AND '13:00'\n    AND Symbol IN (\n        SELECT DISTINCT "
	expected += "Symbol\n        FROM MarketSymbols\n    )\n    AND"
	expected += " InsertTime = CAST(GETDATE() AS TIME)\nORDER BY Symbol DESC"

	input := "Select top 30 percent LastPrice, [Time] , @Hello, PC as 'PercentChange',"
	input += " 143245 from MarketTable mkt inner join IndexTable it on mkt.[Time] = it.QuoteTime where "
	input += "QuoteTime between '6:30' and '13:00' and Symbol in (select distinct Symbol from "
	input += "MarketSymbols) and InsertTime = cast(getdate() as Time) oRDer By Symbol deSC"

	test(t, expected, input)
}

func test(t *testing.T, expected string, input string) {
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()
	sugar := logger.Sugar()

	settings := Settings{
		IndentCommaLists:        ICLNoSpaceAfterComma,
		IndentInLists:           true,
		IndentBetweenConditions: true,
		KeywordCase:             KCUpper,
		MaxWidth:                80,
		IndentWidth:             4,
		UseTab:                  false,
	}
	fmter := NewFormatter(settings, sugar)
	formattedQuery, err := fmter.Format(input)
	if err != nil {
		t.Fatalf("Error: %s", err.Error())
	}
	formattedQueryLines := strings.Split(formattedQuery, "\n")
	expectedLines := strings.Split(expected, "\n")
	if len(formattedQueryLines) != len(expectedLines) {
		t.Fatalf("amount of lines differ:\nactual: %d lines: %s\n\nexpected: %d lines: %s",
			len(formattedQueryLines), formattedQuery, len(expectedLines), expected)
	}
	for i := 0; i < len(formattedQueryLines); i++ {
		expected := expectedLines[i]
		actual := formattedQueryLines[i]
		if expected != actual {
			t.Fatalf("actual: %s expected: %s.", actual, expected)
		}
	}
}
