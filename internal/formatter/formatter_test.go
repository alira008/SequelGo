package formatter

import (
	"strings"
	"testing"

	"go.uber.org/zap"
)

func TestParseBasicSelectQuery(t *testing.T) {
	expected := `SELECT TOP 30 PERCENT WITH TIES
    LastPrice
    ,HighPrice
    ,LowPrice
    ,QuoteTime 'QuoTime'
    ,*
FROM MarketTable mkt
WHERE QuoTime < '6:30'
    AND lastPrice NOT BETWEEN 2
            AND 4
    AND Symbol IN (
        'aal'
        ,'amzn'
        ,'googl'
    )
    AND InsertDate = CAST(GETDATE() AS DATE)
ORDER BY Symbol`

	input := "Select top 30 percent with ties LastPrice, HighPrice , LowPrice,"
	input += " QuoteTime 'QuoTime', * from MarketTable mkt where QuoTime < '6:30' and"
	input += " lastPrice not between 2 and 4 and Symbol in ('aal', 'amzn', 'googl') and"
	input += " InsertDate = cast(getdate() as date) oRDer By Symbol"

	test(t, expected, input)
}

func TestParseDetailedSelectQuery(t *testing.T) {
	expected := `SELECT TOP 30 PERCENT
    LastPrice
    ,[Time]
    ,CAST('47' AS FLOAT)
    ,@Hello
    ,PC AS 'PercentChange'
    ,143245
FROM MarketTable mkt
INNER JOIN IndexTable it ON mkt.[Time] = it.QuoteTime
WHERE QuoteTime BETWEEN '6:30'
        AND '13:00'
    AND Symbol IN (
        SELECT DISTINCT Symbol
        FROM MarketSymbols
    )
    AND InsertTime = CAST(GETDATE() AS TIME)
ORDER BY Symbol DESC`

	input := "Select top 30 percent LastPrice, [Time] , cast('47' as float), @Hello, PC as 'PercentChange',"
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
