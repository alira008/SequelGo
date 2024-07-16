# T-SQL Interpreter

## Future Goals

### Formatter

- Extending to make formatter from the AST that gets generated from parsing T-SQL code
- This will work like opionated formatters like Prettier and
  [Poor Man's TSQL Formatter](https://github.com/TaoK/PoorMansTSqlFormatter)

### Example run 1

`SequelGo-formatter -k UpperCase "select hello from testtable where too"`

#### Output

```sql
[Error Line: 0 Col: 38]: expected expected expression after 'WHERE' keyword
select hello from testtable where too
                                  ^^^
```

### Example run 2

`SequelGo-formatter -k LowerCase "Select top 30 percent with ties LastPrice,
HighPrice , LowPrice, QuoteTime 'QuoTime' from MarketTable mkt where 'QuoTime' < '6:30' oRDer By Symbol
"`

#### Output

```sql
select top 30 percent with ties LastPrice
    ,HighPrice
    ,LowPrice
    ,QuoteTime 'QuoTime'
from MarketTable mkt
where 'QuoTime' < '6:30'
order by Symbol
```

### Example run 3

`SequelGo-formatter -k UpperCase "Select top 30 percent with ties LastPrice,
HighPrice , LowPrice, QuoteTime 'QuoTime' from MarketTable mkt where 'QuoTime' < '6:30' oRDer By Symbol
"`

#### Output

```sql
SELECT TOP 30 PERCENT WITH TIES LastPrice
    ,HighPrice
    ,LowPrice
    ,QuoteTime 'QuoTime'
FROM MarketTable mkt
WHERE 'QuoTime' < '6:30'
ORDER BY Symbol
```

### Help

```
SequelGo-format is an opionated formatter that formats T-SQL
code into a more readable format

Usage:
  SequelGo-format [flags] <sql to parse>

Flags:
  -h, --help                      help for SequelGo-format
  -b, --indentBetweenConditions   choose whether or not you want to indent between conditions.
  -c, --indentCommaLists string   choose whether or not you want to put a 'SpaceAfterComma', 'TrailingComma',
                                          or 'NoSpaceAfterComma'. (default "NoSpaceAfterComma")
  -l, --indentInLists             choose whether or not you want to indent in lists.
  -w, --indentWidth uint32        choose the width of indent (default 4)
  -k, --keywordCase string        choose whether or not you want to make keywords 'UpperCase' or 'LowerCase' (default "UpperCase")
  -m, --maxWidth uint32           choose the max width of a line (default 80)
  -u, --useTab                    choose whether or not you want to use tab instead of spaces.
```

### LSP

- Extending to make an LSP to get diagnostics in editors like Neovim

## TSQL Parser

Parses T-SQL queries and places them into abstract syntax to make it easier for other crates
make sense of the queries that are given to it

### Features In Progress

- [ ] Select Queries

  - [x] \[ ALL | DISTINCT ]
  - [x] TOP (expression) \[ PERCENT ] | \[ WITH TIES ]
  - [x] select items
    - [x] with subqueries
    - [x] with numbers
    - [x] with identifiers
    - [x] with aliases
    - [x] with aggregate functions
  - [ ] from clause
    - [x] basic table
    - [x] table with alias
    - [x] table valued function
    - [ ] pivot table
    - [ ] unpivot table
    - [x] joins
  - [x] where clause
    - [x] with subqueries
    - [x] with numbers
    - [x] with identifiers
    - [x] with aggregate functions
  - [x] group by clause
    - [x] with numbers
    - [x] with identifiers
    - [x] with aggregate functions
  - [x] having clause
    - [x] with subqueries
    - [x] with numbers
    - [x] with identifiers
    - [x] with aggregate functions
  - [x] order by clause
    - [x] with numbers
    - [x] with identifiers

- [x] CTEs
- [ ] Insert Queries
- [ ] Bulk Insert Queries
- [ ] Delete Queries
- [ ] Update Queries

## Credits

Special thanks to Thorsten Ball for writing the book
[Writing An Interpreter In Go](https://interpreterbook.com/). I used this to get a better idea
on how to start and execute my idea