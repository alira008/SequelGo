# SequelGo

T-SQL tools built upon interpreter built in Go

## Formatter

>[!WARNING]
>
> This is still a work in progress and I am still extending the formatter so it generates
> SQL code correctly. Need to figure out how to preserve comments. Currently line comments get
> deleted. Block comments are not supported yet

This formatter works like opinionated formatters like Prettier and
  [Poor Man's TSQL Formatter](https://github.com/TaoK/PoorMansTSqlFormatter)

### Example run 1

```bash
SequelGo format -k UpperCase "select hello from testtable where too"
```

#### Output

```sql
[Error Line: 0 Col: 38]: expected expected expression after 'WHERE' keyword
select hello from testtable where too
                                  ^^^
```

### Example run 2

```bash
SequelGo format -k LowerCase "Select top 30 percent with ties LastPrice, HighPrice , LowPrice, QuoteTime 'QuoTime' from MarketTable mkt where 'QuoTime' < '6:30' oRDer By Symbol"
```

#### Output

```sql
select top 30 percent with ties LastPrice
    ,HighPrice
    ,LowPrice
    ,QuoteTime 'QuoTime'
from MarketTable mkt
where QuoTime < '6:30'
order by Symbol
```

### Example run 3

```bash 
SequelGo format -k UpperCase "Select top 30 percent with ties LastPrice, HighPrice , LowPrice, QuoteTime 'QuoTime' from MarketTable mkt where 'QuoTime' < '6:30' oRDer By Symbol"
```

#### Output

```sql
SELECT TOP 30 PERCENT WITH TIES LastPrice
    ,HighPrice
    ,LowPrice
    ,QuoteTime 'QuoTime'
FROM MarketTable mkt
WHERE QuoTime < '6:30'
ORDER BY Symbol
```

### Help

```
SequelGo is a tool that has a command for an opionated formatter that formats T-SQL
    code into a more readable format. It also provides a command to start a language server
    that will make it easier to develop T-SQL queries

Usage:
  SequelGo [command]

Available Commands:
  format      Format T-SQL code
  help        Help about any command

Flags:
  -h, --help   help for SequelGo
```
```
SequelGo format is an opionated formatter that formats T-SQL
code into a more readable format

Usage:
  SequelGo format [flags] <sql to parse>

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

## Future Goals

### LSP

- Extending to make an LSP to get diagnostics in editors like Neovim

## Credits

Special thanks to Thorsten Ball for writing the book
[Writing An Interpreter In Go](https://interpreterbook.com/). I used this to get a better idea
on how to start and execute my idea
