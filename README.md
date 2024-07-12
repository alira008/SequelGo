# T-SQL Interpreter

## Future Goals

### Formatter

- Extending to make formatter from the AST that gets generated from parsing T-SQL code
- This will work like opionated formatters like Prettier and
  [Poor Man's TSQL Formatter](https://github.com/TaoK/PoorMansTSqlFormatter)

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
