# T-SQL Interpreter

## Future Goals

- Extending to make formatter from the AST that gets generated from parsing T-SQL code
- Extending to make an LSP to get diagnostics in editors like Neovim

## SQL_PARSER

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
