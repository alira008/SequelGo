package lexer

import (
	"fmt"
	"strings"
)

type Lexer struct {
	input   string
	read    int
	current int
	ch      byte
	line    int
	col     int
}

func NewPosition(line, col int) Position {
	return Position{Line: line, Col: col}
}

func (p *Position) String() string {
	return fmt.Sprintf("{Line: %d Col: %d}", p.Line, p.Col)
}

func NewLexer(input string) *Lexer {
	lexer := &Lexer{input: input}
	lexer.readChar()
	return lexer
}

func (l Lexer) CurrentLine() string {
	lines := strings.Split(l.input, "\n")
	var currentLine string
	for i, line := range lines {
		if i == l.line {
			currentLine = line
		}
	}

	return currentLine
}

func (l *Lexer) NextToken() Token {
	l.skipWhitespace()
	token := Token{}
	token.Start.Col = l.col
	token.Start.Line = l.line
	switch l.ch {
	case ',':
		token.Type = TComma
		token.Value = ","
	case '(':
		token.Type = TLeftParen
		token.Value = "("
	case ')':
		token.Type = TRightParen
		token.Value = ")"
	case '=':
		if l.peekChar() == '=' {
			l.readChar()
			token.Type = TDoubleEqual
			token.Value = "=="
		} else {
			token.Type = TEqual
			token.Value = "="
		}
	case '!':
		if l.peekChar() == '=' {
			l.readChar()
			token.Type = TNotEqualBang
			token.Value = "!="
		} else {
			token.Type = TExclamationMark
			token.Value = "!"
		}
	case '<':
		if l.peekChar() == '=' {
			l.readChar()
			token.Type = TLessThanEqual
			token.Value = "<="
		} else if l.peekChar() == '>' {
			l.readChar()
			token.Type = TNotEqualArrow
			token.Value = "<>"
		} else {
			token.Type = TLessThan
			token.Value = "<"
		}
	case '>':
		if l.peekChar() == '=' {
			l.readChar()
			token.Type = TGreaterThanEqual
			token.Value = ">="
		} else {
			token.Type = TGreaterThan
			token.Value = ">"
		}
	case '+':
		if l.peekChar() == '=' {
			l.readChar()
			token.Type = TPlusEqual
			token.Value = "+="
		} else {
			token.Type = TPlus
			token.Value = "+"
		}
	case '-':
		if l.peekChar() == '=' {
			l.readChar()
			token.Type = TMinusEqual
			token.Value = "-="
		} else if l.peekChar() == '-' {
            comment:=l.readCommentLine()
            token.Type = TCommentLine
            token.Value = comment
		} else {
			token.Type = TMinus
			token.Value = "-"
		}
	case '/':
		if l.peekChar() == '=' {
			l.readChar()
			token.Type = TDivideEqual
			token.Value = "/="
		} else {
			token.Type = TDivide
			token.Value = "/"
		}
	case '*':
		if l.peekChar() == '=' {
			l.readChar()
			token.Type = TMultiplyEqual
			token.Value = "*="
		} else {
			token.Type = TAsterisk
			token.Value = "*"
		}
	case '%':
		if l.peekChar() == '=' {
			l.readChar()
			token.Type = TPercentEqual
			token.Value = "%="
		} else {
			token.Type = TPercent
			token.Value = "*"
		}
	case '^':
		if l.peekChar() == '=' {
			l.readChar()
			token.Type = TCaretEqual
			token.Value = "^="
		} else {
			token.Type = TSyntaxError
			token.Value = "^"
		}
	case '|':
		if l.peekChar() == '=' {
			l.readChar()
			token.Type = TOrEqual
			token.Value = "|="
		} else {
			token.Type = TOr
			token.Value = "|"
		}
	case '&':
		if l.peekChar() == '=' {
			l.readChar()
			token.Type = TAndEqual
			token.Value = "&="
		} else {
			token.Type = TAnd
			token.Value = "&"
		}
	case '.':
		token.Type = TPeriod
		token.Value = "."
	case ';':
		token.Type = TSemiColon
		token.Value = ";"
	case '[':
		peekChar := l.peekChar()
		if l.isAlphaNumeric(peekChar) {
			// Read identifier until ']'
			quotedIdentifier := l.readQuotedIdentifier()
			// if the last character is not ']', then it's a syntax error
			if l.ch == 0 {
				token.Type = TSyntaxError
				token.Value = quotedIdentifier
			} else {
				token.Type = TQuotedIdentifier
				token.Value = quotedIdentifier
			}
		} else {
			token.Type = TLeftBracket
			token.Value = "["
		}
	case ']':
		token.Type = TRightBracket
		token.Value = "]"
	case '\'':
		peekChar := l.peekChar()
		if l.isAlphaNumeric(peekChar) {
			// Read identifier until '\''
			stringLiteral := l.readQuotedString()
			// if the last character is not '\'', then it's a syntax error
			if l.ch == 0 {
				token.Type = TSyntaxError
				token.Value = stringLiteral
			} else {
				token.Type = TStringLiteral
				token.Value = stringLiteral
			}
		} else {
			token.Type = TSyntaxError
			token.Value = "'"
		}
	case '{':
		token.Type = TLeftBrace
		token.Value = "{"
	case '}':
		token.Type = TRightBrace
		token.Value = "}"
	case '~':
		token.Type = TTilde
		token.Value = "~"
	case '@':
		// skip the @ character
		l.readChar()
		localVariable := l.readIdentifier()
		token.Type = TLocalVariable
		token.Value = localVariable
	case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
		number := l.readNumber()
		token.Type = TNumericLiteral
		token.Value = number
	case 0:
		token.Type = TEndOfFile
		token.Value = ""
	default:
		if l.isLetter(l.ch) || l.ch == '_' {
			identifier := l.readIdentifier()
			lowerIdentifier := strings.ToLower(identifier)
			keyword, ok := Keywords[lowerIdentifier]
			if ok {
				token.Type = keyword
				token.Value = identifier
			} else {

				token.Type = TIdentifier
				token.Value = identifier
			}
		} else {
			token.Type = TSyntaxError
			token.Value = string(l.ch)
		}
	}

	token.End.Col = l.col
	token.End.Line = l.line

	l.readChar()

	return token
}

func (t *Token) String() string {
	return fmt.Sprintf("{Value: %s, Start line: %d, Start col: %d,  End line: %d, End col: %d}", strings.ToLower(t.Value), t.Start.Line, t.Start.Col, t.End.Line, t.End.Col)
}

func (l *Lexer) readChar() {
	if l.read >= len(l.input) {
		l.ch = 0
	} else {
		l.ch = l.input[l.read]
	}

	if l.ch == '\n' {
		l.line++
		l.col = 0
	} else if l.ch == '\t' {
		l.col += 4
	} else {
		l.col++
	}

	l.current = l.read
	l.read += 1
}

func (l *Lexer) peekChar() byte {
	if l.read >= len(l.input) {
		return 0
	}
	return l.input[l.read]
}

func (l *Lexer) skipWhitespace() {
	for l.ch == ' ' || l.ch == '\t' || l.ch == '\n' || l.ch == '\r' {
		l.readChar()
	}
}

func (l *Lexer) isLetter(ch byte) bool {
	return 'a' <= ch && ch <= 'z' || 'A' <= ch && ch <= 'Z' || ch == '_'
}

func (l *Lexer) isDigit(ch byte) bool {
	return '0' <= ch && ch <= '9'
}

func (l *Lexer) isAlphaNumeric(ch byte) bool {
	return l.isLetter(ch) || l.isDigit(ch)
}

func (l *Lexer) readCommentLine() string {
	// skip the -- characters
	l.readChar()
	l.readChar()

	// read the identifier until next quote
	start := l.current

	for {
		peekChar := l.peekChar()
		if peekChar == '\n' || peekChar == 0 {
			break
		}
		l.readChar()
	}

	return strings.TrimSpace(l.input[start:l.current+1])
}

func (l *Lexer) readQuotedIdentifier() string {
	// skip the quote character
	l.readChar()

	// read the identifier until next quote
	start := l.current

	for {
		peekChar := l.peekChar()
		if peekChar == ']' || peekChar == 0 {
			break
		}
		l.readChar()
	}

	// go to the quote character
	l.readChar()

	return l.input[start:l.current]
}

func (l *Lexer) readQuotedString() string {
	// skip the quote character
	l.readChar()

	// read the identifier until next quote
	start := l.current

	for {
		peekChar := l.peekChar()
		if peekChar == '\'' || peekChar == 0 {
			break
		}
		l.readChar()
	}

	// go to the quote character
	l.readChar()

	return l.input[start:l.current]
}

func (l *Lexer) readNumber() string {
	start := l.current
	for l.isDigit(l.peekChar()) {
		l.readChar()
	}

	// check for floating point
	if l.peekChar() == '.' {
		l.readChar()

		for l.isDigit(l.peekChar()) {
			l.readChar()
		}
	}

	if l.current+1 >= len(l.input) {
		return l.input[start:]
	}
	return l.input[start : l.current+1]
}

func (l *Lexer) readIdentifier() string {
	start := l.current
	peekChar := l.peekChar()
	for l.isAlphaNumeric(peekChar) || peekChar == '_' {
		l.readChar()
		peekChar = l.peekChar()
	}

	if l.current+1 >= len(l.input) {
		return l.input[start:]
	}
	return l.input[start : l.current+1]
}
