package ast

import (
	"SequelGo/internal/lexer"
	"fmt"
	"strings"
)

var DataTypeTokenTypes = []lexer.TokenType{
	lexer.TInt, lexer.TBigint, lexer.TTinyint,
	lexer.TSmallint, lexer.TBit, lexer.TFloat,
	lexer.TReal, lexer.TDate, lexer.TDatetime,
	lexer.TTime, lexer.TDecimal, lexer.TNumeric,
	lexer.TVarchar,
}

type DataType struct {
	Span
	LeadingComments    *[]Comment
	TrailingComments   *[]Comment
	Kind               DataTypeKind
	FloatPrecision     *uint32
	DecimalNumericSize *NumericSize
	VarcharLength      *uint32
}

type NumericSize struct {
	Span
	LeadingComments  *[]Comment
	TrailingComments *[]Comment
	Precision        uint32
	Scale            *uint32
}

func (dt DataType) expressionNode()    {}
func (ns NumericSize) expressionNode() {}

func (ns NumericSize) TokenLiteral() string {
	var str strings.Builder

	str.WriteString(fmt.Sprintf("%d", ns.Precision))
	if ns.Scale != nil {
		str.WriteString(fmt.Sprintf(", %d", *ns.Scale))
	}

	return str.String()

}
func (dt DataType) TokenLiteral() string {
	var str strings.Builder

	switch dt.Kind {
	case DTInt:
		str.WriteString("INT")
		break
	case DTBigInt:
		str.WriteString("BIGINT")
		break
	case DTTinyInt:
		str.WriteString("TINYINT")
		break
	case DTSmallInt:
		str.WriteString("SMALLINT")
		break
	case DTBit:
		str.WriteString("BIT")
		break
	case DTFloat:
		str.WriteString("FLOAT")
		if dt.FloatPrecision != nil {
			str.WriteString(fmt.Sprintf("(%d)", *dt.FloatPrecision))
		}
		break
	case DTReal:
		str.WriteString("REAL")
		break
	case DTDate:
		str.WriteString("DATE")
		break
	case DTDatetime:
		str.WriteString("DATETIME")
		break
	case DTTime:
		str.WriteString("TIME")
		break
	case DTDecimal:
		str.WriteString("DECIMAL")
		if dt.DecimalNumericSize != nil {
			str.WriteString(fmt.Sprintf("(%s)", dt.DecimalNumericSize.TokenLiteral()))
		}
		break
	case DTNumeric:
		str.WriteString("NUMERIC")
		if dt.DecimalNumericSize != nil {
			str.WriteString(fmt.Sprintf("(%s)", dt.DecimalNumericSize.TokenLiteral()))
		}
		break
	case DTVarchar:
		str.WriteString("VARCHAR")
		if dt.VarcharLength != nil {
			str.WriteString(fmt.Sprintf("(%d)", *dt.VarcharLength))
		}
		break
	}
	return str.String()
}

func (dt DataType) GetSpan() Span    { return dt.Span }
func (ns NumericSize) GetSpan() Span { return ns.Span }

func (dt *DataType) SetSpan(span Span)    { dt.Span = span }
func (ns *NumericSize) SetSpan(span Span) { ns.Span = span }

func (dt *DataType) SetLeadingComments(comments []Comment)    { dt.LeadingComments = &comments }
func (ns *NumericSize) SetLeadingComments(comments []Comment) { ns.LeadingComments = &comments }

func (dt *DataType) SetTrailingComments(comments []Comment)    { dt.TrailingComments = &comments }
func (ns *NumericSize) SetTrailingComments(comments []Comment) { ns.TrailingComments = &comments }

func (dt *DataType) GetLeadingComments() *[]Comment    { return dt.LeadingComments }
func (ns *NumericSize) GetLeadingComments() *[]Comment { return ns.LeadingComments }

func (dt *DataType) GetTrailingComments() *[]Comment    { return dt.TrailingComments }
func (ns *NumericSize) GetTrailingComments() *[]Comment { return ns.TrailingComments }

type DataTypeKind uint8

const (
	DTInt DataTypeKind = iota
	DTBigInt
	DTTinyInt
	DTSmallInt
	DTBit
	DTFloat
	DTReal
	DTDate
	DTDatetime
	DTTime
	DTDecimal
	DTNumeric
	DTVarchar
)
