package ast

import (
	"SequelGo/internal/lexer"
	"fmt"
	"strings"
)

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

var DataTypeTokenTypes = []lexer.TokenType{
	lexer.TInt, lexer.TBigint, lexer.TTinyint,
	lexer.TSmallint, lexer.TBit, lexer.TFloat,
	lexer.TReal, lexer.TDate, lexer.TDatetime,
	lexer.TTime, lexer.TDecimal, lexer.TNumeric,
	lexer.TVarchar,
}

type DataType struct {
	BaseNode
	Kind               DataTypeKind
	FloatPrecision     *uint32
	DecimalNumericSize *NumericSize
	VarcharLength      *uint32
}

type NumericSize struct {
	BaseNode
	Precision uint32
	Scale     *uint32
}

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
