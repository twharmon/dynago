package dynago

import (
	"reflect"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func tyVal(v interface{}) (reflect.Type, reflect.Value) {
	ty := reflect.TypeOf(v)
	for ty.Kind() == reflect.Pointer {
		ty = ty.Elem()
	}
	val := reflect.ValueOf(v)
	for val.Kind() == reflect.Pointer {
		val = val.Elem()
	}
	return ty, val
}

func trimDelims(s string) string {
	return s[1 : len(s)-1]
}

func (f *field) float32AttrVal(v float32) *dynamodb.AttributeValue {
	s := strconv.FormatFloat(float64(v), 'f', f.prec, 32)
	return f.floatAttrVal(s)
}

func (f *field) float64AttrVal(v float64) *dynamodb.AttributeValue {
	s := strconv.FormatFloat(v, 'f', f.prec, 64)
	return f.floatAttrVal(s)
}

func (f *field) floatAttrVal(s string) *dynamodb.AttributeValue {
	s = strings.TrimRight(s, "0")
	return &dynamodb.AttributeValue{N: &s}
}
