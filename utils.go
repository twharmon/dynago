package dynago

import "reflect"

func tyVal(v interface{}) (reflect.Type, reflect.Value) {
	ty := reflect.TypeOf(v)
	for ty.Kind() == reflect.Ptr {
		ty = ty.Elem()
	}
	val := reflect.ValueOf(v)
	for val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	return ty, val
}
