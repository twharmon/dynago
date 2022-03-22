package dynago

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

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

func (d *Dynago) simpleUnmarshal(v reflect.Value, av *dynamodb.AttributeValue, layout string) error {
	if av == nil {
		return nil
	}
	for v.Kind() == reflect.Pointer {
		if v.IsNil() {
			v.Set(reflect.New(v.Type().Elem()))
		}
		v = v.Elem()
	}
	switch v.Kind() {
	case reflect.Slice:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			if av.B != nil {
				v.Set(reflect.ValueOf(av.B))
			}
		} else {
			sl := reflect.MakeSlice(v.Type(), len(av.L), len(av.L))
			for i := 0; i < len(av.L); i++ {
				if err := d.simpleUnmarshal(sl.Index(i), av.L[i], layout); err != nil {
					return err
				}
			}
			v.Set(sl)
		}
	case reflect.Struct:
		if v.Type() == timeType {
			ti, err := time.Parse(layout, *av.S)
			if err != nil {
				return err
			}
			v.Set(reflect.ValueOf(ti))
		} else {
			if err := d.Unmarshal(av.M, v.Addr().Interface()); err != nil {
				return err
			}
		}
	case reflect.String:
		if av.S != nil {
			v.Set(reflect.ValueOf(*av.S))
		}
	case reflect.Int64:
		if av.N != nil {
			i, err := strconv.ParseInt(*av.N, 10, 64)
			if err != nil {
				return err
			}
			v.Set(reflect.ValueOf(i))
		}
	case reflect.Float64:
		if av.N != nil {
			i, err := strconv.ParseFloat(*av.N, 64)
			if err != nil {
				return err
			}
			v.Set(reflect.ValueOf(i))
		}
	case reflect.Float32:
		if av.N != nil {
			i, err := strconv.ParseFloat(*av.N, 32)
			if err != nil {
				return err
			}
			v.Set(reflect.ValueOf(float32(i)))
		}
	case reflect.Bool:
		if av.BOOL != nil {
			v.Set(reflect.ValueOf(*av.BOOL))
		}
	default:
		return fmt.Errorf("type %s not supported", v.Kind())
	}
	return nil
}

func (d *Dynago) simpleMarshal(v reflect.Value, layout string) (*dynamodb.AttributeValue, error) {
	for v.Kind() == reflect.Pointer {
		v = v.Elem()
	}
	switch v.Kind() {
	case reflect.Slice:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			val := v.Interface().([]byte)
			return &dynamodb.AttributeValue{B: val}, nil
		}
		av := &dynamodb.AttributeValue{}
		for i := 0; i < v.Len(); i++ {
			item, err := d.simpleMarshal(v.Index(i), layout)
			if err != nil {
				return nil, fmt.Errorf("d.simpleMarshal: %w", err)
			}
			av.L = append(av.L, item)
		}
		return av, nil
	case reflect.Struct:
		if v.Type() == timeType {
			s := v.Interface().(time.Time).Format(layout)
			return &dynamodb.AttributeValue{S: &s}, nil
		}
		item, err := d.Marshal(v.Interface())
		if err != nil {
			return nil, fmt.Errorf("d.Marshal: %w", err)
		}
		return &dynamodb.AttributeValue{M: item}, nil
	case reflect.String:
		val := v.Interface().(string)
		return &dynamodb.AttributeValue{S: &val}, nil
	case reflect.Int64:
		val := v.Interface().(int64)
		s := strconv.FormatInt(val, 10)
		return &dynamodb.AttributeValue{N: &s}, nil
	case reflect.Float64:
		val := v.Interface().(float64)
		s := strconv.FormatFloat(val, 'f', 14, 64)
		s = strings.TrimRight(s, "0")
		return &dynamodb.AttributeValue{N: &s}, nil
	case reflect.Float32:
		val := v.Interface().(float32)
		s := strconv.FormatFloat(float64(val), 'f', 14, 32)
		s = strings.TrimRight(s, "0")
		return &dynamodb.AttributeValue{N: &s}, nil
	case reflect.Bool:
		val := v.Interface().(bool)
		return &dynamodb.AttributeValue{BOOL: &val}, nil
	default:
		return nil, fmt.Errorf("type %s not supported", v.Kind())
	}
}
