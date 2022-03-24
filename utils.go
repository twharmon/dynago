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
	case reflect.Int:
		if av.N != nil {
			i, err := strconv.ParseInt(*av.N, 10, 64)
			if err != nil {
				return err
			}
			v.Set(reflect.ValueOf(int(i)))
		}
	case reflect.Uint:
		if av.N != nil {
			i, err := strconv.ParseInt(*av.N, 10, 64)
			if err != nil {
				return err
			}
			v.Set(reflect.ValueOf(uint(i)))
		}
	case reflect.Int8:
		if av.N != nil {
			i, err := strconv.ParseInt(*av.N, 10, 64)
			if err != nil {
				return err
			}
			v.Set(reflect.ValueOf(int8(i)))
		}
	case reflect.Int16:
		if av.N != nil {
			i, err := strconv.ParseInt(*av.N, 10, 64)
			if err != nil {
				return err
			}
			v.Set(reflect.ValueOf(int16(i)))
		}
	case reflect.Int32:
		if av.N != nil {
			i, err := strconv.ParseInt(*av.N, 10, 64)
			if err != nil {
				return err
			}
			v.Set(reflect.ValueOf(int32(i)))
		}
	case reflect.Int64:
		if av.N != nil {
			i, err := strconv.ParseInt(*av.N, 10, 64)
			if err != nil {
				return err
			}
			v.Set(reflect.ValueOf(i))
		}
	case reflect.Uint8:
		if av.N != nil {
			i, err := strconv.ParseInt(*av.N, 10, 64)
			if err != nil {
				return err
			}
			v.Set(reflect.ValueOf(uint8(i)))
		}
	case reflect.Uint16:
		if av.N != nil {
			i, err := strconv.ParseInt(*av.N, 10, 64)
			if err != nil {
				return err
			}
			v.Set(reflect.ValueOf(uint16(i)))
		}
	case reflect.Uint32:
		if av.N != nil {
			i, err := strconv.ParseInt(*av.N, 10, 64)
			if err != nil {
				return err
			}
			v.Set(reflect.ValueOf(uint32(i)))
		}
	case reflect.Uint64:
		if av.N != nil {
			i, err := strconv.ParseInt(*av.N, 10, 64)
			if err != nil {
				return err
			}
			v.Set(reflect.ValueOf(uint64(i)))
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
		av := &dynamodb.AttributeValue{L: []*dynamodb.AttributeValue{}}
		for i := 0; i < v.Len(); i++ {
			item, err := d.simpleMarshal(v.Index(i), layout)
			if err != nil {
				return nil, fmt.Errorf("d.simpleMarshal: %w", err)
			}
			if item != nil {
				av.L = append(av.L, item)
			}
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
	case reflect.Int:
		val := v.Interface().(int)
		s := strconv.FormatInt(int64(val), 10)
		return &dynamodb.AttributeValue{N: &s}, nil
	case reflect.Uint:
		val := v.Interface().(uint)
		s := strconv.FormatInt(int64(val), 10)
		return &dynamodb.AttributeValue{N: &s}, nil
	case reflect.Int8:
		val := v.Interface().(int8)
		s := strconv.FormatInt(int64(val), 10)
		return &dynamodb.AttributeValue{N: &s}, nil
	case reflect.Int16:
		val := v.Interface().(int16)
		s := strconv.FormatInt(int64(val), 10)
		return &dynamodb.AttributeValue{N: &s}, nil
	case reflect.Int32:
		val := v.Interface().(int32)
		s := strconv.FormatInt(int64(val), 10)
		return &dynamodb.AttributeValue{N: &s}, nil
	case reflect.Int64:
		val := v.Interface().(int64)
		s := strconv.FormatInt(val, 10)
		return &dynamodb.AttributeValue{N: &s}, nil
	case reflect.Uint8:
		val := v.Interface().(uint8)
		s := strconv.FormatInt(int64(val), 10)
		return &dynamodb.AttributeValue{N: &s}, nil
	case reflect.Uint16:
		val := v.Interface().(uint16)
		s := strconv.FormatInt(int64(val), 10)
		return &dynamodb.AttributeValue{N: &s}, nil
	case reflect.Uint32:
		val := v.Interface().(uint32)
		s := strconv.FormatInt(int64(val), 10)
		return &dynamodb.AttributeValue{N: &s}, nil
	case reflect.Uint64:
		val := v.Interface().(uint64)
		s := strconv.FormatInt(int64(val), 10)
		return &dynamodb.AttributeValue{N: &s}, nil
	case reflect.Float64:
		val := v.Interface().(float64)
		s := formatFloat(val)
		return &dynamodb.AttributeValue{N: &s}, nil
	case reflect.Float32:
		val := v.Interface().(float32)
		s := formatFloat(val)
		return &dynamodb.AttributeValue{N: &s}, nil
	case reflect.Bool:
		val := v.Interface().(bool)
		return &dynamodb.AttributeValue{BOOL: &val}, nil
	default:
		return nil, nil
	}
}

type Float interface {
	float32 | float64
}

func formatFloat[T Float](f T) string {
	var s string
	switch val := interface{}(f).(type) {
	case float32:
		s = strconv.FormatFloat(float64(val), 'f', -1, 32)
	case float64:
		s = strconv.FormatFloat(val, 'f', -1, 64)
	}
	s = strings.TrimRight(s, "0")
	s = strings.TrimRight(s, ".")
	return s
}

func expressionAttributeValue(m map[string]*dynamodb.AttributeValue, key string, val interface{}) {
	switch v := val.(type) {
	case string:
		m[key] = &dynamodb.AttributeValue{S: &v}
	case int:
		s := strconv.FormatInt(int64(v), 10)
		m[key] = &dynamodb.AttributeValue{N: &s}
	case uint:
		s := strconv.FormatInt(int64(v), 10)
		m[key] = &dynamodb.AttributeValue{N: &s}
	case int8:
		s := strconv.FormatInt(int64(v), 10)
		m[key] = &dynamodb.AttributeValue{N: &s}
	case int16:
		s := strconv.FormatInt(int64(v), 10)
		m[key] = &dynamodb.AttributeValue{N: &s}
	case int32:
		s := strconv.FormatInt(int64(v), 10)
		m[key] = &dynamodb.AttributeValue{N: &s}
	case int64:
		s := strconv.FormatInt(v, 10)
		m[key] = &dynamodb.AttributeValue{N: &s}
	case uint8:
		s := strconv.FormatInt(int64(v), 10)
		m[key] = &dynamodb.AttributeValue{N: &s}
	case uint16:
		s := strconv.FormatInt(int64(v), 10)
		m[key] = &dynamodb.AttributeValue{N: &s}
	case uint32:
		s := strconv.FormatInt(int64(v), 10)
		m[key] = &dynamodb.AttributeValue{N: &s}
	case uint64:
		s := strconv.FormatInt(int64(v), 10)
		m[key] = &dynamodb.AttributeValue{N: &s}
	case float32:
		s := formatFloat(v)
		m[key] = &dynamodb.AttributeValue{N: &s}
	case float64:
		s := formatFloat(v)
		m[key] = &dynamodb.AttributeValue{N: &s}
	}
}
