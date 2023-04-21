package dynago

import (
	"fmt"
	"reflect"
	"strconv"

	"time"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

func tyVal(v interface{}) (reflect.Type, reflect.Value) {
	val := reflect.ValueOf(v)
	for val.Kind() == reflect.Pointer {
		val = val.Elem()
	}
	return val.Type(), val
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
			v.SetString(*av.S)
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if av.N != nil {
			i, err := strconv.ParseInt(*av.N, 10, v.Type().Bits())
			if err != nil {
				return err
			}
			v.SetInt(i)
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if av.N != nil {
			i, err := strconv.ParseUint(*av.N, 10, v.Type().Bits())
			if err != nil {
				return err
			}
			v.SetUint(i)
		}
	case reflect.Float32, reflect.Float64:
		if av.N != nil {
			i, err := strconv.ParseFloat(*av.N, v.Type().Bits())
			if err != nil {
				return err
			}
			v.SetFloat(i)
		}
	case reflect.Bool:
		if av.BOOL != nil {
			v.SetBool(*av.BOOL)
		}
	case reflect.Map:
		if av.M != nil {
			if err := dynamodbattribute.UnmarshalMap(av.M, v.Addr().Interface()); err != nil {
				return err
			}
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
		val := v.String()
		return &dynamodb.AttributeValue{S: &val}, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		s := strconv.FormatInt(v.Int(), 10)
		return &dynamodb.AttributeValue{N: &s}, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		s := strconv.FormatUint(v.Uint(), 10)
		return &dynamodb.AttributeValue{N: &s}, nil
	case reflect.Float32, reflect.Float64:
		s := strconv.FormatFloat(v.Float(), 'f', -1, v.Type().Bits())
		return &dynamodb.AttributeValue{N: &s}, nil
	case reflect.Bool:
		val := v.Bool()
		return &dynamodb.AttributeValue{BOOL: &val}, nil
	case reflect.Map:
		av, err := dynamodbattribute.MarshalMap(v.Interface())
		return &dynamodb.AttributeValue{M: av}, err
	default:
		return nil, nil
	}
}

type Float interface {
	float32 | float64
}
