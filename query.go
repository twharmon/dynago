package dynago

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type Query struct {
	input  *dynamodb.QueryInput
	dynago *Dynago
}

func (d *Dynago) Query(table ...string) *Query {
	q := Query{
		input: &dynamodb.QueryInput{
			ConsistentRead: &d.config.DefaultConsistentRead,
		},
		dynago: d,
	}
	if len(table) == 0 {
		q.input.TableName = &d.config.DefaultTableName
	} else {
		q.input.TableName = &table[0]
	}
	return &q
}

func (q *Query) Index(index string) *Query {
	q.input.IndexName = &index
	return q
}

func (q *Query) KeyConditionExpression(exp string) *Query {
	q.input.KeyConditionExpression = &exp
	return q
}

func (q *Query) ExpressionAttributeValue(key string, val interface{}) *Query {
	if q.input.ExpressionAttributeValues == nil {
		q.input.ExpressionAttributeValues = make(map[string]*dynamodb.AttributeValue)
	}
	switch v := val.(type) {
	case string:
		q.input.ExpressionAttributeValues[key] = &dynamodb.AttributeValue{S: &v}
	case int64:

	}
	return q
}

func (q *Query) Exec(v interface{}) error {
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Pointer {
		return errors.New("dynago: dynago.Query.Exec: v must be pointer")
	}
	for rv.Kind() == reflect.Pointer {
		rv = reflect.Indirect(rv)
	}
	var err error
	output, err := q.dynago.ddb.Query(q.input)
	if err != nil {
		return fmt.Errorf("d.ddb.GetItem: %w", err)
	}
	rt := reflect.TypeOf(v)
	for rt.Kind() == reflect.Pointer {
		rt = rt.Elem()
	}
	ft := rt.Elem()
	inderect := true
	if ft.Kind() == reflect.Pointer {
		ft = ft.Elem()
		inderect = false
	}
	if ft.Kind() == reflect.Pointer {
		return errors.New("dynago: dynago.Query.Exec: elements of v can not be pointers to pointers")
	}
	s := reflect.MakeSlice(rt, len(output.Items), len(output.Items))
	for i, item := range output.Items {
		iv := reflect.New(ft)
		if err := q.dynago.Unmarshal(item, iv.Interface()); err != nil {
			return fmt.Errorf("q.dynago.Unmarshal: %w", err)
		}
		if inderect {
			iv = reflect.Indirect(iv)
		}
		s.Index(i).Set(iv)
	}
	rv.Set(s)
	return nil
}
