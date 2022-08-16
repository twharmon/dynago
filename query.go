package dynago

import (
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// Query represents a Query operation.
type Query struct {
	input  *dynamodb.QueryInput
	dynago *Dynago
	items  interface{}
	err    error
}

// Query returns a Query operation.
func (d *Dynago) Query(items interface{}) *Query {
	return &Query{
		input: &dynamodb.QueryInput{
			ConsistentRead: &d.config.DefaultConsistentRead,
			TableName:      &d.config.DefaultTableName,
		},
		items:  items,
		dynago: d,
	}
}

// TableName sets the table.
func (q *Query) TableName(name string) *Query {
	q.input.TableName = &name
	return q
}

// IndexName sets the index.
func (q *Query) IndexName(index string) *Query {
	q.input.IndexName = &index
	return q
}

// Select sets which attributes will be selected.
func (q *Query) Select(attrs string) *Query {
	q.input.Select = &attrs
	return q
}

// Limit sets the Limit.
func (q *Query) Limit(limit int64) *Query {
	q.input.Limit = &limit
	return q
}

// ProjectionExpression sets the ProjectionExpression.
func (q *Query) ProjectionExpression(exp string) *Query {
	q.input.ProjectionExpression = &exp
	return q
}

// FilterExpression sets the FilterExpression.
func (q *Query) FilterExpression(exp string) *Query {
	q.input.FilterExpression = &exp
	return q
}

// ExclusiveStartKey sets the ExclusiveStartKey.
func (q *Query) ExclusiveStartKey(key map[string]*dynamodb.AttributeValue) *Query {
	q.input.ExclusiveStartKey = key
	return q
}

// ScanIndexForward sets whether or not to scan index forward.
func (q *Query) ScanIndexForward(val bool) *Query {
	q.input.ScanIndexForward = &val
	return q
}

// ConsistentRead sets ConsistentRead.
func (q *Query) ConsistentRead(val bool) *Query {
	q.input.ConsistentRead = &val
	return q
}

// KeyConditionExpression sets the KeyConditionExpression.
func (q *Query) KeyConditionExpression(exp string) *Query {
	q.input.KeyConditionExpression = &exp
	return q
}

// ExpressionAttributeValue sets an ExpressionAttributeValue.
func (q *Query) ExpressionAttributeValue(key string, val interface{}, layout ...string) *Query {
	if q.input.ExpressionAttributeValues == nil {
		q.input.ExpressionAttributeValues = make(map[string]*dynamodb.AttributeValue)
	}
	lay := time.RFC3339
	if len(layout) > 0 {
		lay = layout[0]
	}
	av, err := q.dynago.simpleMarshal(reflect.ValueOf(val), lay)
	if err != nil {
		q.err = err
	}
	q.input.ExpressionAttributeValues[key] = av
	return q
}

// ExpressionAttributeName sets an ExpressionAttributeName.
func (q *Query) ExpressionAttributeName(name string, sub string) *Query {
	if q.input.ExpressionAttributeNames == nil {
		q.input.ExpressionAttributeNames = make(map[string]*string)
	}
	q.input.ExpressionAttributeNames[name] = &sub
	return q
}

// Exec executes the operation.
func (q *Query) Exec() error {
	if q.err != nil {
		return q.err
	}
	rv := reflect.ValueOf(q.items)
	if rv.Kind() != reflect.Ptr {
		return errors.New("dynago: dynago.Query.Exec: v must be pointer")
	}
	for rv.Kind() == reflect.Ptr {
		rv = reflect.Indirect(rv)
	}
	var err error
	output, err := q.dynago.ddb.Query(q.input)
	if err != nil {
		return fmt.Errorf("d.ddb.GetItem: %w", err)
	}
	rt := reflect.TypeOf(q.items)
	for rt.Kind() == reflect.Ptr {
		rt = rt.Elem()
	}
	ft := rt.Elem()
	indirect := true
	if ft.Kind() == reflect.Ptr {
		ft = ft.Elem()
		indirect = false
	}
	if ft.Kind() == reflect.Ptr {
		return errors.New("dynago: dynago.Query.Exec: elements of v can not be pointers to pointers")
	}
	s := reflect.MakeSlice(rt, len(output.Items), len(output.Items))
	for i, item := range output.Items {
		iv := reflect.New(ft)
		if err := q.dynago.Unmarshal(item, iv.Interface()); err != nil {
			return fmt.Errorf("q.dynago.Unmarshal: %w", err)
		}
		if indirect {
			iv = reflect.Indirect(iv)
		}
		s.Index(i).Set(iv)
	}
	rv.Set(s)
	return nil
}
