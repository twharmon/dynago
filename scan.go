package dynago

import (
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// Scan represents a Scan operation.
type Scan struct {
	input  *dynamodb.ScanInput
	dynago *Dynago
	items  interface{}
	err    error
}

// Scan returns a Scan operation.
func (d *Dynago) Scan(items interface{}) *Scan {
	return &Scan{
		input: &dynamodb.ScanInput{
			ConsistentRead: &d.config.DefaultConsistentRead,
			TableName:      &d.config.DefaultTableName,
		},
		items:  items,
		dynago: d,
	}
}

// TableName sets the table.
func (q *Scan) TableName(name string) *Scan {
	q.input.TableName = &name
	return q
}

// Segment sets Segment.
func (q *Scan) Segment(segment int64) *Scan {
	q.input.Segment = &segment
	return q
}

// TotalSegments sets TotalSegments.
func (q *Scan) TotalSegments(segments int64) *Scan {
	q.input.TotalSegments = &segments
	return q
}

// IndexName sets the IndexName.
func (q *Scan) IndexName(index string) *Scan {
	q.input.IndexName = &index
	return q
}

// Select sets which attributes will be selected.
func (q *Scan) Select(attrs string) *Scan {
	q.input.Select = &attrs
	return q
}

// Limit sets the Limit.
func (q *Scan) Limit(limit int64) *Scan {
	q.input.Limit = &limit
	return q
}

// ProjectionExpression sets the ProjectionExpression.
func (q *Scan) ProjectionExpression(exp string) *Scan {
	q.input.ProjectionExpression = &exp
	return q
}

// FilterExpression sets the FilterExpression.
func (q *Scan) FilterExpression(exp string) *Scan {
	q.input.FilterExpression = &exp
	return q
}

// ExclusiveStartKey sets the ExclusiveStartKey.
func (q *Scan) ExclusiveStartKey(key map[string]*dynamodb.AttributeValue) *Scan {
	q.input.ExclusiveStartKey = key
	return q
}

// ConsistentRead sets ConsistentRead.
func (q *Scan) ConsistentRead(val bool) *Scan {
	q.input.ConsistentRead = &val
	return q
}

// ExpressionAttributeValue sets an ExpressionAttributeValue.
func (q *Scan) ExpressionAttributeValue(key string, val interface{}, layout ...string) *Scan {
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
func (q *Scan) ExpressionAttributeName(name string, sub string) *Scan {
	if q.input.ExpressionAttributeNames == nil {
		q.input.ExpressionAttributeNames = make(map[string]*string)
	}
	q.input.ExpressionAttributeNames[name] = &sub
	return q
}

// Exec executes the operation.
func (q *Scan) Exec() error {
	rv := reflect.ValueOf(q.items)
	if rv.Kind() != reflect.Ptr {
		return errors.New("dynago: dynago.Scan.Exec: v must be pointer")
	}
	for rv.Kind() == reflect.Ptr {
		rv = reflect.Indirect(rv)
	}
	var err error
	output, err := q.dynago.ddb.Scan(q.input)
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
		return errors.New("dynago: dynago.Scan.Exec: elements of v can not be pointers to pointers")
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
