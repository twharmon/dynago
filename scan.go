package dynago

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type Scan struct {
	input  *dynamodb.ScanInput
	dynago *Dynago
	items  interface{}
}

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

func (q *Scan) TableName(name string) *Scan {
	q.input.TableName = &name
	return q
}

func (q *Scan) Segment(segment int64) *Scan {
	q.input.Segment = &segment
	return q
}

func (q *Scan) TotalSegments(segments int64) *Scan {
	q.input.TotalSegments = &segments
	return q
}

func (q *Scan) IndexName(index string) *Scan {
	q.input.IndexName = &index
	return q
}

func (q *Scan) Select(attrs string) *Scan {
	q.input.Select = &attrs
	return q
}

func (q *Scan) Limit(limit int64) *Scan {
	q.input.Limit = &limit
	return q
}

func (q *Scan) ProjectionExpression(exp string) *Scan {
	q.input.ProjectionExpression = &exp
	return q
}

func (q *Scan) FilterExpression(exp string) *Scan {
	q.input.FilterExpression = &exp
	return q
}

func (q *Scan) ExclusiveStartKey(key map[string]*dynamodb.AttributeValue) *Scan {
	q.input.ExclusiveStartKey = key
	return q
}

func (q *Scan) ConsistentRead(val bool) *Scan {
	q.input.ConsistentRead = &val
	return q
}

func (q *Scan) ExpressionAttributeValue(key string, val interface{}) *Scan {
	if q.input.ExpressionAttributeValues == nil {
		q.input.ExpressionAttributeValues = make(map[string]*dynamodb.AttributeValue)
	}
	expressionAttributeValue(q.input.ExpressionAttributeValues, key, val)
	return q
}

func (q *Scan) ExpressionAttributeName(name string, sub string) *Scan {
	if q.input.ExpressionAttributeNames == nil {
		q.input.ExpressionAttributeNames = make(map[string]*string)
	}
	q.input.ExpressionAttributeNames[name] = &sub
	return q
}

func (q *Scan) Exec() error {
	rv := reflect.ValueOf(q.items)
	if rv.Kind() != reflect.Pointer {
		return errors.New("dynago: dynago.Scan.Exec: v must be pointer")
	}
	for rv.Kind() == reflect.Pointer {
		rv = reflect.Indirect(rv)
	}
	var err error
	output, err := q.dynago.ddb.Scan(q.input)
	if err != nil {
		return fmt.Errorf("d.ddb.GetItem: %w", err)
	}
	rt := reflect.TypeOf(q.items)
	for rt.Kind() == reflect.Pointer {
		rt = rt.Elem()
	}
	ft := rt.Elem()
	indirect := true
	if ft.Kind() == reflect.Pointer {
		ft = ft.Elem()
		indirect = false
	}
	if ft.Kind() == reflect.Pointer {
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
