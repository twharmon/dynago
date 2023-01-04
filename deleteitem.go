package dynago

import (
	"fmt"
	"reflect"
	"time"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// DeleteItem represents a delete item operation.
type DeleteItem struct {
	item   Keyer
	input  *dynamodb.DeleteItemInput
	dynago *Dynago
	err    error
}

// DeleteItem creates a DeleteItem operation.
func (d *Dynago) DeleteItem(item Keyer) *DeleteItem {
	return &DeleteItem{
		item: item,
		input: &dynamodb.DeleteItemInput{
			TableName: &d.config.DefaultTableName,
		},
		dynago: d,
	}
}

// TableName sets the table.
func (q *DeleteItem) TableName(name string) *DeleteItem {
	q.input.TableName = &name
	return q
}

// ExpressionAttributeValue sets an ExpressionAttributeValue.
func (q *DeleteItem) ExpressionAttributeValue(key string, val interface{}, layout ...string) *DeleteItem {
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

// ConditionExpression sets the ConditionExpression.
func (q *DeleteItem) ConditionExpression(exp string) *DeleteItem {
	q.input.ConditionExpression = &exp
	return q
}

// ExpressionAttributeName sets a ExpressionAttributeName.
func (q *DeleteItem) ExpressionAttributeName(name string, sub string) *DeleteItem {
	if q.input.ExpressionAttributeNames == nil {
		q.input.ExpressionAttributeNames = make(map[string]*string)
	}
	q.input.ExpressionAttributeNames[name] = &sub
	return q
}

// Exec executes the operation.
func (q *DeleteItem) Exec() error {
	if q.err != nil {
		return q.err
	}
	var err error
	q.input.Key, err = q.dynago.key(q.item)
	if err != nil {
		return fmt.Errorf("q.dynago.key: %w", err)
	}
	_, err = q.dynago.ddb.DeleteItem(q.input)
	if err != nil {
		return fmt.Errorf("d.ddb.DeleteItem: %w", err)
	}
	return nil
}

// TransactionWriteItem implements the TransactionWriteItemer
// interface.
func (q *DeleteItem) TransactionWriteItem() (*dynamodb.TransactWriteItem, error) {
	if q.err != nil {
		return nil, q.err
	}
	key, err := q.dynago.key(q.item)
	if err != nil {
		return nil, err
	}
	return &dynamodb.TransactWriteItem{
		Delete: &dynamodb.Delete{
			Key:                       key,
			TableName:                 q.input.TableName,
			ConditionExpression:       q.input.ConditionExpression,
			ExpressionAttributeNames:  q.input.ExpressionAttributeNames,
			ExpressionAttributeValues: q.input.ExpressionAttributeValues,
		},
	}, nil
}
