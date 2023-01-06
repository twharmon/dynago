package dynago

import (
	"fmt"
	"reflect"
	"time"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// UpdateItem represents an UpdateItem operation.
type UpdateItem struct {
	item   Keyer
	input  *dynamodb.UpdateItemInput
	dynago *Dynago
	err    error
}

// UpdateItem returns an UpdateItem operation.
func (d *Dynago) UpdateItem(item Keyer) *UpdateItem {
	return &UpdateItem{
		input:  &dynamodb.UpdateItemInput{TableName: &d.config.DefaultTableName},
		dynago: d,
		item:   item,
	}
}

// TableName sets the TableName.
func (q *UpdateItem) TableName(table string) *UpdateItem {
	q.input.TableName = &table
	return q
}

// UpdateExpression sets the UpdateExpression.
func (q *UpdateItem) UpdateExpression(exp string) *UpdateItem {
	q.input.UpdateExpression = &exp
	return q
}

// ConditionExpression sets the ConditionExpression.
func (q *UpdateItem) ConditionExpression(exp string) *UpdateItem {
	q.input.ConditionExpression = &exp
	return q
}

// ExpressionAttributeValue sets an ExpressionAttributeValue.
func (q *UpdateItem) ExpressionAttributeValue(key string, val interface{}, layout ...string) *UpdateItem {
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
func (q *UpdateItem) ExpressionAttributeName(name string, sub string) *UpdateItem {
	if q.input.ExpressionAttributeNames == nil {
		q.input.ExpressionAttributeNames = make(map[string]*string)
	}
	q.input.ExpressionAttributeNames[name] = &sub
	return q
}

// Exec executes the operation.
func (q *UpdateItem) Exec() error {
	if q.err != nil {
		return q.err
	}
	var err error
	q.input.Key, err = q.dynago.key(q.item)
	if err != nil {
		return fmt.Errorf("q.dynago.key: %w", err)
	}
	_, err = q.dynago.ddb.UpdateItem(q.input)
	if err != nil {
		return fmt.Errorf("d.ddb.UpdateItem: %w", err)
	}
	return err
}

// TransactionWriteItem implements the TransactionWriteItemer
// interface.
func (q *UpdateItem) TransactionWriteItem() (*dynamodb.TransactWriteItem, error) {
	if q.err != nil {
		return nil, q.err
	}
	var err error
	q.input.Key, err = q.dynago.key(q.item)
	if err != nil {
		return nil, fmt.Errorf("q.dynago.key: %w", err)
	}
	return &dynamodb.TransactWriteItem{
		Update: &dynamodb.Update{
			ConditionExpression:       q.input.ConditionExpression,
			ExpressionAttributeNames:  q.input.ExpressionAttributeNames,
			ExpressionAttributeValues: q.input.ExpressionAttributeValues,
			Key:                       q.input.Key,
			UpdateExpression:          q.input.UpdateExpression,
			TableName:                 q.input.TableName,
		},
	}, nil
}
