package dynago

import (
	"fmt"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// PutItem represents a PutItem operation.
type PutItem struct {
	item   interface{}
	input  *dynamodb.PutItemInput
	dynago *Dynago
}

// PutItem returns a PutItem operation.
func (d *Dynago) PutItem(item interface{}) *PutItem {
	return &PutItem{
		input:  &dynamodb.PutItemInput{TableName: &d.config.DefaultTableName},
		dynago: d,
		item:   item,
	}
}

// TableName sets the table.
func (q *PutItem) TableName(table string) *PutItem {
	q.input.TableName = &table
	return q
}

// ConditionExpression sets the ConditionExpression.
func (q *PutItem) ConditionExpression(exp string) *PutItem {
	q.input.ConditionExpression = &exp
	return q
}

// ExpressionAttributeValue sets an ExpressionAttributeValue.
func (q *PutItem) ExpressionAttributeValue(key string, val interface{}) *PutItem {
	if q.input.ExpressionAttributeValues == nil {
		q.input.ExpressionAttributeValues = make(map[string]*dynamodb.AttributeValue)
	}
	expressionAttributeValue(q.input.ExpressionAttributeValues, key, val)
	return q
}

// ExpressionAttributeName sets an ExpressionAttributeName.
func (q *PutItem) ExpressionAttributeName(name string, sub string) *PutItem {
	if q.input.ExpressionAttributeNames == nil {
		q.input.ExpressionAttributeNames = make(map[string]*string)
	}
	q.input.ExpressionAttributeNames[name] = &sub
	return q
}

// Exec exeutes the operation.
func (q *PutItem) Exec() error {
	var err error
	q.input.Item, err = q.dynago.Marshal(q.item)
	if err != nil {
		return fmt.Errorf("q.dynago.Marshal: %w", err)
	}
	_, err = q.dynago.ddb.PutItem(q.input)
	if err != nil {
		return fmt.Errorf("d.ddb.PutItem: %w", err)
	}
	return err
}

// TransactionWriteItem implements the TransactionWriteItemer
// interface.
func (q *PutItem) TransactionWriteItem() (*dynamodb.TransactWriteItem, error) {
	item, err := q.dynago.Marshal(q.item)
	if err != nil {
		return nil, err
	}
	return &dynamodb.TransactWriteItem{
		Put: &dynamodb.Put{
			Item:                      item,
			TableName:                 q.input.TableName,
			ConditionExpression:       q.input.ConditionExpression,
			ExpressionAttributeNames:  q.input.ExpressionAttributeNames,
			ExpressionAttributeValues: q.input.ExpressionAttributeValues,
		},
	}, nil
}
