package dynago

import (
	"fmt"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type UpdateItem struct {
	item   interface{}
	input  *dynamodb.UpdateItemInput
	dynago *Dynago
}

func (d *Dynago) UpdateItem(item interface{}) *UpdateItem {
	return &UpdateItem{
		input:  &dynamodb.UpdateItemInput{TableName: &d.config.DefaultTableName},
		dynago: d,
		item:   item,
	}
}

func (q *UpdateItem) TableName(table string) *UpdateItem {
	q.input.TableName = &table
	return q
}

func (q *UpdateItem) UpdateExpression(exp string) *UpdateItem {
	q.input.UpdateExpression = &exp
	return q
}

func (q *UpdateItem) ConditionExpression(exp string) *UpdateItem {
	q.input.ConditionExpression = &exp
	return q
}

func (q *UpdateItem) ExpressionAttributeValue(key string, val interface{}) *UpdateItem {
	if q.input.ExpressionAttributeValues == nil {
		q.input.ExpressionAttributeValues = make(map[string]*dynamodb.AttributeValue)
	}
	expressionAttributeValue(q.input.ExpressionAttributeValues, key, val)
	return q
}

func (q *UpdateItem) ExpressionAttributeName(name string, sub string) *UpdateItem {
	if q.input.ExpressionAttributeNames == nil {
		q.input.ExpressionAttributeNames = make(map[string]*string)
	}
	q.input.ExpressionAttributeNames[name] = &sub
	return q
}

func (q *UpdateItem) Exec() error {
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

func (q *UpdateItem) TransactionWriteItem() (*dynamodb.TransactWriteItem, error) {
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
