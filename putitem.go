package dynago

import (
	"fmt"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type PutItem struct {
	item   interface{}
	input  *dynamodb.PutItemInput
	dynago *Dynago
}

func (d *Dynago) Put(item interface{}) *PutItem {
	return &PutItem{
		input:  &dynamodb.PutItemInput{TableName: &d.config.DefaultTableName},
		dynago: d,
		item:   item,
	}
}

func (q *PutItem) TableName(table string) *PutItem {
	q.input.TableName = &table
	return q
}

func (q *PutItem) ConditionExpression(exp string) *PutItem {
	q.input.ConditionExpression = &exp
	return q
}

func (q *PutItem) ExpressionAttributeValue(key string, val interface{}) *PutItem {
	if q.input.ExpressionAttributeValues == nil {
		q.input.ExpressionAttributeValues = make(map[string]*dynamodb.AttributeValue)
	}
	expressionAttributeValue(q.input.ExpressionAttributeValues, key, val)
	return q
}

func (q *PutItem) ExpressionAttributeName(name string, sub string) *PutItem {
	if q.input.ExpressionAttributeNames == nil {
		q.input.ExpressionAttributeNames = make(map[string]*string)
	}
	q.input.ExpressionAttributeNames[name] = &sub
	return q
}

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

func (q *PutItem) TransactionWriteItem() (*dynamodb.TransactWriteItem, error) {
	item, err := q.dynago.Marshal(q.item)
	if err != nil {
		return nil, err
	}
	return &dynamodb.TransactWriteItem{
		Put: &dynamodb.Put{
			Item:      item,
			TableName: q.input.TableName,
		},
	}, nil
}
