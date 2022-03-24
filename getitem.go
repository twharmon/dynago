package dynago

import (
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

var ErrItemNotFound = errors.New("item not found")

type GetItem struct {
	item   interface{}
	input  *dynamodb.GetItemInput
	dynago *Dynago
}

func (d *Dynago) Get(item interface{}) *GetItem {
	return &GetItem{
		input: &dynamodb.GetItemInput{
			ConsistentRead: &d.config.DefaultConsistentRead,
			TableName:      &d.config.DefaultTableName,
		},
		item:   item,
		dynago: d,
	}
}

func (q *GetItem) TableName(name string) *GetItem {
	q.input.TableName = &name
	return q
}

func (q *GetItem) ProjectionExpression(exp string) *GetItem {
	q.input.ProjectionExpression = &exp
	return q
}

func (q *GetItem) ExpressionAttributeName(name string, sub string) *GetItem {
	if q.input.ExpressionAttributeNames == nil {
		q.input.ExpressionAttributeNames = make(map[string]*string)
	}
	q.input.ExpressionAttributeNames[name] = &sub
	return q
}

func (q *GetItem) ConsistentRead(consisten bool) *GetItem {
	q.input.ConsistentRead = &consisten
	return q
}

func (q *GetItem) Exec() error {
	var err error
	q.input.Key, err = q.dynago.key(q.item)
	if err != nil {
		return fmt.Errorf("q.dynago.key: %w", err)
	}
	output, err := q.dynago.ddb.GetItem(q.input)
	if err != nil {
		return fmt.Errorf("d.ddb.GetItem: %w", err)
	}
	if len(output.Item) == 0 {
		return ErrItemNotFound
	}
	return q.dynago.Unmarshal(output.Item, q.item)
}
