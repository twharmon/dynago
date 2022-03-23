package dynago

import (
	"fmt"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type DeleteItem struct {
	item   interface{}
	input  *dynamodb.DeleteItemInput
	dynago *Dynago
}

func (d *Dynago) Delete(item interface{}) *DeleteItem {
	return &DeleteItem{
		item: item,
		input: &dynamodb.DeleteItemInput{
			TableName: &d.config.DefaultTableName,
		},
		dynago: d,
	}
}

func (q *DeleteItem) Table(name string) *DeleteItem {
	q.input.TableName = &name
	return q
}

func (q *DeleteItem) Exec() error {
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

func (q *DeleteItem) TransactionWriteItem() (*dynamodb.TransactWriteItem, error) {
	key, err := q.dynago.key(q.item)
	if err != nil {
		return nil, err
	}
	return &dynamodb.TransactWriteItem{
		Delete: &dynamodb.Delete{
			Key:       key,
			TableName: q.input.TableName,
		},
	}, nil
}
