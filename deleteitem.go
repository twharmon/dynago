package dynago

import (
	"fmt"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type DeleteItem struct {
	input  *dynamodb.DeleteItemInput
	dynago *Dynago
}

func (d *Dynago) DeleteItem(table ...string) *DeleteItem {
	q := DeleteItem{
		input:  &dynamodb.DeleteItemInput{},
		dynago: d,
	}
	if len(table) == 0 {
		q.input.TableName = &d.config.DefaultTableName
	} else {
		q.input.TableName = &table[0]
	}
	return &q
}

func (q *DeleteItem) Exec(v interface{}) error {
	var err error
	q.input.Key, err = q.dynago.key(v)
	if err != nil {
		return fmt.Errorf("q.dynago.key: %w", err)
	}
	_, err = q.dynago.ddb.DeleteItem(q.input)
	if err != nil {
		return fmt.Errorf("d.ddb.DeleteItem: %w", err)
	}
	return nil
}
