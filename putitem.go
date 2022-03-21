package dynago

import (
	"fmt"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type PutItem struct {
	input  *dynamodb.PutItemInput
	dynago *Dynago
}

func (d *Dynago) PutItem(table ...string) *PutItem {
	q := PutItem{
		input:  &dynamodb.PutItemInput{},
		dynago: d,
	}
	if len(table) == 0 {
		q.input.TableName = &d.config.DefaultTableName
	} else {
		q.input.TableName = &table[0]
	}
	return &q
}

func (q *PutItem) Exec(v interface{}) error {
	var err error
	q.input.Item, err = q.dynago.Marshal(v)
	if err != nil {
		return fmt.Errorf("q.dynago.Marshal: %w", err)
	}
	_, err = q.dynago.ddb.PutItem(q.input)
	if err != nil {
		return fmt.Errorf("d.ddb.GetItem: %w", err)
	}
	return err
}
