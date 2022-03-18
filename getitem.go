package dynago

import (
	"fmt"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type GetItemQuery struct {
	input  *dynamodb.GetItemInput
	dynago *Dynago
}

func (d *Dynago) GetItem(table ...string) *GetItemQuery {
	q := GetItemQuery{
		input: &dynamodb.GetItemInput{
			ConsistentRead: &d.config.DefaultConsistentRead,
		},
		dynago: d,
	}
	if len(table) == 0 {
		q.input.TableName = &d.config.DefaultTableName
	} else {
		q.input.TableName = &table[0]
	}
	return &q
}

func (q *GetItemQuery) Exec(v interface{}) error {
	var err error
	q.input.Key, err = q.dynago.key(v)
	if err != nil {
		return fmt.Errorf("q.dynago.key: %w", err)
	}
	output, err := q.dynago.ddb.GetItem(q.input)
	if err != nil {
		return fmt.Errorf("d.ddb.GetItem: %w", err)
	}
	return q.dynago.Unmarshal(output.Item, v)
}
