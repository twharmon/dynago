package dynago

import (
	"fmt"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type ConditionCheck struct {
	check  *dynamodb.ConditionCheck
	item   interface{}
	dynago *Dynago
}

func (d *Dynago) ConditionCheck(item interface{}) *ConditionCheck {
	return &ConditionCheck{
		check: &dynamodb.ConditionCheck{
			TableName: &d.config.DefaultTableName,
		},
		item:   item,
		dynago: d,
	}
}

func (c *ConditionCheck) TableName(name string) *ConditionCheck {
	c.check.TableName = &name
	return c
}
func (q *ConditionCheck) ConditionExpression(exp string) *ConditionCheck {
	q.check.ConditionExpression = &exp
	return q
}

func (q *ConditionCheck) ExpressionAttributeValue(key string, val interface{}) *ConditionCheck {
	if q.check.ExpressionAttributeValues == nil {
		q.check.ExpressionAttributeValues = make(map[string]*dynamodb.AttributeValue)
	}
	expressionAttributeValue(q.check.ExpressionAttributeValues, key, val)
	return q
}

func (q *ConditionCheck) ExpressionAttributeName(name string, sub string) *ConditionCheck {
	if q.check.ExpressionAttributeNames == nil {
		q.check.ExpressionAttributeNames = make(map[string]*string)
	}
	q.check.ExpressionAttributeNames[name] = &sub
	return q
}

func (c *ConditionCheck) TransactionWriteItem() (*dynamodb.TransactWriteItem, error) {
	var err error
	c.check.Key, err = c.dynago.key(c.item)
	if err != nil {
		return nil, fmt.Errorf("q.dynago.key: %w", err)
	}
	return &dynamodb.TransactWriteItem{ConditionCheck: c.check}, nil
}
