package dynago

import (
	"fmt"
	"reflect"
	"time"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// ConditionCheck represents a request to perform a check that an
// item exists or to check the condition of specific attributes of
// the item.
type ConditionCheck struct {
	check  *dynamodb.ConditionCheck
	item   Keyer
	dynago *Dynago
	err    error
}

// ConditionCheck creates a ConditionCheck operation. The given item
// must have the primary key fields set.
func (d *Dynago) ConditionCheck(item Keyer) *ConditionCheck {
	return &ConditionCheck{
		check: &dynamodb.ConditionCheck{
			TableName: &d.config.DefaultTableName,
		},
		item:   item,
		dynago: d,
	}
}

// TableName sets the table to be checked.
func (c *ConditionCheck) TableName(name string) *ConditionCheck {
	c.check.TableName = &name
	return c
}

// ConditionExpression sets the ConditionExpression.
func (q *ConditionCheck) ConditionExpression(exp string) *ConditionCheck {
	q.check.ConditionExpression = &exp
	return q
}

// ExpressionAttributeValue sets an ExpressionAttributeValue.
func (q *ConditionCheck) ExpressionAttributeValue(key string, val interface{}, layout ...string) *ConditionCheck {
	if q.check.ExpressionAttributeValues == nil {
		q.check.ExpressionAttributeValues = make(map[string]*dynamodb.AttributeValue)
	}
	lay := time.RFC3339
	if len(layout) > 0 {
		lay = layout[0]
	}
	av, err := q.dynago.simpleMarshal(reflect.ValueOf(val), lay)
	if err != nil {
		q.err = err
	}
	q.check.ExpressionAttributeValues[key] = av
	return q
}

// ExpressionAttributeName sets an ExpressionAttributeName.
func (q *ConditionCheck) ExpressionAttributeName(name string, sub string) *ConditionCheck {
	if q.check.ExpressionAttributeNames == nil {
		q.check.ExpressionAttributeNames = make(map[string]*string)
	}
	q.check.ExpressionAttributeNames[name] = &sub
	return q
}

// TransactionWriteItem implements the TransactionWriteItemer
// interface.
func (c *ConditionCheck) TransactionWriteItem() (*dynamodb.TransactWriteItem, error) {
	if c.err != nil {
		return nil, c.err
	}
	var err error
	c.check.Key, err = c.dynago.key(c.item)
	if err != nil {
		return nil, fmt.Errorf("q.dynago.key: %w", err)
	}
	return &dynamodb.TransactWriteItem{ConditionCheck: c.check}, nil
}
