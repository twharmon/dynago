package dynago_test

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/twharmon/dynago"
)

func TestTransactWriteItemsBasic(t *testing.T) {
	ddb := mock(t)
	client := dynago.New(ddb)
	type Person struct {
		Name string `idx:"primary" attr:"PK" fmt:"Person#{}"`
		Age  int64
	}
	p := Person{
		Name: "foo",
		Age:  33,
	}
	p2 := Person{
		Name: "bar",
		Age:  34,
	}
	tableName := "bar"
	ddb.MockTransactWriteItems(&dynamodb.TransactWriteItemsInput{
		TransactItems: []*dynamodb.TransactWriteItem{
			{
				Put: &dynamodb.Put{
					Item: map[string]*dynamodb.AttributeValue{
						"PK":  {S: aws.String(fmt.Sprintf("Person#%s", p.Name))},
						"Age": {N: aws.String(strconv.FormatInt(p.Age, 10))},
					},
					TableName: &tableName,
				},
			},
			{
				Update: &dynamodb.Update{
					Key: map[string]*dynamodb.AttributeValue{
						"PK": {S: aws.String(fmt.Sprintf("Person#%s", p.Name))},
					},
					TableName: &tableName,
				},
			},
			{
				Delete: &dynamodb.Delete{
					Key: map[string]*dynamodb.AttributeValue{
						"PK": {S: aws.String(fmt.Sprintf("Person#%s", p2.Name))},
					},
					TableName: &tableName,
				},
			},
			{
				ConditionCheck: &dynamodb.ConditionCheck{
					Key: map[string]*dynamodb.AttributeValue{
						"PK": {S: aws.String(fmt.Sprintf("Person#%s", p2.Name))},
					},
					ConditionExpression: aws.String("foo"),
					ExpressionAttributeNames: map[string]*string{
						"foo": aws.String("bar"),
					},
					ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
						"foo": {S: aws.String("bar")},
					},
					TableName: &tableName,
				},
			},
		},
	})

	if err := client.TransactionWriteItems().
		Items(
			client.PutItem(&p).TableName(tableName),
			client.UpdateItem(&p).TableName(tableName),
			client.DeleteItem(&p2).TableName(tableName),
			client.ConditionCheck(&p2).
				TableName(tableName).
				ConditionExpression("foo").
				ExpressionAttributeName("foo", "bar").
				ExpressionAttributeValue("foo", "bar"),
		).
		Exec(); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	ddb.done()
}

func TestTransactWriteItemsClientReqTok(t *testing.T) {
	ddb := mock(t)
	client := dynago.New(ddb)
	type Person struct {
		Name string `idx:"primary" attr:"PK" fmt:"Person#{}"`
		Age  int64
	}
	p := Person{
		Name: "foo",
		Age:  33,
	}
	p2 := Person{
		Name: "bar",
		Age:  34,
	}
	tableName := "bar"
	ddb.MockTransactWriteItems(&dynamodb.TransactWriteItemsInput{
		ClientRequestToken: aws.String("foo"),
		TransactItems: []*dynamodb.TransactWriteItem{
			{
				Put: &dynamodb.Put{
					Item: map[string]*dynamodb.AttributeValue{
						"PK":  {S: aws.String(fmt.Sprintf("Person#%s", p.Name))},
						"Age": {N: aws.String(strconv.FormatInt(p.Age, 10))},
					},
					TableName: &tableName,
				},
			},
			{
				Delete: &dynamodb.Delete{
					Key: map[string]*dynamodb.AttributeValue{
						"PK": {S: aws.String(fmt.Sprintf("Person#%s", p2.Name))},
					},
					TableName: &tableName,
				},
			},
		},
	})

	if err := client.TransactionWriteItems().
		Items(
			client.PutItem(&p).TableName(tableName),
			client.DeleteItem(&p2).TableName(tableName),
		).
		ClientRequestToken("foo").
		Exec(); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	ddb.done()
}
