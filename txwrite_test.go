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
				Put: &dynamodb.Put{
					Item: map[string]*dynamodb.AttributeValue{
						"PK":  {S: aws.String(fmt.Sprintf("Person#%s", p2.Name))},
						"Age": {N: aws.String(strconv.FormatInt(p2.Age, 10))},
					},
					TableName: &tableName,
				},
			},
		},
	})

	if err := client.TransactWriteItems().
		Items(
			client.Put(&p).Table(tableName),
			client.Put(&p2).Table(tableName),
		).
		Exec(); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	ddb.done()
}