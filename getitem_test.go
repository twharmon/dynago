package dynago_test

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/twharmon/dynago"
)

func TestGetItemBasic(t *testing.T) {
	ddb := mock(t)
	client := dynago.New(ddb)
	type Person struct {
		Name string `idx:"primary" attr:"PK" fmt:"Person#{}"`
		Age  int64
	}
	want := Person{
		Name: "foo",
		Age:  33,
	}
	tableName := "bar"
	ddb.MockGet(&dynamodb.GetItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"PK": {S: aws.String(fmt.Sprintf("Person#%s", want.Name))},
		},
		TableName:      &tableName,
		ConsistentRead: aws.Bool(false),
	}, &dynamodb.GetItemOutput{
		Item: map[string]*dynamodb.AttributeValue{
			"PK":  {S: aws.String(fmt.Sprintf("Person#%s", want.Name))},
			"Age": {N: aws.String(strconv.FormatInt(want.Age, 10))},
		},
	})
	got := Person{
		Name: want.Name,
	}
	if err := client.Get(&got).Table(tableName).Exec(); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
	ddb.done()
}

func TestGetItemDefaultTableName(t *testing.T) {
	ddb := mock(t)
	tableName := "bar"
	client := dynago.New(ddb, &dynago.Config{DefaultTableName: tableName})
	type Person struct {
		Name string `idx:"primary" attr:"PK" fmt:"Person#{}"`
		Age  int64
	}
	want := Person{
		Name: "foo",
		Age:  33,
	}
	ddb.MockGet(&dynamodb.GetItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"PK": {S: aws.String(fmt.Sprintf("Person#%s", want.Name))},
		},
		TableName:      &tableName,
		ConsistentRead: aws.Bool(false),
	}, &dynamodb.GetItemOutput{
		Item: map[string]*dynamodb.AttributeValue{
			"PK":  {S: aws.String(fmt.Sprintf("Person#%s", want.Name))},
			"Age": {N: aws.String(strconv.FormatInt(want.Age, 10))},
		},
	})
	got := Person{
		Name: want.Name,
	}
	if err := client.Get(&got).Exec(); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
	ddb.done()
}
