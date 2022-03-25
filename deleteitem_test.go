package dynago_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/twharmon/dynago"
)

func TestDeleteItemBasic(t *testing.T) {
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
	ddb.MockDelete(&dynamodb.DeleteItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"PK": {S: aws.String(fmt.Sprintf("Person#%s", want.Name))},
		},
		TableName: &tableName,
	})
	got := Person{
		Name: want.Name,
	}
	if err := client.DeleteItem(&got).TableName(tableName).Exec(); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	ddb.done()
}

func TestDeleteItemConditionExpression(t *testing.T) {
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
	ddb.MockDelete(&dynamodb.DeleteItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"PK": {S: aws.String(fmt.Sprintf("Person#%s", want.Name))},
		},
		TableName:           &tableName,
		ConditionExpression: aws.String("foo"),
	})
	got := Person{
		Name: want.Name,
	}
	if err := client.DeleteItem(&got).TableName(tableName).ConditionExpression("foo").Exec(); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	ddb.done()
}

func TestDeleteItemExpressionAttributeNames(t *testing.T) {
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
	ddb.MockDelete(&dynamodb.DeleteItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"PK": {S: aws.String(fmt.Sprintf("Person#%s", want.Name))},
		},
		TableName:                &tableName,
		ExpressionAttributeNames: map[string]*string{"foo": aws.String("#f")},
	})
	got := Person{
		Name: want.Name,
	}
	if err := client.DeleteItem(&got).TableName(tableName).ExpressionAttributeName("foo", "#f").Exec(); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	ddb.done()
}

func TestDeleteItemExpressionAttributValues(t *testing.T) {
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
	ddb.MockDelete(&dynamodb.DeleteItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"PK": {S: aws.String(fmt.Sprintf("Person#%s", want.Name))},
		},
		TableName:                 &tableName,
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{"foo": {S: aws.String("bar")}},
	})
	got := Person{
		Name: want.Name,
	}
	if err := client.DeleteItem(&got).TableName(tableName).ExpressionAttributeValue("foo", "bar").Exec(); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	ddb.done()
}

func TestDeleteItemDefaultTableName(t *testing.T) {
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
	ddb.MockDelete(&dynamodb.DeleteItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"PK": {S: aws.String(fmt.Sprintf("Person#%s", want.Name))},
		},
		TableName: &tableName,
	})
	got := Person{
		Name: want.Name,
	}
	if err := client.DeleteItem(&got).Exec(); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	ddb.done()
}
