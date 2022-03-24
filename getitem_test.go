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
	if err := client.Get(&got).TableName(tableName).Exec(); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
	ddb.done()
}

func TestGetItemConsistentRead(t *testing.T) {
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
		ConsistentRead: aws.Bool(true),
	}, &dynamodb.GetItemOutput{
		Item: map[string]*dynamodb.AttributeValue{
			"PK":  {S: aws.String(fmt.Sprintf("Person#%s", want.Name))},
			"Age": {N: aws.String(strconv.FormatInt(want.Age, 10))},
		},
	})
	got := Person{
		Name: want.Name,
	}
	if err := client.Get(&got).TableName(tableName).ConsistentRead(true).Exec(); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
	ddb.done()
}

func TestGetItemProjectionExpression(t *testing.T) {
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
		TableName:            &tableName,
		ConsistentRead:       aws.Bool(false),
		ProjectionExpression: aws.String("foo"),
	}, &dynamodb.GetItemOutput{
		Item: map[string]*dynamodb.AttributeValue{
			"PK":  {S: aws.String(fmt.Sprintf("Person#%s", want.Name))},
			"Age": {N: aws.String(strconv.FormatInt(want.Age, 10))},
		},
	})
	got := Person{
		Name: want.Name,
	}
	if err := client.Get(&got).TableName(tableName).ProjectionExpression("foo").Exec(); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
	ddb.done()
}

func TestGetItemExpressionAttributeNames(t *testing.T) {
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
		ExpressionAttributeNames: map[string]*string{
			"foo": aws.String("#f"),
		},
	}, &dynamodb.GetItemOutput{
		Item: map[string]*dynamodb.AttributeValue{
			"PK":  {S: aws.String(fmt.Sprintf("Person#%s", want.Name))},
			"Age": {N: aws.String(strconv.FormatInt(want.Age, 10))},
		},
	})
	got := Person{
		Name: want.Name,
	}
	if err := client.Get(&got).TableName(tableName).ExpressionAttributeName("foo", "#f").Exec(); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
	ddb.done()
}

func TestGetItemCopy(t *testing.T) {
	ddb := mock(t)
	client := dynago.New(ddb)
	type Person struct {
		Name string `idx:"primary" attr:"PK" fmt:"Person#{}" copyidx:"SK"`
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
			"SK": {S: aws.String(fmt.Sprintf("Person#%s", want.Name))},
		},
		TableName:      &tableName,
		ConsistentRead: aws.Bool(false),
	}, &dynamodb.GetItemOutput{
		Item: map[string]*dynamodb.AttributeValue{
			"PK":  {S: aws.String(fmt.Sprintf("Person#%s", want.Name))},
			"SK":  {S: aws.String(fmt.Sprintf("Person#%s", want.Name))},
			"Age": {N: aws.String(strconv.FormatInt(want.Age, 10))},
		},
	})
	got := Person{
		Name: want.Name,
	}
	if err := client.Get(&got).TableName(tableName).Exec(); err != nil {
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
