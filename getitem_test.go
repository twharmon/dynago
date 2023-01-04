package dynago_test

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/twharmon/dynago"
)

func TestGetItemBasic(t *testing.T) {
	ddb := mock(t)
	client := dynago.New(ddb)
	type Person struct {
		*SimpleTable
		Name string `attr:"PK" fmt:"Person#{}"`
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
	if err := client.GetItem(&got).TableName(tableName).Exec(); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
	ddb.done()
}

func TestGetItemConsistentRead(t *testing.T) {
	ddb := mock(t)
	client := dynago.New(ddb)
	type Person struct {
		*CompositeTable
		Name string `attr:"PK" fmt:"Person#{}"`
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
	if err := client.GetItem(&got).TableName(tableName).ConsistentRead(true).Exec(); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
	ddb.done()
}

func TestGetItemProjectionExpression(t *testing.T) {
	ddb := mock(t)
	client := dynago.New(ddb)
	type Person struct {
		*CompositeTable
		Name string `attr:"PK" fmt:"Person#{}"`
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
	if err := client.GetItem(&got).TableName(tableName).ProjectionExpression("foo").Exec(); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
	ddb.done()
}

func TestGetItemExpressionAttributeNames(t *testing.T) {
	ddb := mock(t)
	client := dynago.New(ddb)
	type Person struct {
		*CompositeTable
		Name string `attr:"PK" fmt:"Person#{}"`
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
	if err := client.GetItem(&got).TableName(tableName).ExpressionAttributeName("foo", "#f").Exec(); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
	ddb.done()
}

func TestGetItemCopy(t *testing.T) {
	ddb := mock(t)
	client := dynago.New(ddb)
	type Person struct {
		*CompositeTable
		Name string `attr:"PK" fmt:"Person#{}" copy:"SK"`
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
	if err := client.GetItem(&got).TableName(tableName).Exec(); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
	ddb.done()
}

func TestGetItemCopyIdx(t *testing.T) {
	ddb := mock(t)
	client := dynago.New(ddb)
	type Person struct {
		*CompositeTable
		ID   string `attr:"PK" fmt:"Person#{}" copy:"GSISK"`
		Name string `attr:"SK" fmt:"Name#{}" copy:"GSIPK"`
		Age  int64
	}
	want := Person{
		ID:   "bar",
		Name: "foo",
		Age:  33,
	}
	tableName := "bar"
	ddb.MockGet(&dynamodb.GetItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"PK": {S: aws.String(fmt.Sprintf("Person#%s", want.ID))},
			"SK": {S: aws.String(fmt.Sprintf("Name#%s", want.Name))},
		},
		TableName:      &tableName,
		ConsistentRead: aws.Bool(false),
	}, &dynamodb.GetItemOutput{
		Item: map[string]*dynamodb.AttributeValue{
			"PK":  {S: aws.String(fmt.Sprintf("Person#%s", want.ID))},
			"SK":  {S: aws.String(fmt.Sprintf("Name#%s", want.Name))},
			"Age": {N: aws.String(strconv.FormatInt(want.Age, 10))},
		},
	})
	got := Person{
		ID:   want.ID,
		Name: want.Name,
	}
	if err := client.GetItem(&got).TableName(tableName).Exec(); err != nil {
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
		*CompositeTable
		Name string `attr:"PK" fmt:"Person#{}"`
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
	if err := client.GetItem(&got).Exec(); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
	ddb.done()
}

func TestGetItemWithMultilineField(t *testing.T) {
	ddb := mock(t)
	client := dynago.New(ddb)
	type Person struct {
		*CompositeTable
		Name string `attr:"PK" fmt:"Person#{}" copy:"SK"`
		Text string
	}
	text := "foo\nbar"
	want := Person{
		Name: "foo",
		Text: text,
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
			"PK":   {S: aws.String(fmt.Sprintf("Person#%s", want.Name))},
			"SK":   {S: aws.String(fmt.Sprintf("Person#%s", want.Name))},
			"Text": {S: &text},
		},
	})
	got := Person{
		Name: want.Name,
	}
	if err := client.GetItem(&got).TableName(tableName).Exec(); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
	ddb.done()
}

func TestGetItemDuration(t *testing.T) {
	ddb := mock(t)
	client := dynago.New(ddb)
	type Person struct {
		*CompositeTable
		Name string `attr:"PK" fmt:"Person#{}"`
		Age  time.Duration
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
			"Age": {N: aws.String(strconv.FormatInt(int64(want.Age), 10))},
		},
	})
	got := Person{
		Name: want.Name,
	}
	if err := client.GetItem(&got).TableName(tableName).Exec(); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
	ddb.done()
}
