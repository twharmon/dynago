package dynago_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/twharmon/dynago"
)

func TestUpdateItemBasic(t *testing.T) {
	ddb := mock(t)
	client := dynago.New(ddb)
	type Person struct {
		*CompositeTable
		Name string `pk:"primary" attr:"PK" fmt:"Person#{}" idx:"primary"`
		Age  int64
	}
	tableName := "bar"
	p := Person{
		Name: "foo",
		Age:  33,
	}
	ddb.MockUpdate(&dynamodb.UpdateItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"PK": {S: aws.String(fmt.Sprintf("Person#%s", p.Name))},
		},
		UpdateExpression: aws.String("foo"),
		TableName:        &tableName,
	})
	if err := client.UpdateItem(&p).UpdateExpression("foo").TableName(tableName).Exec(); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	ddb.done()
}

func TestUpdateItemExpAttrVals(t *testing.T) {
	ddb := mock(t)
	client := dynago.New(ddb)
	type Person struct {
		*CompositeTable
		Name string `pk:"primary" attr:"PK" fmt:"Person#{}" idx:"primary"`
		Age  int64
	}
	tableName := "bar"
	p := Person{
		Name: "foo",
		Age:  33,
	}
	ddb.MockUpdate(&dynamodb.UpdateItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"PK": {S: aws.String(fmt.Sprintf("Person#%s", p.Name))},
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{"foo": {S: aws.String("bar")}},
		TableName:                 &tableName,
	})
	if err := client.UpdateItem(&p).TableName(tableName).ExpressionAttributeValue("foo", "bar").Exec(); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	ddb.done()
}

func TestUpdateItemCondExp(t *testing.T) {
	ddb := mock(t)
	client := dynago.New(ddb)
	type Person struct {
		*CompositeTable
		Name string `pk:"primary" attr:"PK" fmt:"Person#{}" idx:"primary"`
		Age  int64
	}
	tableName := "bar"
	p := Person{
		Name: "foo",
		Age:  33,
	}
	ddb.MockUpdate(&dynamodb.UpdateItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"PK": {S: aws.String(fmt.Sprintf("Person#%s", p.Name))},
		},
		ConditionExpression: aws.String("foo"),
		TableName:           &tableName,
	})
	if err := client.UpdateItem(&p).TableName(tableName).ConditionExpression("foo").Exec(); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	ddb.done()
}

func TestUpdateItemExpAttrNames(t *testing.T) {
	ddb := mock(t)
	client := dynago.New(ddb)
	type Person struct {
		*CompositeTable
		Name string `pk:"primary" attr:"PK" fmt:"Person#{}" idx:"primary"`
		Age  int64
	}
	tableName := "bar"
	p := Person{
		Name: "foo",
		Age:  33,
	}
	ddb.MockUpdate(&dynamodb.UpdateItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"PK": {S: aws.String(fmt.Sprintf("Person#%s", p.Name))},
		},
		ExpressionAttributeNames: map[string]*string{"foo": aws.String("#f")},
		TableName:                &tableName,
	})
	if err := client.UpdateItem(&p).TableName(tableName).ExpressionAttributeName("foo", "#f").Exec(); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	ddb.done()
}

func TestUpdateItemUpExp(t *testing.T) {
	ddb := mock(t)
	client := dynago.New(ddb)
	type Person struct {
		*CompositeTable
		Name string `pk:"primary" attr:"PK" fmt:"Person#{}" idx:"primary"`
		Age  int64
	}
	tableName := "bar"
	p := Person{
		Name: "foo",
		Age:  33,
	}
	ddb.MockUpdate(&dynamodb.UpdateItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"PK": {S: aws.String(fmt.Sprintf("Person#%s", p.Name))},
		},
		UpdateExpression: aws.String("foo"),
		TableName:        &tableName,
	})
	if err := client.UpdateItem(&p).TableName(tableName).UpdateExpression("foo").Exec(); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	ddb.done()
}

func TestUpdateItemDefaultTableName(t *testing.T) {
	ddb := mock(t)
	tableName := "bar"
	client := dynago.New(ddb, &dynago.Config{DefaultTableName: tableName})
	type Person struct {
		*CompositeTable
		Name string `pk:"primary" attr:"PK" fmt:"Person#{}" idx:"primary"`
		Age  int64
	}
	p := Person{
		Name: "foo",
		Age:  33,
	}
	ddb.MockUpdate(&dynamodb.UpdateItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"PK": {S: aws.String(fmt.Sprintf("Person#%s", p.Name))},
		},
		TableName: &tableName,
	})
	if err := client.UpdateItem(&p).Exec(); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	ddb.done()
}
