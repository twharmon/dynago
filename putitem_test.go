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

func TestPutItemBasic(t *testing.T) {
	ddb := mock(t)
	client := dynago.New(ddb)
	type Person struct {
		Name string `pk:"primary" attr:"PK" fmt:"Person#{}"`
		Age  int64
	}
	tableName := "bar"
	p := Person{
		Name: "foo",
		Age:  33,
	}
	ddb.MockPut(&dynamodb.PutItemInput{
		Item: map[string]*dynamodb.AttributeValue{
			"PK":  {S: aws.String(fmt.Sprintf("Person#%s", p.Name))},
			"Age": {N: aws.String(strconv.FormatInt(p.Age, 10))},
		},
		TableName: &tableName,
	})
	if err := client.PutItem(&p).TableName(tableName).Exec(); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	ddb.done()
}

func TestPutItemExpAttrVals(t *testing.T) {
	ddb := mock(t)
	client := dynago.New(ddb)
	type Person struct {
		Name string `pk:"primary" attr:"PK" fmt:"Person#{}"`
		Age  int64
	}
	tableName := "bar"
	p := Person{
		Name: "foo",
		Age:  33,
	}
	ddb.MockPut(&dynamodb.PutItemInput{
		Item: map[string]*dynamodb.AttributeValue{
			"PK":  {S: aws.String(fmt.Sprintf("Person#%s", p.Name))},
			"Age": {N: aws.String(strconv.FormatInt(p.Age, 10))},
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{"foo": {S: aws.String("bar")}},
		TableName:                 &tableName,
	})
	if err := client.PutItem(&p).TableName(tableName).ExpressionAttributeValue("foo", "bar").Exec(); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	ddb.done()
}

func TestPutItemCondExp(t *testing.T) {
	ddb := mock(t)
	client := dynago.New(ddb)
	type Person struct {
		Name string `pk:"primary" attr:"PK" fmt:"Person#{}"`
		Age  int64
	}
	tableName := "bar"
	p := Person{
		Name: "foo",
		Age:  33,
	}
	ddb.MockPut(&dynamodb.PutItemInput{
		Item: map[string]*dynamodb.AttributeValue{
			"PK":  {S: aws.String(fmt.Sprintf("Person#%s", p.Name))},
			"Age": {N: aws.String(strconv.FormatInt(p.Age, 10))},
		},
		ConditionExpression: aws.String("foo"),
		TableName:           &tableName,
	})
	if err := client.PutItem(&p).TableName(tableName).ConditionExpression("foo").Exec(); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	ddb.done()
}

func TestPutItemExpAttrNames(t *testing.T) {
	ddb := mock(t)
	client := dynago.New(ddb)
	type Person struct {
		Name string `pk:"primary" attr:"PK" fmt:"Person#{}"`
		Age  int64
	}
	tableName := "bar"
	p := Person{
		Name: "foo",
		Age:  33,
	}
	ddb.MockPut(&dynamodb.PutItemInput{
		Item: map[string]*dynamodb.AttributeValue{
			"PK":  {S: aws.String(fmt.Sprintf("Person#%s", p.Name))},
			"Age": {N: aws.String(strconv.FormatInt(p.Age, 10))},
		},
		ExpressionAttributeNames: map[string]*string{"foo": aws.String("#f")},
		TableName:                &tableName,
	})
	if err := client.PutItem(&p).TableName(tableName).ExpressionAttributeName("foo", "#f").Exec(); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	ddb.done()
}

func TestPutItemDefaultTableName(t *testing.T) {
	ddb := mock(t)
	tableName := "bar"
	client := dynago.New(ddb, &dynago.Config{DefaultTableName: tableName})
	type Person struct {
		Name string `pk:"primary" attr:"PK" fmt:"Person#{}"`
		Age  int64
	}
	p := Person{
		Name: "foo",
		Age:  33,
	}
	ddb.MockPut(&dynamodb.PutItemInput{
		Item: map[string]*dynamodb.AttributeValue{
			"PK":  {S: aws.String(fmt.Sprintf("Person#%s", p.Name))},
			"Age": {N: aws.String(strconv.FormatInt(p.Age, 10))},
		},
		TableName: &tableName,
	})
	if err := client.PutItem(&p).Exec(); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	ddb.done()
}

func TestPutItemDuration(t *testing.T) {
	ddb := mock(t)
	client := dynago.New(ddb)
	type Person struct {
		Name string `pk:"primary" attr:"PK" fmt:"Person#{}"`
		Age  time.Duration
	}
	tableName := "bar"
	p := Person{
		Name: "foo",
		Age:  33,
	}
	ddb.MockPut(&dynamodb.PutItemInput{
		Item: map[string]*dynamodb.AttributeValue{
			"PK":  {S: aws.String(fmt.Sprintf("Person#%s", p.Name))},
			"Age": {N: aws.String(strconv.FormatInt(int64(p.Age), 10))},
		},
		TableName: &tableName,
	})
	if err := client.PutItem(&p).TableName(tableName).Exec(); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	ddb.done()
}
