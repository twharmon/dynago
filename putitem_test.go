package dynago_test

import (
	"fmt"
	"strconv"
	"testing"

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
	ddb.MockPutItem(&dynamodb.PutItemInput{
		Item: map[string]*dynamodb.AttributeValue{
			"PK":  {S: aws.String(fmt.Sprintf("Person#%s", p.Name))},
			"Age": {N: aws.String(strconv.FormatInt(p.Age, 10))},
		},
		TableName: &tableName,
	})
	if err := client.PutItem(tableName).Exec(&p); err != nil {
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
	ddb.MockPutItem(&dynamodb.PutItemInput{
		Item: map[string]*dynamodb.AttributeValue{
			"PK":  {S: aws.String(fmt.Sprintf("Person#%s", p.Name))},
			"Age": {N: aws.String(strconv.FormatInt(p.Age, 10))},
		},
		TableName: &tableName,
	})
	if err := client.PutItem().Exec(&p); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	ddb.done()
}
