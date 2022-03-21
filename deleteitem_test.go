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
	ddb.MockDeleteItem(&dynamodb.DeleteItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"PK": {S: aws.String(fmt.Sprintf("Person#%s", want.Name))},
		},
		TableName: &tableName,
	})
	got := Person{
		Name: want.Name,
	}
	if err := client.DeleteItem(tableName).Exec(&got); err != nil {
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
	ddb.MockDeleteItem(&dynamodb.DeleteItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"PK": {S: aws.String(fmt.Sprintf("Person#%s", want.Name))},
		},
		TableName: &tableName,
	})
	got := Person{
		Name: want.Name,
	}
	if err := client.DeleteItem().Exec(&got); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	ddb.done()
}