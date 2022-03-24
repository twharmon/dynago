package dynago_test

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/twharmon/dynago"
)

func TestQueryBasic(t *testing.T) {
	ddb := mock(t)
	client := dynago.New(ddb)
	type Person struct {
		Name string `idx:"primary" attr:"PK" fmt:"Person#{}"`
		Team string `idx:"primary" fmt:"Team#{}"`
		Age  int64
	}
	want := []*Person{{
		Name: "foo",
		Team: "bar",
		Age:  33,
	}}
	tableName := "baz"
	ddb.MockQuery(&dynamodb.QueryInput{
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":pk": {S: aws.String(fmt.Sprintf("Person#%s", want[0].Name))},
			":bw": {S: aws.String("Team#")},
		},
		KeyConditionExpression: aws.String("PK = :pk and begins_with(SK, :bw)"),
		TableName:              &tableName,
		ConsistentRead:         aws.Bool(false),
	}, &dynamodb.QueryOutput{
		Items: []map[string]*dynamodb.AttributeValue{
			{
				"PK":   {S: aws.String(fmt.Sprintf("Person#%s", want[0].Name))},
				"Team": {S: aws.String(fmt.Sprintf("Team#%s", want[0].Team))},
				"Age":  {N: aws.String(strconv.FormatInt(want[0].Age, 10))},
			},
		},
	})
	var got []*Person
	if err := client.Query(&got).
		Table(tableName).
		ExpressionAttributeValue(":pk", fmt.Sprintf("Person#%s", want[0].Name)).
		ExpressionAttributeValue(":bw", "Team#").
		KeyConditionExpression("PK = :pk and begins_with(SK, :bw)").
		Exec(); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
	ddb.done()
}

func TestQueryDefaultTableName(t *testing.T) {
	ddb := mock(t)
	tableName := "baz"
	client := dynago.New(ddb, &dynago.Config{DefaultTableName: tableName})
	type Person struct {
		Name string `idx:"primary" attr:"PK" fmt:"Person#{}"`
		Team string `idx:"primary" fmt:"Team#{}"`
		Age  int64
	}
	want := []*Person{{
		Name: "foo",
		Team: "bar",
		Age:  33,
	}}
	ddb.MockQuery(&dynamodb.QueryInput{
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":pk": {S: aws.String(fmt.Sprintf("Person#%s", want[0].Name))},
			":bw": {S: aws.String("Team#")},
		},
		KeyConditionExpression: aws.String("PK = :pk and begins_with(SK, :bw)"),
		TableName:              &tableName,
		ConsistentRead:         aws.Bool(false),
	}, &dynamodb.QueryOutput{
		Items: []map[string]*dynamodb.AttributeValue{
			{
				"PK":   {S: aws.String(fmt.Sprintf("Person#%s", want[0].Name))},
				"Team": {S: aws.String(fmt.Sprintf("Team#%s", want[0].Team))},
				"Age":  {N: aws.String(strconv.FormatInt(want[0].Age, 10))},
			},
		},
	})
	var got []*Person
	if err := client.Query(&got).
		ExpressionAttributeValue(":pk", fmt.Sprintf("Person#%s", want[0].Name)).
		ExpressionAttributeValue(":bw", "Team#").
		KeyConditionExpression("PK = :pk and begins_with(SK, :bw)").
		Exec(); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
	ddb.done()
}

func TestQueryValues(t *testing.T) {
	ddb := mock(t)
	client := dynago.New(ddb)
	type Person struct {
		Name string `idx:"primary" attr:"PK" fmt:"Person#{}"`
		Team string `idx:"primary" fmt:"Team#{}"`
		Age  int64
	}
	want := []Person{{
		Name: "foo",
		Team: "bar",
		Age:  33,
	}}
	tableName := "baz"
	ddb.MockQuery(&dynamodb.QueryInput{
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":pk": {S: aws.String(fmt.Sprintf("Person#%s", want[0].Name))},
			":bw": {S: aws.String("Team#")},
		},
		KeyConditionExpression: aws.String("PK = :pk and begins_with(SK, :bw)"),
		TableName:              &tableName,
		ConsistentRead:         aws.Bool(false),
	}, &dynamodb.QueryOutput{
		Items: []map[string]*dynamodb.AttributeValue{
			{
				"PK":   {S: aws.String(fmt.Sprintf("Person#%s", want[0].Name))},
				"Team": {S: aws.String(fmt.Sprintf("Team#%s", want[0].Team))},
				"Age":  {N: aws.String(strconv.FormatInt(want[0].Age, 10))},
			},
		},
	})
	var got []Person
	if err := client.Query(&got).
		Table(tableName).
		ExpressionAttributeValue(":pk", fmt.Sprintf("Person#%s", want[0].Name)).
		ExpressionAttributeValue(":bw", "Team#").
		KeyConditionExpression("PK = :pk and begins_with(SK, :bw)").
		Exec(); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
	ddb.done()
}

func TestExpInt(t *testing.T) {
	ddb := mock(t)
	client := dynago.New(ddb)
	type Person struct {
		Name string `idx:"primary" attr:"PK" fmt:"Person#{}"`
		Age  int    `idx:"primary" attr:"SK"`
	}
	want := []*Person{{
		Name: "foo",
		Age:  33,
	}}
	tableName := "baz"
	ddb.MockQuery(&dynamodb.QueryInput{
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":pk":  {S: aws.String(fmt.Sprintf("Person#%s", want[0].Name))},
			":age": {N: aws.String(strconv.FormatInt(int64(want[0].Age), 10))},
		},
		KeyConditionExpression: aws.String("PK = :pk and SK > :age"),
		TableName:              &tableName,
		ConsistentRead:         aws.Bool(false),
	}, &dynamodb.QueryOutput{
		Items: []map[string]*dynamodb.AttributeValue{
			{
				"PK": {S: aws.String(fmt.Sprintf("Person#%s", want[0].Name))},
				"SK": {N: aws.String(strconv.FormatInt(int64(want[0].Age), 10))},
			},
		},
	})
	var got []*Person
	if err := client.Query(&got).
		Table(tableName).
		ExpressionAttributeValue(":pk", fmt.Sprintf("Person#%s", want[0].Name)).
		ExpressionAttributeValue(":age", want[0].Age).
		KeyConditionExpression("PK = :pk and SK > :age").
		Exec(); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
	ddb.done()
}

func TestExpUint(t *testing.T) {
	ddb := mock(t)
	client := dynago.New(ddb)
	type Person struct {
		Name string `idx:"primary" attr:"PK" fmt:"Person#{}"`
		Age  uint   `idx:"primary" attr:"SK"`
	}
	want := []*Person{{
		Name: "foo",
		Age:  33,
	}}
	tableName := "baz"
	ddb.MockQuery(&dynamodb.QueryInput{
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":pk":  {S: aws.String(fmt.Sprintf("Person#%s", want[0].Name))},
			":age": {N: aws.String(strconv.FormatInt(int64(want[0].Age), 10))},
		},
		KeyConditionExpression: aws.String("PK = :pk and SK > :age"),
		TableName:              &tableName,
		ConsistentRead:         aws.Bool(false),
	}, &dynamodb.QueryOutput{
		Items: []map[string]*dynamodb.AttributeValue{
			{
				"PK": {S: aws.String(fmt.Sprintf("Person#%s", want[0].Name))},
				"SK": {N: aws.String(strconv.FormatInt(int64(want[0].Age), 10))},
			},
		},
	})
	var got []*Person
	if err := client.Query(&got).
		Table(tableName).
		ExpressionAttributeValue(":pk", fmt.Sprintf("Person#%s", want[0].Name)).
		ExpressionAttributeValue(":age", want[0].Age).
		KeyConditionExpression("PK = :pk and SK > :age").
		Exec(); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
	ddb.done()
}

func TestExpInt8(t *testing.T) {
	ddb := mock(t)
	client := dynago.New(ddb)
	type Person struct {
		Name string `idx:"primary" attr:"PK" fmt:"Person#{}"`
		Age  int8   `idx:"primary" attr:"SK"`
	}
	want := []*Person{{
		Name: "foo",
		Age:  33,
	}}
	tableName := "baz"
	ddb.MockQuery(&dynamodb.QueryInput{
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":pk":  {S: aws.String(fmt.Sprintf("Person#%s", want[0].Name))},
			":age": {N: aws.String(strconv.FormatInt(int64(want[0].Age), 10))},
		},
		KeyConditionExpression: aws.String("PK = :pk and SK > :age"),
		TableName:              &tableName,
		ConsistentRead:         aws.Bool(false),
	}, &dynamodb.QueryOutput{
		Items: []map[string]*dynamodb.AttributeValue{
			{
				"PK": {S: aws.String(fmt.Sprintf("Person#%s", want[0].Name))},
				"SK": {N: aws.String(strconv.FormatInt(int64(want[0].Age), 10))},
			},
		},
	})
	var got []*Person
	if err := client.Query(&got).
		Table(tableName).
		ExpressionAttributeValue(":pk", fmt.Sprintf("Person#%s", want[0].Name)).
		ExpressionAttributeValue(":age", want[0].Age).
		KeyConditionExpression("PK = :pk and SK > :age").
		Exec(); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
	ddb.done()
}

func TestExpUint8(t *testing.T) {
	ddb := mock(t)
	client := dynago.New(ddb)
	type Person struct {
		Name string `idx:"primary" attr:"PK" fmt:"Person#{}"`
		Age  uint8  `idx:"primary" attr:"SK"`
	}
	want := []*Person{{
		Name: "foo",
		Age:  33,
	}}
	tableName := "baz"
	ddb.MockQuery(&dynamodb.QueryInput{
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":pk":  {S: aws.String(fmt.Sprintf("Person#%s", want[0].Name))},
			":age": {N: aws.String(strconv.FormatInt(int64(want[0].Age), 10))},
		},
		KeyConditionExpression: aws.String("PK = :pk and SK > :age"),
		TableName:              &tableName,
		ConsistentRead:         aws.Bool(false),
	}, &dynamodb.QueryOutput{
		Items: []map[string]*dynamodb.AttributeValue{
			{
				"PK": {S: aws.String(fmt.Sprintf("Person#%s", want[0].Name))},
				"SK": {N: aws.String(strconv.FormatInt(int64(want[0].Age), 10))},
			},
		},
	})
	var got []*Person
	if err := client.Query(&got).
		Table(tableName).
		ExpressionAttributeValue(":pk", fmt.Sprintf("Person#%s", want[0].Name)).
		ExpressionAttributeValue(":age", want[0].Age).
		KeyConditionExpression("PK = :pk and SK > :age").
		Exec(); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
	ddb.done()
}

func TestExpInt16(t *testing.T) {
	ddb := mock(t)
	client := dynago.New(ddb)
	type Person struct {
		Name string `idx:"primary" attr:"PK" fmt:"Person#{}"`
		Age  int16  `idx:"primary" attr:"SK"`
	}
	want := []*Person{{
		Name: "foo",
		Age:  33,
	}}
	tableName := "baz"
	ddb.MockQuery(&dynamodb.QueryInput{
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":pk":  {S: aws.String(fmt.Sprintf("Person#%s", want[0].Name))},
			":age": {N: aws.String(strconv.FormatInt(int64(want[0].Age), 10))},
		},
		KeyConditionExpression: aws.String("PK = :pk and SK > :age"),
		TableName:              &tableName,
		ConsistentRead:         aws.Bool(false),
	}, &dynamodb.QueryOutput{
		Items: []map[string]*dynamodb.AttributeValue{
			{
				"PK": {S: aws.String(fmt.Sprintf("Person#%s", want[0].Name))},
				"SK": {N: aws.String(strconv.FormatInt(int64(want[0].Age), 10))},
			},
		},
	})
	var got []*Person
	if err := client.Query(&got).
		Table(tableName).
		ExpressionAttributeValue(":pk", fmt.Sprintf("Person#%s", want[0].Name)).
		ExpressionAttributeValue(":age", want[0].Age).
		KeyConditionExpression("PK = :pk and SK > :age").
		Exec(); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
	ddb.done()
}

func TestExpUint16(t *testing.T) {
	ddb := mock(t)
	client := dynago.New(ddb)
	type Person struct {
		Name string `idx:"primary" attr:"PK" fmt:"Person#{}"`
		Age  uint16 `idx:"primary" attr:"SK"`
	}
	want := []*Person{{
		Name: "foo",
		Age:  33,
	}}
	tableName := "baz"
	ddb.MockQuery(&dynamodb.QueryInput{
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":pk":  {S: aws.String(fmt.Sprintf("Person#%s", want[0].Name))},
			":age": {N: aws.String(strconv.FormatInt(int64(want[0].Age), 10))},
		},
		KeyConditionExpression: aws.String("PK = :pk and SK > :age"),
		TableName:              &tableName,
		ConsistentRead:         aws.Bool(false),
	}, &dynamodb.QueryOutput{
		Items: []map[string]*dynamodb.AttributeValue{
			{
				"PK": {S: aws.String(fmt.Sprintf("Person#%s", want[0].Name))},
				"SK": {N: aws.String(strconv.FormatInt(int64(want[0].Age), 10))},
			},
		},
	})
	var got []*Person
	if err := client.Query(&got).
		Table(tableName).
		ExpressionAttributeValue(":pk", fmt.Sprintf("Person#%s", want[0].Name)).
		ExpressionAttributeValue(":age", want[0].Age).
		KeyConditionExpression("PK = :pk and SK > :age").
		Exec(); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
	ddb.done()
}

func TestExpInt32(t *testing.T) {
	ddb := mock(t)
	client := dynago.New(ddb)
	type Person struct {
		Name string `idx:"primary" attr:"PK" fmt:"Person#{}"`
		Age  int32  `idx:"primary" attr:"SK"`
	}
	want := []*Person{{
		Name: "foo",
		Age:  33,
	}}
	tableName := "baz"
	ddb.MockQuery(&dynamodb.QueryInput{
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":pk":  {S: aws.String(fmt.Sprintf("Person#%s", want[0].Name))},
			":age": {N: aws.String(strconv.FormatInt(int64(want[0].Age), 10))},
		},
		KeyConditionExpression: aws.String("PK = :pk and SK > :age"),
		TableName:              &tableName,
		ConsistentRead:         aws.Bool(false),
	}, &dynamodb.QueryOutput{
		Items: []map[string]*dynamodb.AttributeValue{
			{
				"PK": {S: aws.String(fmt.Sprintf("Person#%s", want[0].Name))},
				"SK": {N: aws.String(strconv.FormatInt(int64(want[0].Age), 10))},
			},
		},
	})
	var got []*Person
	if err := client.Query(&got).
		Table(tableName).
		ExpressionAttributeValue(":pk", fmt.Sprintf("Person#%s", want[0].Name)).
		ExpressionAttributeValue(":age", want[0].Age).
		KeyConditionExpression("PK = :pk and SK > :age").
		Exec(); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
	ddb.done()
}

func TestExpUint32(t *testing.T) {
	ddb := mock(t)
	client := dynago.New(ddb)
	type Person struct {
		Name string `idx:"primary" attr:"PK" fmt:"Person#{}"`
		Age  uint32 `idx:"primary" attr:"SK"`
	}
	want := []*Person{{
		Name: "foo",
		Age:  33,
	}}
	tableName := "baz"
	ddb.MockQuery(&dynamodb.QueryInput{
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":pk":  {S: aws.String(fmt.Sprintf("Person#%s", want[0].Name))},
			":age": {N: aws.String(strconv.FormatInt(int64(want[0].Age), 10))},
		},
		KeyConditionExpression: aws.String("PK = :pk and SK > :age"),
		TableName:              &tableName,
		ConsistentRead:         aws.Bool(false),
	}, &dynamodb.QueryOutput{
		Items: []map[string]*dynamodb.AttributeValue{
			{
				"PK": {S: aws.String(fmt.Sprintf("Person#%s", want[0].Name))},
				"SK": {N: aws.String(strconv.FormatInt(int64(want[0].Age), 10))},
			},
		},
	})
	var got []*Person
	if err := client.Query(&got).
		Table(tableName).
		ExpressionAttributeValue(":pk", fmt.Sprintf("Person#%s", want[0].Name)).
		ExpressionAttributeValue(":age", want[0].Age).
		KeyConditionExpression("PK = :pk and SK > :age").
		Exec(); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
	ddb.done()
}

func TestExpInt64(t *testing.T) {
	ddb := mock(t)
	client := dynago.New(ddb)
	type Person struct {
		Name string `idx:"primary" attr:"PK" fmt:"Person#{}"`
		Age  int64  `idx:"primary" attr:"SK"`
	}
	want := []*Person{{
		Name: "foo",
		Age:  33,
	}}
	tableName := "baz"
	ddb.MockQuery(&dynamodb.QueryInput{
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":pk":  {S: aws.String(fmt.Sprintf("Person#%s", want[0].Name))},
			":age": {N: aws.String(strconv.FormatInt(int64(want[0].Age), 10))},
		},
		KeyConditionExpression: aws.String("PK = :pk and SK > :age"),
		TableName:              &tableName,
		ConsistentRead:         aws.Bool(false),
	}, &dynamodb.QueryOutput{
		Items: []map[string]*dynamodb.AttributeValue{
			{
				"PK": {S: aws.String(fmt.Sprintf("Person#%s", want[0].Name))},
				"SK": {N: aws.String(strconv.FormatInt(int64(want[0].Age), 10))},
			},
		},
	})
	var got []*Person
	if err := client.Query(&got).
		Table(tableName).
		ExpressionAttributeValue(":pk", fmt.Sprintf("Person#%s", want[0].Name)).
		ExpressionAttributeValue(":age", want[0].Age).
		KeyConditionExpression("PK = :pk and SK > :age").
		Exec(); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
	ddb.done()
}

func TestExpUint64(t *testing.T) {
	ddb := mock(t)
	client := dynago.New(ddb)
	type Person struct {
		Name string `idx:"primary" attr:"PK" fmt:"Person#{}"`
		Age  uint64 `idx:"primary" attr:"SK"`
	}
	want := []*Person{{
		Name: "foo",
		Age:  33,
	}}
	tableName := "baz"
	ddb.MockQuery(&dynamodb.QueryInput{
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":pk":  {S: aws.String(fmt.Sprintf("Person#%s", want[0].Name))},
			":age": {N: aws.String(strconv.FormatInt(int64(want[0].Age), 10))},
		},
		KeyConditionExpression: aws.String("PK = :pk and SK > :age"),
		TableName:              &tableName,
		ConsistentRead:         aws.Bool(false),
	}, &dynamodb.QueryOutput{
		Items: []map[string]*dynamodb.AttributeValue{
			{
				"PK": {S: aws.String(fmt.Sprintf("Person#%s", want[0].Name))},
				"SK": {N: aws.String(strconv.FormatInt(int64(want[0].Age), 10))},
			},
		},
	})
	var got []*Person
	if err := client.Query(&got).
		Table(tableName).
		ExpressionAttributeValue(":pk", fmt.Sprintf("Person#%s", want[0].Name)).
		ExpressionAttributeValue(":age", want[0].Age).
		KeyConditionExpression("PK = :pk and SK > :age").
		Exec(); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
	ddb.done()
}

func TestExpFloat32(t *testing.T) {
	ddb := mock(t)
	client := dynago.New(ddb)
	type Person struct {
		Name string  `idx:"primary" attr:"PK" fmt:"Person#{}"`
		Age  float32 `idx:"primary" attr:"SK"`
	}
	want := []*Person{{
		Name: "foo",
		Age:  33,
	}}
	tableName := "baz"
	ddb.MockQuery(&dynamodb.QueryInput{
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":pk":  {S: aws.String(fmt.Sprintf("Person#%s", want[0].Name))},
			":age": {N: aws.String(strconv.FormatFloat(float64(want[0].Age), 'f', -1, 32))},
		},
		KeyConditionExpression: aws.String("PK = :pk and SK > :age"),
		TableName:              &tableName,
		ConsistentRead:         aws.Bool(false),
	}, &dynamodb.QueryOutput{
		Items: []map[string]*dynamodb.AttributeValue{
			{
				"PK": {S: aws.String(fmt.Sprintf("Person#%s", want[0].Name))},
				"SK": {N: aws.String(strconv.FormatFloat(float64(want[0].Age), 'f', -1, 32))},
			},
		},
	})
	var got []*Person
	if err := client.Query(&got).
		Table(tableName).
		ExpressionAttributeValue(":pk", fmt.Sprintf("Person#%s", want[0].Name)).
		ExpressionAttributeValue(":age", want[0].Age).
		KeyConditionExpression("PK = :pk and SK > :age").
		Exec(); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
	ddb.done()
}

func TestExpFloat64(t *testing.T) {
	ddb := mock(t)
	client := dynago.New(ddb)
	type Person struct {
		Name string  `idx:"primary" attr:"PK" fmt:"Person#{}"`
		Age  float64 `idx:"primary" attr:"SK"`
	}
	want := []*Person{{
		Name: "foo",
		Age:  33,
	}}
	tableName := "baz"
	ddb.MockQuery(&dynamodb.QueryInput{
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":pk":  {S: aws.String(fmt.Sprintf("Person#%s", want[0].Name))},
			":age": {N: aws.String(strconv.FormatFloat(float64(want[0].Age), 'f', -1, 64))},
		},
		KeyConditionExpression: aws.String("PK = :pk and SK > :age"),
		TableName:              &tableName,
		ConsistentRead:         aws.Bool(false),
	}, &dynamodb.QueryOutput{
		Items: []map[string]*dynamodb.AttributeValue{
			{
				"PK": {S: aws.String(fmt.Sprintf("Person#%s", want[0].Name))},
				"SK": {N: aws.String(strconv.FormatFloat(float64(want[0].Age), 'f', -1, 64))},
			},
		},
	})
	var got []*Person
	if err := client.Query(&got).
		Table(tableName).
		ExpressionAttributeValue(":pk", fmt.Sprintf("Person#%s", want[0].Name)).
		ExpressionAttributeValue(":age", want[0].Age).
		KeyConditionExpression("PK = :pk and SK > :age").
		Exec(); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
	ddb.done()
}
