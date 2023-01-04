package dynago_test

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/twharmon/dynago"
)

func TestScanBasic(t *testing.T) {
	ddb := mock(t)
	client := dynago.New(ddb)
	type Person struct {
		Name string `attr:"PK" fmt:"Person#{}"`
		Team string `fmt:"Team#{}"`
		Age  int64
	}
	want := []*Person{{
		Name: "foo",
		Team: "bar",
		Age:  33,
	}}
	tableName := "baz"
	ddb.MockScan(&dynamodb.ScanInput{
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":pk": {S: aws.String(fmt.Sprintf("Person#%s", want[0].Name))},
			":bw": {S: aws.String("Team#")},
		},
		TableName:      &tableName,
		ConsistentRead: aws.Bool(false),
	}, &dynamodb.ScanOutput{
		Items: []map[string]*dynamodb.AttributeValue{
			{
				"PK":   {S: aws.String(fmt.Sprintf("Person#%s", want[0].Name))},
				"Team": {S: aws.String(fmt.Sprintf("Team#%s", want[0].Team))},
				"Age":  {N: aws.String(strconv.FormatInt(want[0].Age, 10))},
			},
		},
	})
	var got []*Person
	if err := client.Scan(&got).
		TableName(tableName).
		ExpressionAttributeValue(":pk", fmt.Sprintf("Person#%s", want[0].Name)).
		ExpressionAttributeValue(":bw", "Team#").
		Exec(); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
	ddb.done()
}

func TestScanSegment(t *testing.T) {
	ddb := mock(t)
	client := dynago.New(ddb)
	type Person struct {
		Name string `attr:"PK" fmt:"Person#{}"`
		Team string `fmt:"Team#{}"`
		Age  int64
	}
	want := []*Person{{
		Name: "foo",
		Team: "bar",
		Age:  33,
	}}
	tableName := "baz"
	ddb.MockScan(&dynamodb.ScanInput{
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":pk": {S: aws.String(fmt.Sprintf("Person#%s", want[0].Name))},
			":bw": {S: aws.String("Team#")},
		},
		TableName:      &tableName,
		Segment:        aws.Int64(5),
		ConsistentRead: aws.Bool(false),
	}, &dynamodb.ScanOutput{
		Items: []map[string]*dynamodb.AttributeValue{
			{
				"PK":   {S: aws.String(fmt.Sprintf("Person#%s", want[0].Name))},
				"Team": {S: aws.String(fmt.Sprintf("Team#%s", want[0].Team))},
				"Age":  {N: aws.String(strconv.FormatInt(want[0].Age, 10))},
			},
		},
	})
	var got []*Person
	if err := client.Scan(&got).
		TableName(tableName).
		ExpressionAttributeValue(":pk", fmt.Sprintf("Person#%s", want[0].Name)).
		ExpressionAttributeValue(":bw", "Team#").
		Segment(5).
		Exec(); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
	ddb.done()
}

func TestScanTotalSegments(t *testing.T) {
	ddb := mock(t)
	client := dynago.New(ddb)
	type Person struct {
		Name string `attr:"PK" fmt:"Person#{}"`
		Team string `fmt:"Team#{}"`
		Age  int64
	}
	want := []*Person{{
		Name: "foo",
		Team: "bar",
		Age:  33,
	}}
	tableName := "baz"
	ddb.MockScan(&dynamodb.ScanInput{
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":pk": {S: aws.String(fmt.Sprintf("Person#%s", want[0].Name))},
			":bw": {S: aws.String("Team#")},
		},
		TableName:      &tableName,
		TotalSegments:  aws.Int64(5),
		ConsistentRead: aws.Bool(false),
	}, &dynamodb.ScanOutput{
		Items: []map[string]*dynamodb.AttributeValue{
			{
				"PK":   {S: aws.String(fmt.Sprintf("Person#%s", want[0].Name))},
				"Team": {S: aws.String(fmt.Sprintf("Team#%s", want[0].Team))},
				"Age":  {N: aws.String(strconv.FormatInt(want[0].Age, 10))},
			},
		},
	})
	var got []*Person
	if err := client.Scan(&got).
		TableName(tableName).
		ExpressionAttributeValue(":pk", fmt.Sprintf("Person#%s", want[0].Name)).
		ExpressionAttributeValue(":bw", "Team#").
		TotalSegments(5).
		Exec(); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
	ddb.done()
}

func TestScanProjExp(t *testing.T) {
	ddb := mock(t)
	client := dynago.New(ddb)
	type Person struct {
		Name string `attr:"PK" fmt:"Person#{}"`
		Team string `fmt:"Team#{}"`
		Age  int64
	}
	want := []*Person{{
		Name: "foo",
		Team: "bar",
		Age:  33,
	}}
	tableName := "baz"
	ddb.MockScan(&dynamodb.ScanInput{
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":pk": {S: aws.String(fmt.Sprintf("Person#%s", want[0].Name))},
			":bw": {S: aws.String("Team#")},
		},
		ProjectionExpression: aws.String("foo"),
		TableName:            &tableName,
		ConsistentRead:       aws.Bool(false),
	}, &dynamodb.ScanOutput{
		Items: []map[string]*dynamodb.AttributeValue{
			{
				"PK":   {S: aws.String(fmt.Sprintf("Person#%s", want[0].Name))},
				"Team": {S: aws.String(fmt.Sprintf("Team#%s", want[0].Team))},
				"Age":  {N: aws.String(strconv.FormatInt(want[0].Age, 10))},
			},
		},
	})
	var got []*Person
	if err := client.Scan(&got).
		TableName(tableName).
		ExpressionAttributeValue(":pk", fmt.Sprintf("Person#%s", want[0].Name)).
		ExpressionAttributeValue(":bw", "Team#").
		ProjectionExpression("foo").
		Exec(); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
	ddb.done()
}

func TestScanFilterExp(t *testing.T) {
	ddb := mock(t)
	client := dynago.New(ddb)
	type Person struct {
		Name string `attr:"PK" fmt:"Person#{}"`
		Team string `fmt:"Team#{}"`
		Age  int64
	}
	want := []*Person{{
		Name: "foo",
		Team: "bar",
		Age:  33,
	}}
	tableName := "baz"
	ddb.MockScan(&dynamodb.ScanInput{
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":pk": {S: aws.String(fmt.Sprintf("Person#%s", want[0].Name))},
			":bw": {S: aws.String("Team#")},
		},
		TableName:        &tableName,
		ConsistentRead:   aws.Bool(false),
		FilterExpression: aws.String("foo"),
	}, &dynamodb.ScanOutput{
		Items: []map[string]*dynamodb.AttributeValue{
			{
				"PK":   {S: aws.String(fmt.Sprintf("Person#%s", want[0].Name))},
				"Team": {S: aws.String(fmt.Sprintf("Team#%s", want[0].Team))},
				"Age":  {N: aws.String(strconv.FormatInt(want[0].Age, 10))},
			},
		},
	})
	var got []*Person
	if err := client.Scan(&got).
		TableName(tableName).
		ExpressionAttributeValue(":pk", fmt.Sprintf("Person#%s", want[0].Name)).
		ExpressionAttributeValue(":bw", "Team#").
		FilterExpression("foo").
		Exec(); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
	ddb.done()
}

func TestScanExcStKey(t *testing.T) {
	ddb := mock(t)
	client := dynago.New(ddb)
	type Person struct {
		Name string `attr:"PK" fmt:"Person#{}"`
		Team string `fmt:"Team#{}"`
		Age  int64
	}
	want := []*Person{{
		Name: "foo",
		Team: "bar",
		Age:  33,
	}}
	tableName := "baz"
	ddb.MockScan(&dynamodb.ScanInput{
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":pk": {S: aws.String(fmt.Sprintf("Person#%s", want[0].Name))},
			":bw": {S: aws.String("Team#")},
		},
		TableName:         &tableName,
		ConsistentRead:    aws.Bool(false),
		ExclusiveStartKey: map[string]*dynamodb.AttributeValue{"foo": {S: aws.String("bar")}},
	}, &dynamodb.ScanOutput{
		Items: []map[string]*dynamodb.AttributeValue{
			{
				"PK":   {S: aws.String(fmt.Sprintf("Person#%s", want[0].Name))},
				"Team": {S: aws.String(fmt.Sprintf("Team#%s", want[0].Team))},
				"Age":  {N: aws.String(strconv.FormatInt(want[0].Age, 10))},
			},
		},
	})
	var got []*Person
	if err := client.Scan(&got).
		TableName(tableName).
		ExpressionAttributeValue(":pk", fmt.Sprintf("Person#%s", want[0].Name)).
		ExpressionAttributeValue(":bw", "Team#").
		ExclusiveStartKey(map[string]*dynamodb.AttributeValue{"foo": {S: aws.String("bar")}}).
		Exec(); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
	ddb.done()
}

func TestScanLimit(t *testing.T) {
	ddb := mock(t)
	client := dynago.New(ddb)
	type Person struct {
		Name string `attr:"PK" fmt:"Person#{}"`
		Team string `fmt:"Team#{}"`
		Age  int64
	}
	want := []*Person{{
		Name: "foo",
		Team: "bar",
		Age:  33,
	}}
	tableName := "baz"
	ddb.MockScan(&dynamodb.ScanInput{
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":pk": {S: aws.String(fmt.Sprintf("Person#%s", want[0].Name))},
			":bw": {S: aws.String("Team#")},
		},
		TableName:      &tableName,
		Limit:          aws.Int64(100),
		ConsistentRead: aws.Bool(false),
	}, &dynamodb.ScanOutput{
		Items: []map[string]*dynamodb.AttributeValue{
			{
				"PK":   {S: aws.String(fmt.Sprintf("Person#%s", want[0].Name))},
				"Team": {S: aws.String(fmt.Sprintf("Team#%s", want[0].Team))},
				"Age":  {N: aws.String(strconv.FormatInt(want[0].Age, 10))},
			},
		},
	})
	var got []*Person
	if err := client.Scan(&got).
		TableName(tableName).
		ExpressionAttributeValue(":pk", fmt.Sprintf("Person#%s", want[0].Name)).
		ExpressionAttributeValue(":bw", "Team#").
		Limit(100).
		Exec(); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
	ddb.done()
}

func TestScanSelect(t *testing.T) {
	ddb := mock(t)
	client := dynago.New(ddb)
	type Person struct {
		Name string `attr:"PK" fmt:"Person#{}"`
		Team string `fmt:"Team#{}"`
		Age  int64
	}
	want := []*Person{{
		Name: "foo",
		Team: "bar",
		Age:  33,
	}}
	tableName := "baz"
	ddb.MockScan(&dynamodb.ScanInput{
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":pk": {S: aws.String(fmt.Sprintf("Person#%s", want[0].Name))},
			":bw": {S: aws.String("Team#")},
		},
		TableName:      &tableName,
		Select:         aws.String("foo"),
		ConsistentRead: aws.Bool(false),
	}, &dynamodb.ScanOutput{
		Items: []map[string]*dynamodb.AttributeValue{
			{
				"PK":   {S: aws.String(fmt.Sprintf("Person#%s", want[0].Name))},
				"Team": {S: aws.String(fmt.Sprintf("Team#%s", want[0].Team))},
				"Age":  {N: aws.String(strconv.FormatInt(want[0].Age, 10))},
			},
		},
	})
	var got []*Person
	if err := client.Scan(&got).
		TableName(tableName).
		ExpressionAttributeValue(":pk", fmt.Sprintf("Person#%s", want[0].Name)).
		ExpressionAttributeValue(":bw", "Team#").
		Select("foo").
		Exec(); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
	ddb.done()
}

func TestScanExpAttrNames(t *testing.T) {
	ddb := mock(t)
	client := dynago.New(ddb)
	type Person struct {
		Name string `attr:"PK" fmt:"Person#{}"`
		Team string `fmt:"Team#{}"`
		Age  int64
	}
	want := []*Person{{
		Name: "foo",
		Team: "bar",
		Age:  33,
	}}
	tableName := "baz"
	ddb.MockScan(&dynamodb.ScanInput{
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":pk": {S: aws.String(fmt.Sprintf("Person#%s", want[0].Name))},
			":bw": {S: aws.String("Team#")},
		},
		TableName:                &tableName,
		ExpressionAttributeNames: map[string]*string{"foo": aws.String("#f")},
		ConsistentRead:           aws.Bool(false),
	}, &dynamodb.ScanOutput{
		Items: []map[string]*dynamodb.AttributeValue{
			{
				"PK":   {S: aws.String(fmt.Sprintf("Person#%s", want[0].Name))},
				"Team": {S: aws.String(fmt.Sprintf("Team#%s", want[0].Team))},
				"Age":  {N: aws.String(strconv.FormatInt(want[0].Age, 10))},
			},
		},
	})
	var got []*Person
	if err := client.Scan(&got).
		TableName(tableName).
		ExpressionAttributeValue(":pk", fmt.Sprintf("Person#%s", want[0].Name)).
		ExpressionAttributeValue(":bw", "Team#").
		ExpressionAttributeName("foo", "#f").
		Exec(); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
	ddb.done()
}

func TestScanConsistentRead(t *testing.T) {
	ddb := mock(t)
	client := dynago.New(ddb)
	type Person struct {
		Name string `attr:"PK" fmt:"Person#{}"`
		Team string `fmt:"Team#{}"`
		Age  int64
	}
	want := []*Person{{
		Name: "foo",
		Team: "bar",
		Age:  33,
	}}
	tableName := "baz"
	ddb.MockScan(&dynamodb.ScanInput{
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":pk": {S: aws.String(fmt.Sprintf("Person#%s", want[0].Name))},
			":bw": {S: aws.String("Team#")},
		},
		TableName:      &tableName,
		ConsistentRead: aws.Bool(true),
	}, &dynamodb.ScanOutput{
		Items: []map[string]*dynamodb.AttributeValue{
			{
				"PK":   {S: aws.String(fmt.Sprintf("Person#%s", want[0].Name))},
				"Team": {S: aws.String(fmt.Sprintf("Team#%s", want[0].Team))},
				"Age":  {N: aws.String(strconv.FormatInt(want[0].Age, 10))},
			},
		},
	})
	var got []*Person
	if err := client.Scan(&got).
		TableName(tableName).
		ExpressionAttributeValue(":pk", fmt.Sprintf("Person#%s", want[0].Name)).
		ExpressionAttributeValue(":bw", "Team#").
		ConsistentRead(true).
		Exec(); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
	ddb.done()
}

func TestScanIndex(t *testing.T) {
	ddb := mock(t)
	client := dynago.New(ddb)
	type Person struct {
		Name string `attr:"PK" fmt:"Person#{}"`
		Team string `fmt:"Team#{}"`
		Age  int64
	}
	want := []*Person{{
		Name: "foo",
		Team: "bar",
		Age:  33,
	}}
	tableName := "baz"
	ddb.MockScan(&dynamodb.ScanInput{
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":pk": {S: aws.String(fmt.Sprintf("Person#%s", want[0].Name))},
			":bw": {S: aws.String("Team#")},
		},
		TableName:      &tableName,
		ConsistentRead: aws.Bool(false),
		IndexName:      aws.String("foo"),
	}, &dynamodb.ScanOutput{
		Items: []map[string]*dynamodb.AttributeValue{
			{
				"PK":   {S: aws.String(fmt.Sprintf("Person#%s", want[0].Name))},
				"Team": {S: aws.String(fmt.Sprintf("Team#%s", want[0].Team))},
				"Age":  {N: aws.String(strconv.FormatInt(want[0].Age, 10))},
			},
		},
	})
	var got []*Person
	if err := client.Scan(&got).
		TableName(tableName).
		ExpressionAttributeValue(":pk", fmt.Sprintf("Person#%s", want[0].Name)).
		ExpressionAttributeValue(":bw", "Team#").
		IndexName("foo").
		Exec(); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
	ddb.done()
}
