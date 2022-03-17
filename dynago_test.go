package dynago_test

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/twharmon/dynago"
)

// BenchmarkItemNoTags-10          	  884364	        1347 ns/op	    1352 B/op	      26 allocs/op
// BenchmarkItemNoTagsByHand-10    	 1694782	       707.0 ns/op	     744 B/op	       9 allocs/op

func assertEq(t *testing.T, want, got interface{}) {
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("want: %v\n got: %v", want, got)
	}
}

func TestItemString(t *testing.T) {
	type Person struct {
		Name string
	}
	p := Person{
		Name: "foo",
	}
	want := map[string]*dynamodb.AttributeValue{
		"Name": {S: aws.String(p.Name)},
	}
	client := dynago.New()
	got := client.Item(&p)
	assertEq(t, want, got)
}

func TestItemPtrPtr(t *testing.T) {
	type Person struct {
		Name **string
	}
	name := "foo"
	namePtr := &name
	p := Person{
		Name: &namePtr,
	}
	want := map[string]*dynamodb.AttributeValue{
		"Name": {S: *p.Name},
	}
	client := dynago.New()
	got := client.Item(&p)
	assertEq(t, want, got)
}

func TestItemAttribute(t *testing.T) {
	type Person struct {
		Name string `attribute:"PK"`
	}
	p := Person{
		Name: "foo",
	}
	want := map[string]*dynamodb.AttributeValue{
		"PK": {S: aws.String(p.Name)},
	}
	client := dynago.New()
	got := client.Item(&p)
	assertEq(t, want, got)
}

func TestItemStringFmt(t *testing.T) {
	type Person struct {
		Name string `fmt:"Person#%s"`
	}
	p := Person{
		Name: "foo",
	}
	want := map[string]*dynamodb.AttributeValue{
		"Name": {S: aws.String(fmt.Sprintf("Person#%s", p.Name))},
	}
	client := dynago.New()
	got := client.Item(&p)
	assertEq(t, want, got)
}

func TestUnmarshalString(t *testing.T) {
	type Person struct {
		Name string
	}
	want := Person{
		Name: "bar",
	}
	item := map[string]*dynamodb.AttributeValue{
		"Name": {S: aws.String(want.Name)},
	}
	client := dynago.New()
	var got Person
	client.Unmarshal(item, &got)
	assertEq(t, want, got)
}

func TestUnmarshalAttribute(t *testing.T) {
	type Person struct {
		Name string `attribute:"PK"`
	}
	want := Person{
		Name: "bar",
	}
	item := map[string]*dynamodb.AttributeValue{
		"PK": {S: aws.String(want.Name)},
	}
	client := dynago.New()
	var got Person
	client.Unmarshal(item, &got)
	assertEq(t, want, got)
}

func TestUnmarshalStringFmt(t *testing.T) {
	type Person struct {
		Name string `fmt:"Person#%s"`
	}
	want := Person{
		Name: "bar",
	}
	item := map[string]*dynamodb.AttributeValue{
		"Name": {S: aws.String(fmt.Sprintf("Person#%s", want.Name))},
	}
	client := dynago.New()
	var got Person
	client.Unmarshal(item, &got)
	assertEq(t, want, got)
}

func TestItemInt64(t *testing.T) {
	type Person struct {
		Age int64
	}
	p := Person{
		Age: 33,
	}
	want := map[string]*dynamodb.AttributeValue{
		"Age": {N: aws.String(fmt.Sprintf("%d", p.Age))},
	}
	client := dynago.New()
	got := client.Item(&p)
	assertEq(t, want, got)
}

func TestUnmarshalInt64(t *testing.T) {
	type Person struct {
		Age int64
	}
	want := Person{
		Age: 33,
	}
	item := map[string]*dynamodb.AttributeValue{
		"Age": {N: aws.String(strconv.FormatInt(want.Age, 10))},
	}
	client := dynago.New()
	var got Person
	if err := client.Unmarshal(item, &got); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
}

func TestItemFloat64(t *testing.T) {
	type Person struct {
		Age float64
	}
	p := Person{
		Age: 33.234,
	}
	want := map[string]*dynamodb.AttributeValue{
		"Age": {N: aws.String("33.234")},
	}
	client := dynago.New()
	got := client.Item(&p)
	assertEq(t, want, got)
}

func TestUnmarshalFloat64(t *testing.T) {
	type Person struct {
		Age float64
	}
	want := Person{
		Age: 33.234,
	}
	item := map[string]*dynamodb.AttributeValue{
		"Age": {N: aws.String("33.234")},
	}
	client := dynago.New()
	var got Person
	if err := client.Unmarshal(item, &got); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
}

func TestItemFloat64Prec(t *testing.T) {
	type Person struct {
		Age float64 `prec:"2"`
	}
	p := Person{
		Age: 33.234,
	}
	want := map[string]*dynamodb.AttributeValue{
		"Age": {N: aws.String("33.23")},
	}
	client := dynago.New()
	got := client.Item(&p)
	assertEq(t, want, got)
}

func TestUnmarshalFloat64Prec(t *testing.T) {
	type Person struct {
		Age float64 `prec:"2"`
	}
	want := Person{
		Age: 33.23,
	}
	item := map[string]*dynamodb.AttributeValue{
		"Age": {N: aws.String("33.23")},
	}
	client := dynago.New()
	var got Person
	if err := client.Unmarshal(item, &got); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
}

func TestItemBytes(t *testing.T) {
	type Person struct {
		ID []byte
	}
	p := Person{
		ID: []byte{1, 2, 3},
	}
	want := map[string]*dynamodb.AttributeValue{
		"ID": {B: p.ID},
	}
	client := dynago.New()
	got := client.Item(&p)
	assertEq(t, want, got)
}

func TestUnmarshalBytes(t *testing.T) {
	type Person struct {
		ID []byte
	}
	want := Person{
		ID: []byte{1, 2, 3},
	}
	item := map[string]*dynamodb.AttributeValue{
		"ID": {B: want.ID},
	}
	client := dynago.New()
	var got Person
	if err := client.Unmarshal(item, &got); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
}

func TestItemBool(t *testing.T) {
	type Person struct {
		Tall bool
	}
	p := Person{
		Tall: true,
	}
	want := map[string]*dynamodb.AttributeValue{
		"Tall": {BOOL: &p.Tall},
	}
	client := dynago.New()
	got := client.Item(&p)
	assertEq(t, want, got)
}

func TestUnmarshalBool(t *testing.T) {
	type Person struct {
		Tall bool
	}
	want := Person{
		Tall: true,
	}
	item := map[string]*dynamodb.AttributeValue{
		"Tall": {BOOL: &want.Tall},
	}
	client := dynago.New()
	var got Person
	if err := client.Unmarshal(item, &got); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
}

func BenchmarkItemNoTags(b *testing.B) {
	type Person struct {
		Name       string
		Age        int64
		Percentage float64
		Alive      bool
	}
	p := Person{
		Name:       "George",
		Age:        33,
		Percentage: 25.323521,
		Alive:      true,
	}
	client := dynago.New()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		client.Item(&p)
	}
}

func BenchmarkItemNoTagsByHand(b *testing.B) {
	type Person struct {
		Name       string
		Age        int64
		Percentage float64
		Alive      bool
	}
	p := Person{
		Name:       "George",
		Age:        33,
		Percentage: 25.323521,
		Alive:      true,
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = map[string]*dynamodb.AttributeValue{
			"Name":       {S: &p.Name},
			"Age":        {N: aws.String(strconv.FormatInt(p.Age, 10))},
			"Percentage": {N: aws.String(strings.TrimRight(strconv.FormatFloat(p.Percentage, 'f', 14, 64), "0"))},
			"Alive":      {BOOL: &p.Alive},
		}
	}
}

func BenchmarkItemTags(b *testing.B) {
	type Person struct {
		Name       string  `attribute:"name" fmt:"Person#%s"`
		Age        int64   `attribute:"age"`
		Percentage float64 `attribute:"percentage"`
		Alive      bool    `attribute:"alive"`
	}
	p := Person{
		Name:       "George",
		Age:        33,
		Percentage: 25.323521,
		Alive:      true,
	}
	client := dynago.New()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		client.Item(&p)
	}
}

func BenchmarkItemTagsByHand(b *testing.B) {
	type Person struct {
		Name       string
		Age        int64
		Percentage float64
		Alive      bool
	}
	p := Person{
		Name:       "George",
		Age:        33,
		Percentage: 25.323521,
		Alive:      true,
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = map[string]*dynamodb.AttributeValue{
			"name":       {S: aws.String(fmt.Sprintf("Person#%s", p.Name))},
			"age":        {N: aws.String(strconv.FormatInt(p.Age, 10))},
			"percentage": {N: aws.String(strings.TrimRight(strconv.FormatFloat(p.Percentage, 'f', 14, 64), "0"))},
			"alive":      {BOOL: &p.Alive},
		}
	}
}

func BenchmarkUnmarshalNoTags(b *testing.B) {
	type Person struct {
		Name       string
		Age        int64
		Percentage float64
		Alive      bool
	}
	item := map[string]*dynamodb.AttributeValue{
		"Name":       {S: aws.String("George")},
		"Age":        {N: aws.String(strconv.FormatInt(33, 10))},
		"Percentage": {N: aws.String(strings.TrimRight(strconv.FormatFloat(25.323521, 'f', 14, 64), "0"))},
		"Alive":      {BOOL: aws.Bool(true)},
	}
	client := dynago.New()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var p Person
		if err := client.Unmarshal(item, &p); err != nil {
			panic(err)
		}
	}
}

func BenchmarkUnmarshalNoTagsByHand(b *testing.B) {
	type Person struct {
		Name       string
		Age        int64
		Percentage float64
		Alive      bool
	}
	item := map[string]*dynamodb.AttributeValue{
		"Name":       {S: aws.String("George")},
		"Age":        {N: aws.String(strconv.FormatInt(33, 10))},
		"Percentage": {N: aws.String(strings.TrimRight(strconv.FormatFloat(25.323521, 'f', 14, 64), "0"))},
		"Alive":      {BOOL: aws.Bool(true)},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var p Person
		var err error
		if item["Name"] != nil && item["Name"].S != nil {
			p.Name = *item["Name"].S
		}
		if item["Age"] != nil && item["Age"].N != nil {
			p.Age, err = strconv.ParseInt(*item["Age"].N, 10, 64)
			if err != nil {
				panic(err)
			}
		}
		if item["Percentage"] != nil && item["Percentage"].N != nil {
			p.Percentage, err = strconv.ParseFloat(*item["Percentage"].N, 64)
			if err != nil {
				panic(err)
			}
		}
		if item["Alive"] != nil && item["Alive"].BOOL != nil {
			p.Alive = *item["Alive"].BOOL
		}
	}
}
