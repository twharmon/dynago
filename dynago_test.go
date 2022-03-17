package dynago_test

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/twharmon/dynago"
)

// BenchmarkItem-10               	  317056	      3504 ns/op	    2274 B/op	      40 allocs/op
// BenchmarkItemByHand-10         	 1000000	      1144 ns/op	    1000 B/op	      15 allocs/op
// BenchmarkUnmarshal-10          	  115984	     10089 ns/op	    8740 B/op	     137 allocs/op
// BenchmarkUnmarshalByHand-10    	 3469292	       349.9 ns/op	       0 B/op	       0 allocs/op

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
		Name string `fmt:"Person#{Name}"`
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

func TestItemStringFmtImplicit(t *testing.T) {
	type Person struct {
		Name string `fmt:"Person#{}"`
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

func TestItemStringCompoundFmt(t *testing.T) {
	type Person struct {
		Team string `attribute:"-"`
		Name string `fmt:"Team#{Team}#Person#{}" attribute:"PK"`
	}
	p := Person{
		Team: "foo",
		Name: "bar",
	}
	want := map[string]*dynamodb.AttributeValue{
		"PK": {S: aws.String(fmt.Sprintf("Team#%s#Person#%s", p.Team, p.Name))},
	}
	client := dynago.New()
	got := client.Item(&p)
	assertEq(t, want, got)
}

func TestUnmarshalStringCompoundFmt(t *testing.T) {
	type Person struct {
		Team string `attribute:"-"`
		Name string `fmt:"Team#{Team}#Person#{}" attribute:"PK"`
	}
	want := Person{
		Team: "foo",
		Name: "bar",
	}
	item := map[string]*dynamodb.AttributeValue{
		"PK": {S: aws.String(fmt.Sprintf("Team#%s#Person#%s", want.Team, want.Name))},
	}
	client := dynago.New()
	var got Person
	client.Unmarshal(item, &got)
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
		Name string `fmt:"Person#{Name}"`
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

func TestItemTimeNoLayoutTag(t *testing.T) {
	type Person struct {
		Born time.Time
	}
	p := Person{
		Born: time.Now(),
	}
	want := map[string]*dynamodb.AttributeValue{
		"Born": {S: aws.String(p.Born.Format(time.RFC3339))},
	}
	client := dynago.New()
	got := client.Item(&p)
	assertEq(t, want, got)
}

func TestUnmarshalTimeNoLayoutTag(t *testing.T) {
	type Person struct {
		Born time.Time
	}
	want := Person{
		Born: time.Now(),
	}
	item := map[string]*dynamodb.AttributeValue{
		"Born": {S: aws.String(want.Born.Format(time.RFC3339))},
	}
	client := dynago.New()
	var got Person
	if err := client.Unmarshal(item, &got); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want.Born.Unix(), got.Born.Unix())
}

func TestItemTimeWithLayoutTag(t *testing.T) {
	type Person struct {
		Born time.Time `layout:"15:04:05 Z07:00 2006-01-02"`
	}
	p := Person{
		Born: time.Now(),
	}
	want := map[string]*dynamodb.AttributeValue{
		"Born": {S: aws.String(p.Born.Format("15:04:05 Z07:00 2006-01-02"))},
	}
	client := dynago.New()
	got := client.Item(&p)
	assertEq(t, want, got)
}

func TestUnmarshalTimeWithLayoutTag(t *testing.T) {
	type Person struct {
		Born time.Time `layout:"15:04:05 Z07:00 2006-01-02"`
	}
	want := Person{
		Born: time.Now(),
	}
	item := map[string]*dynamodb.AttributeValue{
		"Born": {S: aws.String(want.Born.Format("15:04:05 Z07:00 2006-01-02"))},
	}
	client := dynago.New()
	var got Person
	if err := client.Unmarshal(item, &got); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want.Born.Unix(), got.Born.Unix())
}

func TestItemWithAdditionalAttributes(t *testing.T) {
	type Person struct {
		Name string
	}
	client := dynago.New(&dynago.Config{
		AdditionalAttributes: func(v reflect.Value) map[string]*dynamodb.AttributeValue {
			switch v.Interface().(type) {
			case Person:
				return map[string]*dynamodb.AttributeValue{
					"Type": {S: aws.String("Person")},
				}
			}
			return nil
		},
	})
	p := Person{
		Name: "foo",
	}
	want := map[string]*dynamodb.AttributeValue{
		"Name": {S: &p.Name},
		"Type": {S: aws.String("Person")},
	}
	got := client.Item(&p)
	assertEq(t, want, got)
}

func BenchmarkItem(b *testing.B) {
	type Person struct {
		Name       string  `attribute:"name" fmt:"Person#{Name}"`
		Age        int64   `attribute:"age"`
		Percentage float64 `attribute:"percentage"`
		Alive      bool    `attribute:"alive"`
		Born       time.Time
	}
	p := Person{
		Name:       "George",
		Age:        33,
		Percentage: 25.323521,
		Alive:      true,
		Born:       time.Now(),
	}
	client := dynago.New()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = client.Item(&p)
	}
}

func BenchmarkItemByHand(b *testing.B) {
	type Person struct {
		Name       string `attribute:"name" fmt:"Person#{Name}"`
		Age        int64
		Percentage float64
		Alive      bool
		Born       time.Time
	}
	p := Person{
		Name:       "George",
		Age:        33,
		Percentage: 25.323521,
		Alive:      true,
		Born:       time.Now(),
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = map[string]*dynamodb.AttributeValue{
			"name":       {S: aws.String(fmt.Sprintf("Person#%s", p.Name))},
			"Age":        {N: aws.String(strconv.FormatInt(p.Age, 10))},
			"Percentage": {N: aws.String(strings.TrimRight(strconv.FormatFloat(p.Percentage, 'f', 14, 64), "0"))},
			"Alive":      {BOOL: &p.Alive},
			"Born":       {S: aws.String(p.Born.Format(time.RFC3339))},
		}
	}
}

func BenchmarkUnmarshal(b *testing.B) {
	type Person struct {
		Name       string `attribute:"name" fmt:"Person#{Name}"`
		Age        int64
		Percentage float64
		Alive      bool
		Born       time.Time
	}
	item := map[string]*dynamodb.AttributeValue{
		"name":       {S: aws.String("Person#George")},
		"Age":        {N: aws.String(strconv.FormatInt(33, 10))},
		"Percentage": {N: aws.String(strings.TrimRight(strconv.FormatFloat(25.323521, 'f', 14, 64), "0"))},
		"Alive":      {BOOL: aws.Bool(true)},
		"Born":       {S: aws.String(time.Now().Format(time.RFC3339))},
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

func BenchmarkUnmarshalByHand(b *testing.B) {
	type Person struct {
		Name       string
		Age        int64
		Percentage float64
		Alive      bool
		Born       time.Time
	}
	item := map[string]*dynamodb.AttributeValue{
		"name":       {S: aws.String("Person#George")},
		"Age":        {N: aws.String(strconv.FormatInt(33, 10))},
		"Percentage": {N: aws.String(strings.TrimRight(strconv.FormatFloat(25.323521, 'f', 14, 64), "0"))},
		"Alive":      {BOOL: aws.Bool(true)},
		"Born":       {S: aws.String(time.Now().Format(time.RFC3339))},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var p Person
		var err error
		if item["name"] != nil && item["name"].S != nil {
			p.Name = strings.TrimPrefix(*item["name"].S, "Person#")
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
		if item["Born"] != nil && item["Born"].S != nil {
			p.Born, err = time.Parse(time.RFC3339, *item["Born"].S)
			if err != nil {
				panic(err)
			}
		}
	}
}
