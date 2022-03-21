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

// BenchmarkMarshal-10            	  446264	      2313 ns/op	    1819 B/op	      30 allocs/op
// BenchmarkMarshalByHand-10      	 1000000	      1120 ns/op	    1000 B/op	      15 allocs/op
// BenchmarkUnmarshal-10          	  614174	      1921 ns/op	     584 B/op	      11 allocs/op
// BenchmarkUnmarshalByHand-10    	 3537462	       343.4 ns/op	       0 B/op	       0 allocs/op

func TestMarshalString(t *testing.T) {
	type Person struct {
		Name string
	}
	p := Person{
		Name: "foo",
	}
	want := map[string]*dynamodb.AttributeValue{
		"Name": {S: aws.String(p.Name)},
	}
	client := dynago.New(nil)
	got, err := client.Marshal(&p)
	if err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
}

func TestMarshalPtrPtr(t *testing.T) {
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
	client := dynago.New(nil)
	got, err := client.Marshal(&p)
	if err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
}

func TestMarshalAttribute(t *testing.T) {
	type Person struct {
		Name string `attr:"PK"`
	}
	p := Person{
		Name: "foo",
	}
	want := map[string]*dynamodb.AttributeValue{
		"PK": {S: aws.String(p.Name)},
	}
	client := dynago.New(nil)
	got, err := client.Marshal(&p)
	if err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
}

func TestMarshalStringFmt(t *testing.T) {
	type Person struct {
		Name string `fmt:"Person#{Name}"`
	}
	p := Person{
		Name: "foo",
	}
	want := map[string]*dynamodb.AttributeValue{
		"Name": {S: aws.String(fmt.Sprintf("Person#%s", p.Name))},
	}
	client := dynago.New(nil)
	got, err := client.Marshal(&p)
	if err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
}

func TestMarshalStringFmtImplicit(t *testing.T) {
	type Person struct {
		Name string `fmt:"Person#{}"`
	}
	p := Person{
		Name: "foo",
	}
	want := map[string]*dynamodb.AttributeValue{
		"Name": {S: aws.String(fmt.Sprintf("Person#%s", p.Name))},
	}
	client := dynago.New(nil)
	got, err := client.Marshal(&p)
	if err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
}

func TestMarshalStringCompoundFmt(t *testing.T) {
	type Person struct {
		Team string `attr:"-"`
		Name string `fmt:"Team#{Team}#Person#{}" attr:"PK"`
	}
	p := Person{
		Team: "foo",
		Name: "bar",
	}
	want := map[string]*dynamodb.AttributeValue{
		"PK": {S: aws.String(fmt.Sprintf("Team#%s#Person#%s", p.Team, p.Name))},
	}
	client := dynago.New(nil)
	got, err := client.Marshal(&p)
	if err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
}

func TestUnmarshalStringCompoundFmt(t *testing.T) {
	type Person struct {
		Team string `attr:"-"`
		Name string `fmt:"Team#{Team}#Person#{}" attr:"PK"`
	}
	want := Person{
		Team: "foo",
		Name: "bar",
	}
	item := map[string]*dynamodb.AttributeValue{
		"PK": {S: aws.String(fmt.Sprintf("Team#%s#Person#%s", want.Team, want.Name))},
	}
	client := dynago.New(nil)
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
	client := dynago.New(nil)
	var got Person
	client.Unmarshal(item, &got)
	assertEq(t, want, got)
}

func TestUnmarshalAttribute(t *testing.T) {
	type Person struct {
		Name string `attr:"PK"`
	}
	want := Person{
		Name: "bar",
	}
	item := map[string]*dynamodb.AttributeValue{
		"PK": {S: aws.String(want.Name)},
	}
	client := dynago.New(nil)
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
	client := dynago.New(nil)
	var got Person
	client.Unmarshal(item, &got)
	assertEq(t, want, got)
}

func TestMarshalStruct(t *testing.T) {
	type Pet struct {
		Type string
		Age  int64 `attr:"AGE"`
	}
	type Person struct {
		Name string
		Pet  *Pet
	}
	p := Person{
		Name: "foo",
		Pet: &Pet{
			Type: "dog",
			Age:  5,
		},
	}
	want := map[string]*dynamodb.AttributeValue{
		"Name": {S: &p.Name},
		"Pet": {M: map[string]*dynamodb.AttributeValue{
			"Type": {S: &p.Pet.Type},
			"AGE":  {N: aws.String(strconv.FormatInt(p.Pet.Age, 10))},
		}},
	}
	client := dynago.New(nil)
	got, err := client.Marshal(&p)
	if err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
}

func TestUnmarshalStruct(t *testing.T) {
	type Pet struct {
		Type string
		Age  int64 `attr:"AGE"`
	}
	type Person struct {
		Name string
		Pet  *Pet
	}
	want := Person{
		Name: "foo",
		Pet: &Pet{
			Type: "dog",
			Age:  5,
		},
	}
	item := map[string]*dynamodb.AttributeValue{
		"Name": {S: &want.Name},
		"Pet": {M: map[string]*dynamodb.AttributeValue{
			"Type": {S: &want.Pet.Type},
			"AGE":  {N: aws.String(strconv.FormatInt(want.Pet.Age, 10))},
		}},
	}
	client := dynago.New(nil)
	var got Person
	if err := client.Unmarshal(item, &got); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
}

func TestMarshalInt64(t *testing.T) {
	type Person struct {
		Age int64
	}
	p := Person{
		Age: 33,
	}
	want := map[string]*dynamodb.AttributeValue{
		"Age": {N: aws.String(fmt.Sprintf("%d", p.Age))},
	}
	client := dynago.New(nil)
	got, err := client.Marshal(&p)
	if err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
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
	client := dynago.New(nil)
	var got Person
	if err := client.Unmarshal(item, &got); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
}

func TestMarshalFloat64(t *testing.T) {
	type Person struct {
		Age float64
	}
	p := Person{
		Age: 33.234,
	}
	want := map[string]*dynamodb.AttributeValue{
		"Age": {N: aws.String("33.234")},
	}
	client := dynago.New(nil)
	got, err := client.Marshal(&p)
	if err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
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
	client := dynago.New(nil)
	var got Person
	if err := client.Unmarshal(item, &got); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
}

func TestMarshalFloat64Prec(t *testing.T) {
	type Person struct {
		Age float64 `prec:"2"`
	}
	p := Person{
		Age: 33.234,
	}
	want := map[string]*dynamodb.AttributeValue{
		"Age": {N: aws.String("33.23")},
	}
	client := dynago.New(nil)
	got, err := client.Marshal(&p)
	if err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
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
	client := dynago.New(nil)
	var got Person
	if err := client.Unmarshal(item, &got); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
}

func TestMarshalBytes(t *testing.T) {
	type Person struct {
		ID []byte
	}
	p := Person{
		ID: []byte{1, 2, 3},
	}
	want := map[string]*dynamodb.AttributeValue{
		"ID": {B: p.ID},
	}
	client := dynago.New(nil)
	got, err := client.Marshal(&p)
	if err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
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
	client := dynago.New(nil)
	var got Person
	if err := client.Unmarshal(item, &got); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
}

func TestMarshalBool(t *testing.T) {
	type Person struct {
		Tall bool
	}
	p := Person{
		Tall: true,
	}
	want := map[string]*dynamodb.AttributeValue{
		"Tall": {BOOL: &p.Tall},
	}
	client := dynago.New(nil)
	got, err := client.Marshal(&p)
	if err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
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
	client := dynago.New(nil)
	var got Person
	if err := client.Unmarshal(item, &got); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
}

func TestMarshalTimeNoLayoutTag(t *testing.T) {
	type Person struct {
		Born time.Time
	}
	p := Person{
		Born: time.Now(),
	}
	want := map[string]*dynamodb.AttributeValue{
		"Born": {S: aws.String(p.Born.Format(time.RFC3339))},
	}
	client := dynago.New(nil)
	got, err := client.Marshal(&p)
	if err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
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
	client := dynago.New(nil)
	var got Person
	if err := client.Unmarshal(item, &got); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want.Born.Unix(), got.Born.Unix())
}

func TestMarshalTimeWithLayoutTag(t *testing.T) {
	type Person struct {
		Born time.Time `layout:"15:04:05 Z07:00 2006-01-02"`
	}
	p := Person{
		Born: time.Now(),
	}
	want := map[string]*dynamodb.AttributeValue{
		"Born": {S: aws.String(p.Born.Format("15:04:05 Z07:00 2006-01-02"))},
	}
	client := dynago.New(nil)
	got, err := client.Marshal(&p)
	if err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
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
	client := dynago.New(nil)
	var got Person
	if err := client.Unmarshal(item, &got); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want.Born.Unix(), got.Born.Unix())
}

func TestMarshalWithAdditionalAttributes(t *testing.T) {
	type Person struct {
		Name string
	}
	client := dynago.New(nil, &dynago.Config{
		AdditionalAttrs: func(item map[string]*dynamodb.AttributeValue, v reflect.Value) {
			switch v.Interface().(type) {
			case Person:
				item["Type"] = &dynamodb.AttributeValue{S: aws.String("Person")}
			}
		},
	})
	p := Person{
		Name: "foo",
	}
	want := map[string]*dynamodb.AttributeValue{
		"Name": {S: &p.Name},
		"Type": {S: aws.String("Person")},
	}
	got, err := client.Marshal(&p)
	if err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
}

func BenchmarkMarshal(b *testing.B) {
	type Person struct {
		Name       string  `attr:"name" fmt:"Person#{Name}"`
		Age        int64   `attr:"age"`
		Percentage float64 `attr:"percentage"`
		Alive      bool    `attr:"alive"`
		Born       time.Time
	}
	p := Person{
		Name:       "George",
		Age:        33,
		Percentage: 25.323521,
		Alive:      true,
		Born:       time.Now(),
	}
	client := dynago.New(nil)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = client.Marshal(&p)
	}
}

func BenchmarkMarshalByHand(b *testing.B) {
	type Person struct {
		Name       string `attr:"name" fmt:"Person#{Name}"`
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
		Name       string `attr:"name" fmt:"Person#{Name}"`
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
	client := dynago.New(nil)
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
