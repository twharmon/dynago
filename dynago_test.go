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
	if err := client.Unmarshal(item, &got); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
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
	if err := client.Unmarshal(item, &got); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
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
	if err := client.Unmarshal(item, &got); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
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
	if err := client.Unmarshal(item, &got); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
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

func TestMarshalSliceString(t *testing.T) {
	type Person struct {
		Names []string
	}
	p := Person{
		Names: []string{"foo", "bar"},
	}
	want := map[string]*dynamodb.AttributeValue{
		"Names": {},
	}
	for i := range p.Names {
		want["Names"].L = append(want["Names"].L, &dynamodb.AttributeValue{S: &p.Names[i]})
	}
	client := dynago.New(nil)
	got, err := client.Marshal(&p)
	if err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
}

func TestMarshalSliceInt64(t *testing.T) {
	type Person struct {
		Scores []int64
	}
	p := Person{
		Scores: []int64{2, 3, 4},
	}
	want := map[string]*dynamodb.AttributeValue{
		"Scores": {},
	}
	for i := range p.Scores {
		s := strconv.FormatInt(p.Scores[i], 10)
		want["Scores"].L = append(want["Scores"].L, &dynamodb.AttributeValue{N: &s})
	}
	client := dynago.New(nil)
	got, err := client.Marshal(&p)
	if err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
}

func TestMarshalSliceStruct(t *testing.T) {
	type Pet struct {
		Name string
	}
	type Person struct {
		Pets []Pet
	}
	p := Person{
		Pets: []Pet{{Name: "Harry"}, {Name: "Larry"}},
	}
	want := map[string]*dynamodb.AttributeValue{
		"Pets": {},
	}
	for i := range p.Pets {
		av := &dynamodb.AttributeValue{
			M: map[string]*dynamodb.AttributeValue{
				"Name": {S: &p.Pets[i].Name},
			},
		}
		want["Pets"].L = append(want["Pets"].L, av)
	}
	client := dynago.New(nil)
	got, err := client.Marshal(&p)
	if err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
}

func TestMarshalSliceStructPtr(t *testing.T) {
	type Pet struct {
		Name string
	}
	type Person struct {
		Pets []*Pet
	}
	p := Person{
		Pets: []*Pet{{Name: "Harry"}, {Name: "Larry"}},
	}
	want := map[string]*dynamodb.AttributeValue{
		"Pets": {},
	}
	for i := range p.Pets {
		av := &dynamodb.AttributeValue{
			M: map[string]*dynamodb.AttributeValue{
				"Name": {S: &p.Pets[i].Name},
			},
		}
		want["Pets"].L = append(want["Pets"].L, av)
	}
	client := dynago.New(nil)
	got, err := client.Marshal(&p)
	if err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
}

func TestMarshalSliceTime(t *testing.T) {
	type Person struct {
		Appointments []time.Time `layout:"Monday, 02-Jan-06 15:04:05 MST"`
	}
	p := Person{
		Appointments: []time.Time{time.Now(), time.Now().Add(time.Hour)},
	}
	want := map[string]*dynamodb.AttributeValue{
		"Appointments": {},
	}
	for i := range p.Appointments {
		s := p.Appointments[i].Format("Monday, 02-Jan-06 15:04:05 MST")
		av := &dynamodb.AttributeValue{S: &s}
		want["Appointments"].L = append(want["Appointments"].L, av)
	}
	client := dynago.New(nil)
	got, err := client.Marshal(&p)
	if err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
}

func TestUnmarshalSliceTime(t *testing.T) {
	type Person struct {
		Appointments []time.Time `layout:"Monday, 02-Jan-06 15:04:05 MST"`
	}
	want := Person{
		Appointments: []time.Time{time.Now().Round(time.Second), time.Now().Round(time.Second).Add(time.Hour)},
	}
	item := map[string]*dynamodb.AttributeValue{
		"Appointments": {},
	}
	for i := range want.Appointments {
		s := want.Appointments[i].Format("Monday, 02-Jan-06 15:04:05 MST")
		av := &dynamodb.AttributeValue{S: &s}
		item["Appointments"].L = append(item["Appointments"].L, av)
	}
	client := dynago.New(nil)
	var got Person
	err := client.Unmarshal(item, &got)
	if err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	for i := range got.Appointments {
		gota := got.Appointments[i]
		wanta := want.Appointments[i]
		if !gota.Equal(wanta) {
			t.Fatalf("times not equal: %s != %s", wanta.Format(time.RFC3339), gota.Format(time.RFC3339))
		}
	}
}

func TestMarshalSliceTimePtr(t *testing.T) {
	type Person struct {
		Appointments []*time.Time
	}
	a := time.Now()
	b := time.Now().Add(time.Hour)
	p := Person{
		Appointments: []*time.Time{&a, &b},
	}
	want := map[string]*dynamodb.AttributeValue{
		"Appointments": {},
	}
	for i := range p.Appointments {
		s := p.Appointments[i].Format(time.RFC3339)
		av := &dynamodb.AttributeValue{S: &s}
		want["Appointments"].L = append(want["Appointments"].L, av)
	}
	client := dynago.New(nil)
	got, err := client.Marshal(&p)
	if err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
}

func TestUnmarshalSliceString(t *testing.T) {
	type Person struct {
		Names []string
	}
	want := Person{
		Names: []string{"foo", "bar"},
	}
	item := map[string]*dynamodb.AttributeValue{
		"Names": {},
	}
	for i := range want.Names {
		item["Names"].L = append(item["Names"].L, &dynamodb.AttributeValue{S: &want.Names[i]})
	}
	client := dynago.New(nil)
	var got Person
	if err := client.Unmarshal(item, &got); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
}

func TestUnmarshalSliceStruct(t *testing.T) {
	type Pet struct {
		Name string
	}
	type Person struct {
		Pets []Pet
	}
	want := Person{
		Pets: []Pet{{Name: "Harry"}, {Name: "Larry"}},
	}
	item := map[string]*dynamodb.AttributeValue{
		"Pets": {},
	}
	for i := range want.Pets {
		item["Pets"].L = append(item["Pets"].L, &dynamodb.AttributeValue{M: map[string]*dynamodb.AttributeValue{
			"Name": {S: &want.Pets[i].Name},
		}})
	}
	client := dynago.New(nil)
	var got Person
	if err := client.Unmarshal(item, &got); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
}

func TestUnmarshalSliceStructPtr(t *testing.T) {
	type Pet struct {
		Name string
	}
	type Person struct {
		Pets []*Pet
	}
	want := Person{
		Pets: []*Pet{{Name: "Harry"}, {Name: "Larry"}},
	}
	item := map[string]*dynamodb.AttributeValue{
		"Pets": {},
	}
	for i := range want.Pets {
		item["Pets"].L = append(item["Pets"].L, &dynamodb.AttributeValue{M: map[string]*dynamodb.AttributeValue{
			"Name": {S: &want.Pets[i].Name},
		}})
	}
	client := dynago.New(nil)
	var got Person
	if err := client.Unmarshal(item, &got); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
}

func TestUnmarshalSliceSlice(t *testing.T) {
	type T struct {
		SS [][]string
	}
	want := T{
		SS: [][]string{{"foo", "bar"}, {"bar", "baz"}},
	}
	item := map[string]*dynamodb.AttributeValue{
		"SS": {L: []*dynamodb.AttributeValue{
			{},
			{},
		}},
	}
	for i := range want.SS {
		for j := range want.SS[i] {
			item["SS"].L[i].L = append(item["SS"].L[i].L, &dynamodb.AttributeValue{S: &want.SS[i][j]})
		}
	}
	client := dynago.New(nil)
	var got T
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

func TestMarshalInt8(t *testing.T) {
	type Person struct {
		Age int8
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

func TestUnmarshalInt8(t *testing.T) {
	type Person struct {
		Age int8
	}
	want := Person{
		Age: 33,
	}
	item := map[string]*dynamodb.AttributeValue{
		"Age": {N: aws.String(strconv.FormatInt(int64(want.Age), 10))},
	}
	client := dynago.New(nil)
	var got Person
	if err := client.Unmarshal(item, &got); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
}

func TestMarshalInt16(t *testing.T) {
	type Person struct {
		Age int16
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

func TestUnmarshalInt16(t *testing.T) {
	type Person struct {
		Age int16
	}
	want := Person{
		Age: 33,
	}
	item := map[string]*dynamodb.AttributeValue{
		"Age": {N: aws.String(strconv.FormatInt(int64(want.Age), 10))},
	}
	client := dynago.New(nil)
	var got Person
	if err := client.Unmarshal(item, &got); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
}

func TestMarshalInt32(t *testing.T) {
	type Person struct {
		Age int32
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

func TestUnmarshalInt32(t *testing.T) {
	type Person struct {
		Age int32
	}
	want := Person{
		Age: 33,
	}
	item := map[string]*dynamodb.AttributeValue{
		"Age": {N: aws.String(strconv.FormatInt(int64(want.Age), 10))},
	}
	client := dynago.New(nil)
	var got Person
	if err := client.Unmarshal(item, &got); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
}

func TestMarshalInt(t *testing.T) {
	type Person struct {
		Age int
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

func TestUnmarshalInt(t *testing.T) {
	type Person struct {
		Age int
	}
	want := Person{
		Age: 33,
	}
	item := map[string]*dynamodb.AttributeValue{
		"Age": {N: aws.String(strconv.FormatInt(int64(want.Age), 10))},
	}
	client := dynago.New(nil)
	var got Person
	if err := client.Unmarshal(item, &got); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
}

func TestMarshalUint(t *testing.T) {
	type Person struct {
		Age uint
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

func TestMarshalUint8(t *testing.T) {
	type Person struct {
		Age uint8
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

func TestMarshalUint16(t *testing.T) {
	type Person struct {
		Age uint16
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

func TestMarshalUint32(t *testing.T) {
	type Person struct {
		Age uint32
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

func TestMarshalUint64(t *testing.T) {
	type Person struct {
		Age uint64
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

func TestUnmarshalUint(t *testing.T) {
	type Person struct {
		Age uint
	}
	want := Person{
		Age: 33,
	}
	item := map[string]*dynamodb.AttributeValue{
		"Age": {N: aws.String(strconv.FormatInt(int64(want.Age), 10))},
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

func TestMarshalFloat32(t *testing.T) {
	type Person struct {
		Age float32
	}
	p := Person{
		Age: 33.44567,
	}
	want := map[string]*dynamodb.AttributeValue{
		"Age": {N: aws.String("33.44567")},
	}
	client := dynago.New(nil)
	got, err := client.Marshal(&p)
	if err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
}

func TestUnmarshalFloat32(t *testing.T) {
	type Person struct {
		Age float32
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
func TestMarshalSet(t *testing.T) {
	type Person struct {
		Tags []string `type:"SS"`
	}
	p := Person{
		Tags: []string{"a", "b"},
	}
	want := map[string]*dynamodb.AttributeValue{
		"Tags": {SS: []*string{aws.String("a"), aws.String("b")}},
	}
	client := dynago.New(nil)
	got, err := client.Marshal(&p)
	if err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	assertEq(t, want, got)
}

func TestUnmarshalSet(t *testing.T) {
	type Person struct {
		Tags []string `type:"SS"`
	}
	want := Person{
		Tags: []string{"a", "b"},
	}
	item := map[string]*dynamodb.AttributeValue{
		"Tags": {SS: []*string{aws.String("a"), aws.String("b")}},
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
			"Percentage": {N: aws.String(strings.TrimRight(strconv.FormatFloat(p.Percentage, 'f', -1, 64), "0"))},
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
		"Percentage": {N: aws.String(strings.TrimRight(strconv.FormatFloat(25.323521, 'f', -1, 64), "0"))},
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
		"Percentage": {N: aws.String(strings.TrimRight(strconv.FormatFloat(25.323521, 'f', -1, 64), "0"))},
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
