package dynago_test

import (
	"testing"

	"github.com/twharmon/dynago"
)

// BenchmarkMarshall-10      	 1279446	       924.3 ns/op	     867 B/op	      10 allocs/op
// BenchmarkUnmarshall-10    	 1500501	       801.1 ns/op	     254 B/op	       6 allocs/op

func BenchmarkMarshall(b *testing.B) {
	ddb := mock(b)
	client := dynago.New(ddb)

	type Person struct {
		*CompositeTable
		Name string `attr:"PK" fmt:"Person#{}"`
		Age  int64
	}
	person := Person{
		Name: "Gopher",
		Age:  14,
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := client.Marshal(person); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkUnmarshall(b *testing.B) {
	ddb := mock(b)
	client := dynago.New(ddb)

	type Person struct {
		*CompositeTable
		Name string `attr:"PK" fmt:"Person#{}"`
		Age  int64
	}
	person := Person{
		Name: "Gopher",
		Age:  14,
	}
	av, err := client.Marshal(person)
	if err != nil {
		b.Fatal(err)
	}
	var output Person
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := client.Unmarshal(av, &output); err != nil {
			b.Fatal(err)
		}
	}
}
