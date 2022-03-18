# Dynago

![](https://github.com/twharmon/dynago/workflows/Test/badge.svg) [![](https://goreportcard.com/badge/github.com/twharmon/dynago)](https://goreportcard.com/report/github.com/twharmon/dynago) [![](https://gocover.io/_badge/github.com/twharmon/dynago)](https://gocover.io/github.com/twharmon/dynago)

The aim of this package is to make it easier to work with AWS DynamoDB.

## Documentation
For full documentation see [pkg.go.dev](https://pkg.go.dev/github.com/twharmon/dynago).

## Basic Example
```go
package main

import (
	"fmt"

	"github.com/twharmon/dynago"
)

type Post struct {
	ID      string `attr:"PK" fmt:"Post#{}"`
	Title   string
	Body    string
	Created time.Time
}

func main() {
	ddb := dynago.New(getDdbClient())
	p := Post{
		ID:       "abc123",
		Title:    "Hi",
		Body:     "Hello world!",
		Created:  time.Now(),
	}
	item, _ := ddb.Marshal(&p)
	fmt.Println(item) // map[string]*dynamodb.AttributeValue

	ddb.PutItem()

	var p2 Post
	_ = ddb.Unmarshal(item, &p2)
	fmt.Println(p2) // same as original Post
}
```

## Advanced Example
```go
package main

import (
	"fmt"

	"github.com/twharmon/dynago"
)

type Post struct {
	ID        string    `av:"SK" fmt:"Post#{}#Created#{Created}"`
	AuthorID  string    `av:"PK" fmt:"Author#{}"`
	Title     string
	Body      string
	Created   time.Time `av:"-"`
}

type Author struct {
	ID string `av:"PK" fmt:"Author#{}"`
}

func main() {
	ddb := dynago.New(&dynago.Config{
		AdditionalAttrs: additionalAttrs,
		AttrTagName:     "av",
	})
	
	// ...
}

func additionalAttrs(item map[string]*dynamodb.AttributeValue, v reflect.Value) {
	ty := v.Type().Name()

	// Add a "Type" attribute to every item
	item["Type"] = &dynamodb.AttributeValue{S: &ty}

	// Add additional attributes for specific types
	switch val := v.Interface().(type) {
	case Author:
		// Sort key identical to partition key 
		author := fmt.Sprintf("Author#%s", val.ID)
		item["SK"] = &dynamodb.AttributeValue{S: &author}

		// Add a fat partition on sparse global secondary index to
		// make querying for all authors possible
		item["GSIPK"] = &dynamodb.AttributeValue{S: &ty}
		item["GSISK"] = &dynamodb.AttributeValue{S: &author}
	}
}
```

## Todo
- types
	- Map (map, struct)
	- List (slice, array)
	- Set (slice, array)
- query builder (out of scope?)

## Benchmarks
```
BenchmarkMarshal-10            	  446264	      2313 ns/op	    1819 B/op	      30 allocs/op
BenchmarkMarshalByHand-10      	 1000000	      1120 ns/op	    1000 B/op	      15 allocs/op
BenchmarkUnmarshal-10          	  614174	      1921 ns/op	     584 B/op	      11 allocs/op
BenchmarkUnmarshalByHand-10    	 3537462	       343.4 ns/op	       0 B/op	       0 allocs/op
```

## Contribute
Make a pull request.