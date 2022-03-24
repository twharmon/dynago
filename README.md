# Dynago

![](https://github.com/twharmon/dynago/workflows/Test/badge.svg) [![](https://goreportcard.com/badge/github.com/twharmon/dynago)](https://goreportcard.com/report/github.com/twharmon/dynago) [![](https://gocover.io/_badge/github.com/twharmon/dynago)](https://gocover.io/github.com/twharmon/dynago)

The aim of this package is to make it easier to work with AWS DynamoDB.

## Documentation
For full documentation see [pkg.go.dev](https://pkg.go.dev/github.com/twharmon/dynago).

## Usage

### Basic
```go
package main

import (
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/twharmon/dynago"
)

type Post struct {
	// Set attribute name with `attr` tag if it needs to be different
	// that field name. Specify that this attribute is part of
	// primary key with `idx:"primary"`. Use `fmt:"Post#{}"` to
	// indicate how the value will be stored in DynamoDB.
	ID string `idx:"primary" attr:"PK" fmt:"Post#{}"`

	Created  time.Time `idx:"primary" attr:"SK" fmt:"Created#{}"`
	AuthorID string
	Title    string
	Body     string
}

func main() {
	// Get client.
	ddb := dynago.New(getDynamoDB(), &dynago.Config{
		DefaultTableName: "tmp",
	})

	// Put item in DynamoDB.
	p := Post{
		ID:      "hello-world",
		Title:   "Hi",
		Body:    "Hello world!",
		Created: time.Now(),
	}
	if err := ddb.Put(&p).Exec(); err != nil {
		panic(err)
	}

	// Get same item from DynamoDB. Fields used in the primary key
	// must be set.
	p2 := Post{
		ID:      p.ID,
		Created: p.Created,
	}
	if err := ddb.Get(&p2).Exec(); err != nil {
		panic(err)
	}
	fmt.Println(p2)
}

func getDynamoDB() *dynamodb.DynamoDB {
	os.Setenv("AWS_SDK_LOAD_CONFIG", "true")
	sess, err := session.NewSession()
	if err != nil {
		panic(err)
	}
	return dynamodb.New(sess)
}
```

### Additional Attributes
```go
package main

import (
	"fmt"
	"os"
	"reflect"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/twharmon/dynago"
)

type Post struct {
	ID       string `idx:"primary" attr:"PK" fmt:"Post#{}"`
	AuthorID string
	Title    string
	Body     string
	Created  time.Time `idx:"primary" attr:"SK" fmt:"Created#{}"`
}

type Author struct {
	// Copy same value to attribute SK by using `copy:"SK"` in tag.
	ID   string `idx:"primary" attr:"PK" fmt:"Author#{}" copy:"SK"`
	Name string
}

func main() {
	// Get client.
	ddb := dynago.New(getDynamoDB(), &dynago.Config{
		DefaultTableName: "tmp",
		AdditionalAttrs:  additionalAttrs,
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
		// Add a fat partition on sparse global secondary index to
		// make querying for all authors possible
		author := fmt.Sprintf("Author#%s", val.ID)
		item["GSIPK"] = &dynamodb.AttributeValue{S: &ty}
		item["GSISK"] = &dynamodb.AttributeValue{S: &author}
	}
}
```

### Compound Field Attributes
```go
type Event struct {
	Org string `attr:"PK" idx:"primary" fmt:"Org#{}"`

	// In this `fmt` tag, {} is equivalent to {Country}. You can
	// reference a different field name by putting it's name in
	// curly brackets.
	Country string `idx:"primary" attr:"SK" fmt:"Country#{}#City#{City}"`

	// Since the City is specified in the "SK" attribute, we can
	// skip putting it in another attribute if we want.
	City    string `attr:"-"`
	Created time.Time
}
```

## Todo
- format types
	- numbers
- query builder
	- scan
	- update
	- transactions
	- misc
		- add all sdk features

## Benchmarks
```
BenchmarkMarshal-10            	  472528	      2180 ns/op	    1733 B/op	      27 allocs/op
BenchmarkMarshalByHand-10      	 1000000	      1049 ns/op	     976 B/op	      14 allocs/op
BenchmarkUnmarshal-10          	  625406	      1862 ns/op	     578 B/op	      11 allocs/op
BenchmarkUnmarshalByHand-10    	 3540060	       344.1 ns/op	       0 B/op	       0 allocs/op
```

## Contribute
Make a pull request.