# Dynago

![](https://github.com/twharmon/dynago/workflows/Test/badge.svg) [![](https://goreportcard.com/badge/github.com/twharmon/dynago)](https://goreportcard.com/report/github.com/twharmon/dynago) [![codecov](https://codecov.io/gh/twharmon/dynago/branch/main/graph/badge.svg?token=K0P59TPRAL)](https://codecov.io/gh/twharmon/dynago)

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

type Schema struct {}

func (s *Schema) PrimaryKeys() []string {
	return []string{"PK", "SK"}
}

type Post struct {
	// Embed a struct that implements the dynago.Keyer interface.
	*Schema

	// Set attribute name with `attr` tag if it needs to be different
	// than field name. Use `fmt:"Post#{}"` to indicate how the value
	// will be stored in DynamoDB.
	ID string `attr:"PK" fmt:"Post#{}"`

	Created  time.Time `attr:"SK" fmt:"Created#{}"`
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
	if err := ddb.PutItem(&p).Exec(); err != nil {
		panic(err)
	}

	// Get same item from DynamoDB. Fields used in the primary key
	// must be set.
	p2 := Post{
		ID:      p.ID,
		Created: p.Created,
	}
	if err := ddb.GetItem(&p2).Exec(); err != nil {
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

type Schema struct {}

func (s *Schema) PrimaryKeys() []string {
	return []string{"PK", "SK"}
}

type Post struct {
	// Embed a struct that implements the dynago.Keyer interface.
	*Schema

	ID       string `attr:"PK" fmt:"Post#{}"`
	AuthorID string
	Title    string
	Body     string
	Created  time.Time `attr:"SK" fmt:"Created#{}"`
}

type Author struct {
	// Copy same value to attribute SK by using `copyidx:"SK"` in tag,
	// while also specifying that SK is part of the same index.
	ID   string `attr:"PK" fmt:"Author#{}"`

	// Copy same value to attribute AltName by using `copy:"AltName"` in tag.
	Name string `copy:"AltName"`
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
	Org string `attr:"PK" fmt:"Org#{}"`

	// In this `fmt` tag, {} is equivalent to {Country}. You can
	// reference a different field name by putting it's name in
	// curly brackets.
	Country string `attr:"SK" fmt:"Country#{}#City#{City}"`

	// Since the City is specified in the "SK" attribute, we can
	// skip putting it in another attribute if we want.
	City    string `attr:"-"`
	Created time.Time
}
```

## Contribute
Make a pull request.
