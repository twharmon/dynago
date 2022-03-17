# Dynago
![](https://github.com/twharmon/dynago/workflows/Test/badge.svg) [![](https://goreportcard.com/badge/github.com/twharmon/dynago)](https://goreportcard.com/report/github.com/twharmon/dynago) [![](https://gocover.io/_badge/github.com/twharmon/dynago)](https://gocover.io/github.com/twharmon/dynago)
The aim of this package is to make it easier to work with AWS DynamoDB.

## Documentation
For full documentation see [pkg.go.dev](https://pkg.go.dev/github.com/twharmon/dynago).

## Example
```go
package main

import (
	"fmt"

	"github.com/twharmon/dynago"
)

type Post struct {
	ID string `attribute:"PK" fmt:"Post#%s"`
	Title string
	Body string
	Category string
}

func main() {
    ddb := dynago.New()
	p := Post{
		ID:       "abc123",
		Title:    "Hi",
		Body:     "Hello world!",
		Category: "announcement",
	}
	item := ddb.Item(&p)
	fmt.Println(item)
    // item ready for DynamoDB PutItem:
    // {
	// 	"PK": {"S": "Post#abc123"},
    //     "Title": {"S": "Hi"},
    //     "Body": {"S": "Hello world!"},
    //     "Category": {"S": "announcement"},
	// }
}
```

## Benchmarks
```
BenchmarkItemNoTags-10               	  865939	      1372 ns/op	    1352 B/op	      26 allocs/op
BenchmarkItemNoTagsByHand-10         	 1664457	       720.4 ns/op	     744 B/op	       9 allocs/op
BenchmarkItemTags-10                 	  627104	      1897 ns/op	    1361 B/op	      26 allocs/op
BenchmarkItemTagsByHand-10           	 1414177	       849.8 ns/op	     792 B/op	      12 allocs/op
BenchmarkUnmarshalNoTags-10          	 1000000	      1002 ns/op	     448 B/op	      15 allocs/op
BenchmarkUnmarshalNoTagsByHand-10    	 8324685	       157.8 ns/op	       0 B/op	       0 allocs/op
```

## Contribute
Make a pull request.