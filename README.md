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
BenchmarkItem-10               	  438477	      2476 ns/op	    1793 B/op	      34 allocs/op
BenchmarkItemByHand-10         	 1000000	      1122 ns/op	    1000 B/op	      15 allocs/op
BenchmarkUnmarshal-10          	  450906	      2586 ns/op	     752 B/op	      22 allocs/op
BenchmarkUnmarshalByHand-10    	 3552892	       343.0 ns/op	       0 B/op	       0 allocs/op
```

## Contribute
Make a pull request.