package dynago

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type Key struct {
}

func SimpleKey() *Key {
	return &Key{}
}

func (k *Key) Key() map[string]*dynamodb.AttributeValue {
	return map[string]*dynamodb.AttributeValue{}
}
