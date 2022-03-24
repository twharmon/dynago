package dynago

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type TransactionWriteItemer interface {
	TransactionWriteItem() (*dynamodb.TransactWriteItem, error)
}

type TransactionWriteItem struct {
	input  *dynamodb.TransactWriteItemsInput
	items  []TransactionWriteItemer
	client *Dynago
}

func (d *Dynago) TransactWriteItems() *TransactionWriteItem {
	var i TransactionWriteItem
	i.input = &dynamodb.TransactWriteItemsInput{}
	i.client = d
	return &i
}

func (i *TransactionWriteItem) Items(items ...TransactionWriteItemer) *TransactionWriteItem {
	i.items = append(i.items, items...)
	return i
}

func (i *TransactionWriteItem) ClientRequestToken(token string) *TransactionWriteItem {
	i.input.ClientRequestToken = &token
	return i
}

func (i *TransactionWriteItem) Exec() error {
	for _, item := range i.items {
		txitem, err := item.TransactionWriteItem()
		if err != nil {
			return err
		}
		i.input.TransactItems = append(i.input.TransactItems, txitem)
	}
	_, err := i.client.ddb.TransactWriteItems(i.input)
	return err
}
