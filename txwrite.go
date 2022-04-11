package dynago

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// TransactionWriteItemer interface is to provide a
// dynamodb.TransactWriteItem.
type TransactionWriteItemer interface {
	TransactionWriteItem() (*dynamodb.TransactWriteItem, error)
}

// TransactionWriteItems represents a TransactWriteItems operation.
type TransactionWriteItems struct {
	input  *dynamodb.TransactWriteItemsInput
	items  []TransactionWriteItemer
	client *Dynago
}

// TransactionWriteItems returns a TransactionWriteItems operation.
func (d *Dynago) TransactionWriteItems() *TransactionWriteItems {
	var i TransactionWriteItems
	i.input = &dynamodb.TransactWriteItemsInput{}
	i.client = d
	return &i
}

// Items adds items to the transaction.
func (i *TransactionWriteItems) Items(items ...TransactionWriteItemer) *TransactionWriteItems {
	i.items = append(i.items, items...)
	return i
}

// ClientRequestToken sets the ClientRequestToken.
func (i *TransactionWriteItems) ClientRequestToken(token string) *TransactionWriteItems {
	i.input.ClientRequestToken = &token
	return i
}

// Exec executes the operation.
func (i *TransactionWriteItems) Exec() error {
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
