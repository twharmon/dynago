package dynago_test

import (
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

func assertEq(t *testing.T, want, got interface{}) {
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("want: %v\n got: %v", want, got)
	}
}

type ddbMock struct {
	dynamodbiface.DynamoDBAPI
	t               *testing.T
	getItemInput    *dynamodb.GetItemInput
	getItemOutput   *dynamodb.GetItemOutput
	queryInput      *dynamodb.QueryInput
	queryOutput     *dynamodb.QueryOutput
	scanInput       *dynamodb.ScanInput
	scanOutput      *dynamodb.ScanOutput
	putItemInput    *dynamodb.PutItemInput
	deleteItemInput *dynamodb.DeleteItemInput
	txWriteInput    *dynamodb.TransactWriteItemsInput
	updateItemInput *dynamodb.UpdateItemInput
}

func (m *ddbMock) GetItem(i *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
	if !reflect.DeepEqual(i, m.getItemInput) {
		m.t.Fatalf("want %v; got %v", m.getItemInput, i)
	}
	o := *m.getItemOutput
	m.getItemInput = nil
	m.getItemOutput = nil
	return &o, nil
}

func (m *ddbMock) MockGet(i *dynamodb.GetItemInput, o *dynamodb.GetItemOutput) {
	m.getItemInput = i
	m.getItemOutput = o
}

func (m *ddbMock) MockTransactWriteItems(i *dynamodb.TransactWriteItemsInput) {
	m.txWriteInput = i
}

func (m *ddbMock) TransactWriteItems(i *dynamodb.TransactWriteItemsInput) (*dynamodb.TransactWriteItemsOutput, error) {
	if !reflect.DeepEqual(i, m.txWriteInput) {
		m.t.Fatalf("want %v; got %v", m.txWriteInput, i)
	}
	m.txWriteInput = nil
	return nil, nil
}

func (m *ddbMock) done() {
	if m.getItemInput != nil {
		m.t.Fatalf("expectations not met")
	}
	if m.putItemInput != nil {
		m.t.Fatalf("expectations not met")
	}
	if m.deleteItemInput != nil {
		m.t.Fatalf("expectations not met")
	}
	if m.queryInput != nil {
		m.t.Fatalf("expectations not met")
	}
	if m.scanInput != nil {
		m.t.Fatalf("expectations not met")
	}
	if m.getItemOutput != nil {
		m.t.Fatalf("expectations not met")
	}
	if m.txWriteInput != nil {
		m.t.Fatalf("expectations not met")
	}
	if m.updateItemInput != nil {
		m.t.Fatalf("expectations not met")
	}
}

func (m *ddbMock) PutItem(i *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
	if !reflect.DeepEqual(i, m.putItemInput) {
		m.t.Fatalf("want %v; got %v", m.putItemInput, i)
	}
	m.putItemInput = nil
	return nil, nil
}

func (m *ddbMock) MockPut(i *dynamodb.PutItemInput) {
	m.putItemInput = i
}

func (m *ddbMock) UpdateItem(i *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error) {
	if !reflect.DeepEqual(i, m.updateItemInput) {
		m.t.Fatalf("want %v; got %v", m.updateItemInput, i)
	}
	m.updateItemInput = nil
	return nil, nil
}

func (m *ddbMock) MockUpdate(i *dynamodb.UpdateItemInput) {
	m.updateItemInput = i
}

func (m *ddbMock) DeleteItem(i *dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error) {
	if !reflect.DeepEqual(i, m.deleteItemInput) {
		m.t.Fatalf("want %v; got %v", m.deleteItemInput, i)
	}
	m.deleteItemInput = nil
	return nil, nil
}

func (m *ddbMock) MockDelete(i *dynamodb.DeleteItemInput) {
	m.deleteItemInput = i
}

func mock(t *testing.T) *ddbMock {
	m := ddbMock{t: t}
	return &m
}

func (m *ddbMock) Query(i *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
	if !reflect.DeepEqual(i, m.queryInput) {
		m.t.Fatalf("want %v; got %v", m.queryInput, i)
	}
	o := *m.queryOutput
	m.queryInput = nil
	m.queryOutput = nil
	return &o, nil
}

func (m *ddbMock) Scan(i *dynamodb.ScanInput) (*dynamodb.ScanOutput, error) {
	if !reflect.DeepEqual(i, m.scanInput) {
		m.t.Fatalf("want %v; got %v", m.scanInput, i)
	}
	o := *m.scanOutput
	m.scanInput = nil
	m.scanOutput = nil
	return &o, nil
}

func (m *ddbMock) MockScan(i *dynamodb.ScanInput, o *dynamodb.ScanOutput) {
	m.scanInput = i
	m.scanOutput = o
}

func (m *ddbMock) MockQuery(i *dynamodb.QueryInput, o *dynamodb.QueryOutput) {
	m.queryInput = i
	m.queryOutput = o
}
