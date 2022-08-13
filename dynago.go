package dynago

import (
	"fmt"
	"reflect"
	"sync"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/twharmon/slices"
)

// DynagoAPI provides an interface to enable mocking the
// dynago.Dynago service client's API operations. This make unit
// testing your code easier.
type DynagoAPI interface {
	DeleteItem(interface{}) *DeleteItem
	PutItem(interface{}) *PutItem
	GetItem(interface{}) *GetItem
	Query(interface{}) *Query
	Scan(interface{}) *Scan
	UpdateItem(interface{}) *UpdateItem
	ConditionCheck(interface{}) *ConditionCheck
	TransactionWriteItems() *TransactionWriteItems
	Marshal(interface{}) (map[string]*dynamodb.AttributeValue, error)
	Unmarshal(map[string]*dynamodb.AttributeValue, interface{}) error
}

// Dynago is
type Dynago struct {
	config   *Config
	cache    map[string]map[int]*field
	cacheMtx sync.Mutex
	ddb      dynamodbiface.DynamoDBAPI
}

// Config is used to customize struct tag names.
type Config struct {
	// AttrTagName specifies which tag is used for a DynamoDB
	// item attribute name. Defaults to "attr".
	AttrTagName string

	// FmtTagName specifies which tag is used to format the attribute
	// value. This is only used for String types. Defaults to "fmt".
	FmtTagName string

	// TypeTagName specifies which tag is used for DynamoDB type.
	// Defaults to "type".
	TypeTagName string

	// LayoutTagName specifies which tag is used for formatting time
	// values. Defaults to "layout".
	LayoutTagName string

	// IndexTagName specifies which tag is used for table index name.
	// Defaults to "idx".
	IndexTagName string

	// AttrsToCopyTagName specifies which tag is used to determine
	// which other attributes should have same value. Defaults to
	// "copy".
	AttrsToCopyTagName string

	// AttrsToCopyIndexTagName specifies which tag is used to
	// determine which other attributes should have same index.
	// Defaults to "copyidx".
	AttrsToCopyIndexTagName string

	// AdditionalAttrs can be added for each dynamodb item.
	AdditionalAttrs func(map[string]*dynamodb.AttributeValue, reflect.Value)

	// DefaultTableName is the default table queried when not
	// specified.
	DefaultTableName string

	// DefaultConsistentRead is the default read consistency model.
	DefaultConsistentRead bool
}

// New creates a new Dynago client. An optional config can be passed
// in second argument.
func New(ddb dynamodbiface.DynamoDBAPI, config ...*Config) *Dynago {
	d := Dynago{
		cache:  make(map[string]map[int]*field),
		config: &Config{},
	}
	if len(config) > 0 {
		d.config = config[0]
	}
	if d.config.AttrTagName == "" {
		d.config.AttrTagName = "attr"
	}
	if d.config.AttrsToCopyTagName == "" {
		d.config.AttrsToCopyTagName = "copy"
	}
	if d.config.AttrsToCopyIndexTagName == "" {
		d.config.AttrsToCopyIndexTagName = "copyidx"
	}
	if d.config.FmtTagName == "" {
		d.config.FmtTagName = "fmt"
	}
	if d.config.TypeTagName == "" {
		d.config.TypeTagName = "type"
	}
	if d.config.LayoutTagName == "" {
		d.config.LayoutTagName = "layout"
	}
	if d.config.IndexTagName == "" {
		d.config.IndexTagName = "idx"
	}
	d.ddb = ddb
	return &d
}

// Unmarshal converts a DynamoDB item into a Go struct.
func (d *Dynago) Unmarshal(item map[string]*dynamodb.AttributeValue, v interface{}) error {
	ty, val := tyVal(v)
	cache, err := d.cachedStruct(ty)
	if err != nil {
		return fmt.Errorf("d.cachedStruct: %w", err)
	}
	for i := 0; i < ty.NumField(); i++ {
		if cache[i].attrName == "-" {
			continue
		}
		if err := cache[i].unmarshal(item, val); err != nil {
			return err
		}
	}
	return nil
}

// Marshal converts a Go struct into a DynamoDB item.
func (d *Dynago) Marshal(v interface{}) (map[string]*dynamodb.AttributeValue, error) {
	m := make(map[string]*dynamodb.AttributeValue)
	ty, val := tyVal(v)
	cache, err := d.cachedStruct(ty)
	if err != nil {
		return nil, fmt.Errorf("d.cachedStruct: %w", err)
	}
	isTopLevel := false
	for i := 0; i < ty.NumField(); i++ {
		if cache[i].attrName == "-" {
			continue
		}
		attrVal, err := cache[i].attrVal(val)
		if err != nil {
			return nil, fmt.Errorf("cache.attrVal: %w", err)
		}
		if attrVal == nil {
			continue
		}
		if !isTopLevel && cache[i].tableIndex != "" {
			isTopLevel = true
		}
		m[cache[i].attrName] = attrVal
		for _, cp := range cache[i].attrsToCopy {
			m[cp] = attrVal
		}
	}
	if isTopLevel && d.config.AdditionalAttrs != nil {
		d.config.AdditionalAttrs(m, val)
	}
	return m, nil
}

func (d *Dynago) key(v interface{}) (map[string]*dynamodb.AttributeValue, error) {
	m := make(map[string]*dynamodb.AttributeValue)
	ty, val := tyVal(v)
	cache, err := d.cachedStruct(ty)
	if err != nil {
		return nil, fmt.Errorf("d.cachedStruct: %w", err)
	}
	for i := 0; i < ty.NumField(); i++ {
		if cache[i].tableIndex != "primary" {
			continue
		}
		if cache[i].attrName == "-" {
			continue
		}
		attrVal, err := cache[i].attrVal(val)
		if err != nil {
			return nil, fmt.Errorf("cache.attrVal: %w", err)
		}
		if attrVal == nil {
			continue
		}
		m[cache[i].attrName] = attrVal
		// TODO: any way to avoid nested loop?
		if copyIdx := cache[i].attrToCopyIdx; copyIdx != "" {
			for j := 0; j < ty.NumField(); j++ {
				if cache[j].attrName == copyIdx || slices.Contains(cache[j].attrsToCopy, copyIdx) {
					av, err := cache[j].attrVal(val)
					if err != nil {
						return nil, fmt.Errorf("cache.attrVal: %w", err)
					}
					m[copyIdx] = av
					break
				}
			}
		}
	}
	return m, nil
}

func (d *Dynago) cachedStruct(ty reflect.Type) (map[int]*field, error) {
	key := ty.String()
	d.cacheMtx.Lock()
	defer d.cacheMtx.Unlock()
	if d.cache[key] == nil {
		d.cache[key] = make(map[int]*field)
		for i := 0; i < ty.NumField(); i++ {
			cfg, err := d.field(ty.Field(i), i)
			if err != nil {
				return nil, fmt.Errorf("d.field")
			}
			d.cache[key][i] = cfg
		}
	}
	return d.cache[key], nil
}
