package dynago

import (
	"fmt"
	"reflect"
	"sync"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

type Dynago struct {
	config   *Config
	cache    map[string]map[int]*field
	cacheMtx sync.Mutex
	ddb      dynamodbiface.DynamoDBAPI
}

// Config is used to customize struct tag names.
type Config struct {
	// AttrTagName specifies which tag is used for a DynamoDB
	// item attribute name.
	AttrTagName string

	// FmtTagName specifies which tag is used to format the attribute
	// value. This is only used for String types.
	FmtTagName string

	// TypeTagName specifies which tag is used for DynamoDB type.
	TypeTagName string

	// LayoutTagName specifies which tag is used for formatting time
	// values.
	LayoutTagName string

	// IndexTagName specifies which tag is used for table index name.
	IndexTagName string

	// AttrsToCopyTagName specifies which tag is used to determine
	// which other attributes should have same value.
	AttrsToCopyTagName string

	// AdditionalAttrs can be added for each dynamodb item.
	AdditionalAttrs func(map[string]*dynamodb.AttributeValue, reflect.Value)

	// DefaultTableName is the default table queried when not
	// specified.
	DefaultTableName string

	// DefaultConsistentRead is the default read consistency model.
	DefaultConsistentRead bool
}

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

func (d *Dynago) Marshal(v interface{}) (map[string]*dynamodb.AttributeValue, error) {
	m := make(map[string]*dynamodb.AttributeValue)
	ty, val := tyVal(v)
	cache, err := d.cachedStruct(ty)
	if err != nil {
		return nil, fmt.Errorf("d.cachedStruct: %w", err)
	}
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
		m[cache[i].attrName] = attrVal
		for _, cp := range cache[i].attrsToCopy {
			m[cp] = attrVal
		}
	}
	if d.config.AdditionalAttrs != nil {
		d.config.AdditionalAttrs(m, val)
	}
	return m, nil
}

func (d *Dynago) key(v interface{}, index ...string) (map[string]*dynamodb.AttributeValue, error) {
	m := make(map[string]*dynamodb.AttributeValue)
	ty, val := tyVal(v)
	cache, err := d.cachedStruct(ty)
	if err != nil {
		return nil, fmt.Errorf("d.cachedStruct: %w", err)
	}
	idx := "primary"
	if len(index) > 0 {
		idx = index[0]
	}
	for i := 0; i < ty.NumField(); i++ {
		if cache[i].tableIndex != idx {
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
		for _, cp := range cache[i].attrsToCopy { // TODO: won't work if copy to non-primary key attr
			m[cp] = attrVal
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
