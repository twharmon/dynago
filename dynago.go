package dynago

import (
	"fmt"
	"reflect"
	"sync"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type Dynago struct {
	config   *Config
	cache    map[string]map[int]*field
	cacheMtx sync.Mutex
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

	// PrecTagName specifies which tag is used for floating point.
	// number precision
	PrecTagName string

	// LayoutTagName specifies which tag is used for formatting time
	// values.
	LayoutTagName string

	// AdditionalAttrs can be added for each dynamodb item.
	AdditionalAttrs func(reflect.Value) map[string]*dynamodb.AttributeValue
}

func New(config ...*Config) *Dynago {
	if len(config) == 0 {
		return &Dynago{
			config: &Config{
				AttrTagName:   "attr",
				FmtTagName:    "fmt",
				TypeTagName:   "type",
				PrecTagName:   "prec",
				LayoutTagName: "layout",
				AdditionalAttrs: func(v reflect.Value) map[string]*dynamodb.AttributeValue {
					return nil
				},
			},
			cache: make(map[string]map[int]*field),
		}
	}
	return &Dynago{
		config: config[0],
		cache:  make(map[string]map[int]*field),
	}
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
		m[cache[i].attrName] = cache[i].attrVal(val)
	}
	if add := d.config.AdditionalAttrs(val); add != nil {
		for k, v := range add {
			m[k] = v
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
