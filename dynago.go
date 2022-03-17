package dynago

import (
	"reflect"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type Dynago struct {
	config *Config
}

// Config is used to customize struct tag names.
type Config struct {
	// AttributeTagName specifies which tag is used for a DynamoDB
	// item attribute name.
	AttributeTagName string

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

	// AdditionalAttributes can be added for each dynamodb item.
	AdditionalAttributes func(reflect.Value) map[string]*dynamodb.AttributeValue
}

func New(config ...*Config) *Dynago {
	if len(config) == 0 {
		return &Dynago{config: &Config{
			AttributeTagName: "attribute",
			FmtTagName:       "fmt",
			TypeTagName:      "type",
			PrecTagName:      "prec",
			LayoutTagName:    "layout",
			AdditionalAttributes: func(v reflect.Value) map[string]*dynamodb.AttributeValue {
				return nil
			},
		}}
	}
	return &Dynago{config: config[0]}
}

func (d *Dynago) Unmarshal(item map[string]*dynamodb.AttributeValue, v interface{}) error {
	ty, val := tyVal(v)
	for i := 0; i < ty.NumField(); i++ {
		cfg := d.fieldConfig(ty.Field(i))
		if cfg.attrName == "-" {
			continue
		}
		if err := cfg.unmarshal(item, val, i); err != nil {
			return err
		}
	}
	return nil
}

func (d *Dynago) Item(v interface{}) map[string]*dynamodb.AttributeValue {
	m := make(map[string]*dynamodb.AttributeValue)
	ty, val := tyVal(v)
	for i := 0; i < ty.NumField(); i++ {
		cfg := d.fieldConfig(ty.Field(i))
		if cfg.attrName == "-" {
			continue
		}
		m[cfg.attrName] = cfg.attrVal(val, i)
	}
	if add := d.config.AdditionalAttributes(val); add != nil {
		for k, v := range add {
			m[k] = v
		}
	}
	return m
}
