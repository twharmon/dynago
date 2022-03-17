package dynago

import (
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
}

func New(config ...*Config) *Dynago {
	if len(config) == 0 {
		return &Dynago{config: &Config{
			AttributeTagName: "attribute",
			FmtTagName:       "fmt",
			TypeTagName:      "type",
			PrecTagName:      "prec",
			LayoutTagName:    "layout",
		}}
	}
	return &Dynago{config: config[0]}
}

func (d *Dynago) Unmarshal(item map[string]*dynamodb.AttributeValue, v interface{}) error {
	ty, val := tyVal(v)
	for i := 0; i < ty.NumField(); i++ {
		cfg := d.fieldConfig(ty.Field(i))
		if err := cfg.unmarshal(item, val.Field(i)); err != nil {
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
		m[cfg.attrName] = cfg.attrVal(val.Field(i))
	}
	return m
}
