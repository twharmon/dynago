package dynago

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type Dynago struct {
	config *Config
}

type Config struct {
	AttributeTagName string
	FmtTagName       string
	TypeTagName      string
	PrecTagName      string
}

func New(config ...*Config) *Dynago {
	if len(config) == 0 {
		return &Dynago{config: &Config{
			AttributeTagName: "attribute",
			FmtTagName:       "fmt",
			TypeTagName:      "type",
			PrecTagName:      "prec",
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
