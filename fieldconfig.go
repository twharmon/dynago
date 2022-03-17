package dynago

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

var timeType = reflect.TypeOf(time.Time{})

type fieldConfig struct {
	attrName string
	attrType string
	fmt      string
	prec     int
	layout   string
}

func (d *Dynago) fieldConfig(sf reflect.StructField) *fieldConfig {
	var fc fieldConfig
	if tag, ok := sf.Tag.Lookup(d.config.AttributeTagName); ok {
		fc.attrName = tag
	} else {
		fc.attrName = sf.Name
	}
	ty := sf.Type
	for ty.Kind() == reflect.Ptr {
		ty = ty.Elem()
	}
	kind := ty.Kind()
	if tag, ok := sf.Tag.Lookup(d.config.TypeTagName); ok {
		fc.attrType = tag
	} else {
		switch kind {
		case reflect.String:
			fc.attrType = "S"
		case reflect.Int64, reflect.Float64:
			fc.attrType = "N"
		case reflect.Bool:
			fc.attrType = "BOOL"
		case reflect.Slice:
			switch ty.Elem().Kind() {
			case reflect.Uint8:
				fc.attrType = "B"
			}
		case reflect.Struct:
			switch ty {
			case timeType:
				fc.attrType = "S"
			}
		}
	}
	if tag, ok := sf.Tag.Lookup(d.config.FmtTagName); ok {
		fc.fmt = tag
	} else {
		switch kind {
		case reflect.String:
			fc.fmt = "%s"
		case reflect.Struct:
			switch ty {
			case timeType:
				fc.fmt = "%s"
			}
		}
	}
	if tag, ok := sf.Tag.Lookup(d.config.PrecTagName); ok {
		fc.prec, _ = strconv.Atoi(tag)
	} else {
		switch kind {
		case reflect.Float64:
			fc.prec = 14
		}
	}
	if tag, ok := sf.Tag.Lookup(d.config.LayoutTagName); ok {
		fc.layout = tag
	} else {
		switch kind {
		case reflect.Struct:
			switch ty {
			case timeType:
				fc.layout = time.RFC3339
			}
		}
	}
	return &fc
}

func (c *fieldConfig) attrVal(v reflect.Value) *dynamodb.AttributeValue {
	for v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	iface := v.Interface()
	switch c.attrType {
	case "S":
		switch val := iface.(type) {
		case time.Time:
			iface = val.Format(c.layout)
		}
		return &dynamodb.AttributeValue{S: aws.String(fmt.Sprintf(c.fmt, iface))}
	case "N":
		switch val := iface.(type) {
		case int64:
			s := strconv.FormatInt(val, 10)
			return &dynamodb.AttributeValue{N: &s}
		case float64:
			s := strconv.FormatFloat(val, 'f', c.prec, 64)
			s = strings.TrimRight(s, "0")
			return &dynamodb.AttributeValue{N: &s}
		}
	case "B":
		return &dynamodb.AttributeValue{B: iface.([]byte)}
	case "BOOL":
		b := iface.(bool)
		return &dynamodb.AttributeValue{BOOL: &b}
	}
	panic("invalid attrTy")
}

func (c *fieldConfig) unmarshal(item map[string]*dynamodb.AttributeValue, v reflect.Value) error {
	for v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	ty := v.Type()
	switch c.attrType {
	case "S":
		if item[c.attrName] != nil && item[c.attrName].S != nil {
			str := *item[c.attrName].S
			fmt.Sscanf(str, c.fmt, &str)
			switch ty.Kind() {
			case reflect.String:
				v.Set(reflect.ValueOf(str))
			case reflect.Struct:
				switch ty {
				case timeType:
					t, err := time.Parse(c.layout, str)
					if err != nil {
						err = fmt.Errorf("attribute: %s, time.Parse: %w", c.attrName, err)
						return err
					}
					v.Set(reflect.ValueOf(t))
				}
			}
		}
	case "N":
		if item[c.attrName] != nil && item[c.attrName].N != nil {
			switch ty.Kind() {
			case reflect.Int64:
				i, err := strconv.ParseInt(*item[c.attrName].N, 10, 64)
				if err != nil {
					err = fmt.Errorf("attribute: %s, strconv.ParseInt: %w", c.attrName, err)
					return err
				}
				v.Set(reflect.ValueOf(i))
			case reflect.Float64:
				i, err := strconv.ParseFloat(*item[c.attrName].N, 64)
				if err != nil {
					err = fmt.Errorf("attribute: %s, strconv.ParseFloat: %w", c.attrName, err)
					return err
				}
				v.Set(reflect.ValueOf(i))
			}
		}
	case "B":
		if item[c.attrName] != nil && item[c.attrName].B != nil {
			v.Set(reflect.ValueOf(item[c.attrName].B))
		}
	case "BOOL":
		if item[c.attrName] != nil && item[c.attrName].BOOL != nil {
			v.Set(reflect.ValueOf(*item[c.attrName].BOOL))
		}
	}
	return nil
}
