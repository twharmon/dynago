package dynago

import (
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

var timeType = reflect.TypeOf(time.Time{})
var fmtRegExp = regexp.MustCompile(`\{([A-Z]?[a-zA-Z0-9_]*)\}`)

type fieldConfig struct {
	attrName   string
	attrType   string
	fmt        string
	fmtRegExps map[string]*regexp.Regexp
	prec       int
	layout     string
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
		fc.fmt = "{}"
	}
	fc.fmtRegExps = make(map[string]*regexp.Regexp)
	for _, match := range fmtRegExp.FindAllString(fc.fmt, -1) {
		fname := strings.TrimPrefix(strings.TrimSuffix(match, "}"), "{")
		fc.fmtRegExps[fname] = regexp.MustCompile("^" + fmtRegExp.ReplaceAllString(strings.ReplaceAll(fc.fmt, match, "(.*?)"), ".*?") + "$")
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

func (c *fieldConfig) format(v reflect.Value, fieldIndex int) *string {
	output := c.fmt
	for _, match := range fmtRegExp.FindAllString(c.fmt, -1) {
		match = strings.TrimPrefix(strings.TrimSuffix(match, "}"), "{")
		var fval reflect.Value
		if match == "" {
			fval = v.Field(fieldIndex)
		} else {
			fval = v.FieldByName(match)
		}
		for fval.Kind() == reflect.Ptr {
			fval = fval.Elem()
		}
		switch val := fval.Interface().(type) {
		case string:
			output = strings.ReplaceAll(output, "{"+match+"}", val)
		case time.Time:
			output = strings.ReplaceAll(output, "{"+match+"}", val.Format(c.layout))
		}
	}
	return &output
}

func (c *fieldConfig) parse(s string, v reflect.Value, fieldIndex int) {
	for _, match := range fmtRegExp.FindAllString(c.fmt, -1) {
		fname := strings.TrimPrefix(strings.TrimSuffix(match, "}"), "{")
		var fval reflect.Value
		if fname == "" {
			fval = v.Field(fieldIndex)
		} else {
			fval = v.FieldByName(fname)
		}
		// re := regexp.MustCompile("^" + fmtRegExp.ReplaceAllString(strings.ReplaceAll(c.fmt, match, "(.*?)"), ".*?") + "$")
		strSubs := c.fmtRegExps[fname].FindStringSubmatch(s)
		if strSubs == nil {
			continue
		}
		str := strSubs[1]
		for fval.Kind() == reflect.Ptr {
			fval = fval.Elem()
		}
		fty := fval.Type()
		switch fty.Kind() {
		case reflect.String:
			fval.Set(reflect.ValueOf(str))
		case reflect.Struct:
			switch fty {
			case timeType:
				t, _ := time.Parse(c.layout, str)
				fval.Set(reflect.ValueOf(t))
			}
		}
	}
}

func (c *fieldConfig) attrVal(v reflect.Value, fieldIndex int) *dynamodb.AttributeValue {
	fv := v.Field(fieldIndex)
	for fv.Kind() == reflect.Ptr {
		fv = fv.Elem()
	}
	iface := fv.Interface()
	switch c.attrType {
	case "S":
		return &dynamodb.AttributeValue{S: c.format(v, fieldIndex)}
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

func (c *fieldConfig) unmarshal(item map[string]*dynamodb.AttributeValue, v reflect.Value, fieldIndex int) error {
	fv := v.Field(fieldIndex)
	for fv.Kind() == reflect.Ptr {
		fv = fv.Elem()
	}
	ty := fv.Type()
	switch c.attrType {
	case "S":
		if item[c.attrName] != nil && item[c.attrName].S != nil {
			c.parse(*item[c.attrName].S, v, fieldIndex)
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
				fv.Set(reflect.ValueOf(i))
			case reflect.Float64:
				i, err := strconv.ParseFloat(*item[c.attrName].N, 64)
				if err != nil {
					err = fmt.Errorf("attribute: %s, strconv.ParseFloat: %w", c.attrName, err)
					return err
				}
				fv.Set(reflect.ValueOf(i))
			}
		}
	case "B":
		if item[c.attrName] != nil && item[c.attrName].B != nil {
			fv.Set(reflect.ValueOf(item[c.attrName].B))
		}
	case "BOOL":
		if item[c.attrName] != nil && item[c.attrName].BOOL != nil {
			fv.Set(reflect.ValueOf(*item[c.attrName].BOOL))
		}
	}
	return nil
}
