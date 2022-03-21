package dynago

import (
	"errors"
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

type field struct {
	attrName    string
	attrType    string
	fmt         string
	fmtRegExps  map[string]*regexp.Regexp
	prec        int
	layout      string
	index       int
	attrsToCopy []string
	tableIndex  string
	client      *Dynago
}

func (d *Dynago) field(sf reflect.StructField, index int) (*field, error) {
	var f field
	f.index = index
	f.client = d
	if tag, ok := sf.Tag.Lookup(d.config.AttrTagName); ok {
		f.attrName = tag
	} else {
		f.attrName = sf.Name
	}
	ty := sf.Type
	for ty.Kind() == reflect.Ptr {
		ty = ty.Elem()
	}
	kind := ty.Kind()
	if tag, ok := sf.Tag.Lookup(d.config.TypeTagName); ok {
		f.attrType = tag
	} else {
		switch kind {
		case reflect.String:
			f.attrType = "S"
		case reflect.Int64, reflect.Float64:
			f.attrType = "N"
		case reflect.Bool:
			f.attrType = "BOOL"
		case reflect.Slice:
			switch ty.Elem().Kind() {
			case reflect.Uint8:
				f.attrType = "B"
			}
		case reflect.Struct:
			switch ty {
			case timeType:
				f.attrType = "S"
			default:
				f.attrType = "M"
			}
		}
	}
	if tag, ok := sf.Tag.Lookup(d.config.FmtTagName); ok {
		f.fmt = tag
	} else {
		f.fmt = "{}"
	}
	f.fmtRegExps = make(map[string]*regexp.Regexp)
	for _, match := range fmtRegExp.FindAllString(f.fmt, -1) {
		fname := trimDelims(match)
		f.fmtRegExps[fname] = regexp.MustCompile("^" + fmtRegExp.ReplaceAllString(strings.ReplaceAll(f.fmt, match, "(.*?)"), ".*?") + "$")
	}
	var err error
	if tag, ok := sf.Tag.Lookup(d.config.PrecTagName); ok {
		f.prec, err = strconv.Atoi(tag)
		if err != nil {
			return nil, fmt.Errorf("strconv.Atoi: %w", err)
		}
	} else {
		switch kind {
		case reflect.Float64:
			f.prec = 14
		}
	}
	if tag, ok := sf.Tag.Lookup(d.config.LayoutTagName); ok {
		f.layout = tag
	} else {
		switch kind {
		case reflect.Struct:
			switch ty {
			case timeType:
				f.layout = time.RFC3339
			}
		}
	}
	if tag, ok := sf.Tag.Lookup(d.config.IndexTagName); ok {
		f.tableIndex = tag
	}
	if tag, ok := sf.Tag.Lookup(d.config.AttrsToCopyTagName); ok {
		f.attrsToCopy = strings.Split(tag, ",")
	}
	return &f, nil
}

func (f *field) format(v reflect.Value, fieldIndex int) *string {
	output := f.fmt
	for _, match := range fmtRegExp.FindAllString(f.fmt, -1) {
		var fval reflect.Value
		if match == "{}" {
			fval = v.Field(fieldIndex)
		} else {
			fval = v.FieldByName(trimDelims(match))
		}
		for fval.Kind() == reflect.Ptr {
			fval = fval.Elem()
		}
		switch val := fval.Interface().(type) {
		case string:
			output = strings.ReplaceAll(output, match, val)
		case time.Time:
			output = strings.ReplaceAll(output, match, val.Format(f.layout))
		}
	}
	return &output
}

func (f *field) parse(s string, v reflect.Value) error {
	for _, match := range fmtRegExp.FindAllString(f.fmt, -1) {
		fname := trimDelims(match)
		strSubs := f.fmtRegExps[fname].FindStringSubmatch(s)
		if strSubs == nil {
			continue
		}
		str := strSubs[1]
		var fval reflect.Value
		if fname == "" {
			fval = v.Field(f.index)
		} else {
			fval = v.FieldByName(fname)
		}
		for fval.Kind() == reflect.Pointer {
			fval = fval.Elem()
		}
		fty := fval.Type()
		switch fty.Kind() {
		case reflect.String:
			fval.Set(reflect.ValueOf(str))
		case reflect.Struct:
			switch fty {
			case timeType:
				t, err := time.Parse(f.layout, str)
				if err != nil {
					return fmt.Errorf("time.Parse: %w", err)
				}
				fval.Set(reflect.ValueOf(t))
			}
		}
	}
	return nil
}

func (f *field) attrVal(v reflect.Value) (*dynamodb.AttributeValue, error) {
	fv := v.Field(f.index)
	for fv.Kind() == reflect.Ptr {
		fv = fv.Elem()
	}
	iface := fv.Interface()
	switch f.attrType {
	case "S":
		return &dynamodb.AttributeValue{S: f.format(v, f.index)}, nil
	case "N":
		switch val := iface.(type) {
		case int64:
			s := strconv.FormatInt(val, 10)
			return &dynamodb.AttributeValue{N: &s}, nil
		case float64:
			return f.float64AttrVal(val), nil
		case float32:
			return f.float32AttrVal(val), nil
		}
	case "B":
		return &dynamodb.AttributeValue{B: iface.([]byte)}, nil
	case "BOOL":
		b := iface.(bool)
		return &dynamodb.AttributeValue{BOOL: &b}, nil
	case "M":
		item, err := f.client.Marshal(iface)
		if err != nil {
			return nil, fmt.Errorf("f.client.Marshal: %w", err)
		}
		return &dynamodb.AttributeValue{M: item}, nil
	}
	return nil, errors.New("invalid attrTy")
}

func (f *field) unmarshal(item map[string]*dynamodb.AttributeValue, v reflect.Value) error {
	fv := v.Field(f.index)
	for fv.Kind() == reflect.Pointer {
		if fv.IsNil() {
			fv.Set(reflect.New(fv.Type().Elem()))
		}
		fv = fv.Elem()
	}
	switch f.attrType {
	case "S":
		if item[f.attrName] != nil && item[f.attrName].S != nil {
			if err := f.parse(*item[f.attrName].S, v); err != nil {
				return fmt.Errorf("parse: %s", err)
			}
		}
	case "N":
		if item[f.attrName] != nil && item[f.attrName].N != nil {
			ty := fv.Type()
			switch ty.Kind() {
			case reflect.Int64:
				i, err := strconv.ParseInt(*item[f.attrName].N, 10, 64)
				if err != nil {
					err = fmt.Errorf("attr: %s, strconv.ParseInt: %w", f.attrName, err)
					return err
				}
				fv.Set(reflect.ValueOf(i))
			case reflect.Float64:
				i, err := strconv.ParseFloat(*item[f.attrName].N, 64)
				if err != nil {
					err = fmt.Errorf("attr: %s, strconv.ParseFloat: %w", f.attrName, err)
					return err
				}
				fv.Set(reflect.ValueOf(i))
			case reflect.Float32:
				i, err := strconv.ParseFloat(*item[f.attrName].N, 32)
				if err != nil {
					err = fmt.Errorf("attr: %s, strconv.ParseFloat: %w", f.attrName, err)
					return err
				}
				fv.Set(reflect.ValueOf(i))
			}
		}
	case "B":
		if item[f.attrName] != nil && item[f.attrName].B != nil {
			fv.Set(reflect.ValueOf(item[f.attrName].B))
		}
	case "BOOL":
		if item[f.attrName] != nil && item[f.attrName].BOOL != nil {
			fv.Set(reflect.ValueOf(*item[f.attrName].BOOL))
		}
	case "M":
		if item[f.attrName] != nil && item[f.attrName].M != nil {
			if err := f.client.Unmarshal(item[f.attrName].M, fv.Addr().Interface()); err != nil {
				return fmt.Errorf("f.client.Unmarshal: %w", err)
			}
		}
	}
	return nil
}
