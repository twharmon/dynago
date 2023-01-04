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

var timeType = reflect.TypeOf(time.Now())
var durationType = reflect.TypeOf(time.Second)
var fmtRegExp = regexp.MustCompile(`\{([A-Z]?[a-zA-Z0-9_]*)\}`)

type field struct {
	attrName    string
	attrType    string
	fmt         string
	fmtRegExps  map[string]*regexp.Regexp
	layout      string
	index       int
	attrsToCopy []string
	client      *Dynago
}

func (d *Dynago) field(sf reflect.StructField, index int) (*field, error) {
	var f field
	f.index = index
	f.client = d
	if sf.IsExported() {
		if tag, ok := sf.Tag.Lookup(d.config.AttrTagName); ok {
			f.attrName = tag
		} else {
			f.attrName = sf.Name
		}
	} else {
		f.attrName = "-"
		return &f, nil
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
		case reflect.Int, reflect.Uint, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float64, reflect.Float32:
			f.attrType = "N"
		case reflect.Bool:
			f.attrType = "BOOL"
		case reflect.Slice:
			switch ty.Elem().Kind() {
			case reflect.Uint8:
				f.attrType = "B"
			default:
				f.attrType = "L"
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
		f.attrType = "S"
	} else {
		f.fmt = "{}"
	}
	f.fmtRegExps = make(map[string]*regexp.Regexp)
	for _, match := range fmtRegExp.FindAllString(f.fmt, -1) {
		fname := trimDelims(match)
		f.fmtRegExps[fname] = regexp.MustCompile("(?s)^" + fmtRegExp.ReplaceAllString(strings.ReplaceAll(f.fmt, match, "(.*?)"), ".*?") + "$")
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
		case reflect.Slice:
			elTy := ty.Elem()
			for elTy.Kind() == reflect.Ptr {
				elTy = elTy.Elem()
			}
			switch elTy {
			case timeType:
				f.layout = time.RFC3339
			}
		}
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
		case int:
			output = strings.ReplaceAll(output, match, strconv.FormatInt(int64(val), 10))
		case uint:
			output = strings.ReplaceAll(output, match, strconv.FormatInt(int64(val), 10))
		case int8:
			output = strings.ReplaceAll(output, match, strconv.FormatInt(int64(val), 10))
		case uint8:
			output = strings.ReplaceAll(output, match, strconv.FormatInt(int64(val), 10))
		case int16:
			output = strings.ReplaceAll(output, match, strconv.FormatInt(int64(val), 10))
		case uint16:
			output = strings.ReplaceAll(output, match, strconv.FormatInt(int64(val), 10))
		case int32:
			output = strings.ReplaceAll(output, match, strconv.FormatInt(int64(val), 10))
		case uint32:
			output = strings.ReplaceAll(output, match, strconv.FormatInt(int64(val), 10))
		case int64:
			output = strings.ReplaceAll(output, match, strconv.FormatInt(val, 10))
		case uint64:
			output = strings.ReplaceAll(output, match, strconv.FormatInt(int64(val), 10))
		case float32:
			output = strings.ReplaceAll(output, match, strconv.FormatFloat(float64(val), 'f', -1, 32))
		case float64:
			output = strings.ReplaceAll(output, match, strconv.FormatFloat(val, 'f', -1, 64))
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
		for fval.Kind() == reflect.Ptr {
			fval = fval.Elem()
		}
		fty := fval.Type()
		switch fty.Kind() {
		case reflect.String:
			fval.Set(reflect.ValueOf(str))
		case reflect.Int:
			val, err := strconv.ParseInt(str, 10, 64)
			if err != nil {
				return err
			}
			fval.Set(reflect.ValueOf(int(val)))
		case reflect.Uint:
			val, err := strconv.ParseInt(str, 10, 64)
			if err != nil {
				return err
			}
			fval.Set(reflect.ValueOf(uint(val)))
		case reflect.Int8:
			val, err := strconv.ParseInt(str, 10, 64)
			if err != nil {
				return err
			}
			fval.Set(reflect.ValueOf(int8(val)))
		case reflect.Uint8:
			val, err := strconv.ParseInt(str, 10, 64)
			if err != nil {
				return err
			}
			fval.Set(reflect.ValueOf(uint8(val)))
		case reflect.Int16:
			val, err := strconv.ParseInt(str, 10, 64)
			if err != nil {
				return err
			}
			fval.Set(reflect.ValueOf(int16(val)))
		case reflect.Uint16:
			val, err := strconv.ParseInt(str, 10, 64)
			if err != nil {
				return err
			}
			fval.Set(reflect.ValueOf(uint16(val)))
		case reflect.Int32:
			val, err := strconv.ParseInt(str, 10, 64)
			if err != nil {
				return err
			}
			fval.Set(reflect.ValueOf(int32(val)))
		case reflect.Uint32:
			val, err := strconv.ParseInt(str, 10, 64)
			if err != nil {
				return err
			}
			fval.Set(reflect.ValueOf(uint32(val)))
		case reflect.Int64:
			val, err := strconv.ParseInt(str, 10, 64)
			if err != nil {
				return err
			}
			fval.Set(reflect.ValueOf(val))
		case reflect.Uint64:
			val, err := strconv.ParseInt(str, 10, 64)
			if err != nil {
				return err
			}
			fval.Set(reflect.ValueOf(uint64(val)))
		case reflect.Float32:
			val, err := strconv.ParseFloat(str, 32)
			if err != nil {
				return err
			}
			fval.Set(reflect.ValueOf(float32(val)))
		case reflect.Float64:
			val, err := strconv.ParseFloat(str, 64)
			if err != nil {
				return err
			}
			fval.Set(reflect.ValueOf(val))
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
	switch f.attrType {
	case "S":
		return &dynamodb.AttributeValue{S: f.format(v, f.index)}, nil
	case "SS":
		av := &dynamodb.AttributeValue{}
		ss := fv.Interface().([]string)
		for i := range ss {
			av.SS = append(av.SS, &ss[i])
		}
		return av, nil
	}
	return f.client.simpleMarshal(fv, f.layout)
}

func (f *field) unmarshal(item map[string]*dynamodb.AttributeValue, v reflect.Value) error {
	fv := v.Field(f.index)
	for fv.Kind() == reflect.Ptr {
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
	case "SS":
		if item[f.attrName] != nil && item[f.attrName].SS != nil {
			ssptr := item[f.attrName].SS
			var ss []string
			for i := range ssptr {
				ss = append(ss, *ssptr[i])
			}
			fv.Set(reflect.ValueOf(ss))
		}
	default:
		return f.client.simpleUnmarshal(fv, item[f.attrName], f.layout)
	}
	return nil
}
