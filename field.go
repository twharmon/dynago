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
	for ty.Kind() == reflect.Pointer {
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
			for elTy.Kind() == reflect.Pointer {
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

func (f *field) format(v reflect.Value) (*string, error) {
	output := f.fmt
	for _, match := range fmtRegExp.FindAllString(f.fmt, -1) {
		var fval reflect.Value
		var refFieldLayout string
		if match == "{}" {
			fval = v.Field(f.index)
			refFieldLayout = f.layout
		} else {
			cache, err := f.client.cachedStruct(v.Type()) // TODO: do this lazily
			if err != nil {
				return nil, err
			}
			fname := trimDelims(match)
			vt := v.Type()
			for i := 0; i < vt.NumField(); i++ {
				fld := vt.Field(i)
				if fld.Name == fname {
					fval = v.Field(i)
					ff := cache[i]
					refFieldLayout = ff.layout
					break
				}
			}
		}
		for fval.Kind() == reflect.Pointer {
			fval = fval.Elem()
		}
		switch fval.Kind() {
		case reflect.String:
			output = strings.ReplaceAll(output, match, fval.String())
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			output = strings.ReplaceAll(output, match, strconv.FormatInt(fval.Int(), 10))
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			output = strings.ReplaceAll(output, match, strconv.FormatUint(fval.Uint(), 10))
		case reflect.Float32:
			output = strings.ReplaceAll(output, match, strconv.FormatFloat(fval.Float(), 'f', -1, 32))
		case reflect.Float64:
			output = strings.ReplaceAll(output, match, strconv.FormatFloat(fval.Float(), 'f', -1, 64))
		case reflect.Struct:
			switch val := fval.Interface().(type) {
			case time.Time:
				output = strings.ReplaceAll(output, match, val.Format(refFieldLayout))
			}
		}
	}
	return &output, nil
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
		var refFieldLayout string
		if fname == "" {
			fval = v.Field(f.index)
			refFieldLayout = f.layout
		} else {
			fval = v.FieldByName(fname)
			cache, err := f.client.cachedStruct(v.Type()) // TODO: do this lazily
			if err != nil {
				return err
			}
			vt := v.Type()
			for i := 0; i < vt.NumField(); i++ {
				fld := vt.Field(i)
				if fld.Name == fname {
					fval = v.Field(i)
					ff := cache[i]
					refFieldLayout = ff.layout
					break
				}
			}
		}
		for fval.Kind() == reflect.Pointer {
			fval = fval.Elem()
		}
		fty := fval.Type()
		switch fty.Kind() {
		case reflect.String:
			fval.SetString(str)
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			val, err := strconv.ParseInt(str, 10, fval.Type().Bits())
			if err != nil {
				return err
			}
			fval.SetInt(val)
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			val, err := strconv.ParseUint(str, 10, fval.Type().Bits())
			if err != nil {
				return err
			}
			fval.SetUint(val)
		case reflect.Float32, reflect.Float64:
			val, err := strconv.ParseFloat(str, fval.Type().Bits())
			if err != nil {
				return err
			}
			fval.SetFloat(val)
		case reflect.Struct:
			switch fty {
			case timeType:
				t, err := time.Parse(refFieldLayout, str)
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
	for fv.Kind() == reflect.Pointer {
		fv = fv.Elem()
	}
	switch f.attrType {
	case "S":
		s, err := f.format(v)
		if err != nil {
			return nil, err
		}
		return &dynamodb.AttributeValue{S: s}, nil
	case "SS":
		av := &dynamodb.AttributeValue{}
		ss := fv.Interface().([]string)
		av.SS = make([]*string, len(ss))
		for i := range ss {
			av.SS[i] = &ss[i]
		}
		return av, nil
	}
	return f.client.simpleMarshal(fv, f.layout)
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
	case "SS":
		if item[f.attrName] != nil && item[f.attrName].SS != nil {
			ssptr := item[f.attrName].SS
			ss := make([]string, len(ssptr))
			for i := range ssptr {
				ss[i] = *ssptr[i]
			}
			fv.Set(reflect.ValueOf(ss))
		}
	default:
		return f.client.simpleUnmarshal(fv, item[f.attrName], f.layout)
	}
	return nil
}
