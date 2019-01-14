package netgraph

import (
	"strconv"

	// used by protoc
	_ "github.com/gogo/protobuf/proto"
)

// Check checks is Bucket satisfies filter f.
func (f Filter) Check(b Bucket) bool {
	if sf := f.GetF(); sf != nil {
		return f.Key == b.Key && sf.Check(b.Value)
	}
	return false
}

// Check returns result of applying sf to value.
// For numeric comparisons, value is parsed to int64.
func (sf SimpleFilter) Check(value string) bool {
	switch sf.Op {
	case Operation_OR:
		if args := sf.GetFArgs(); args != nil {
			result := false
			for _, f := range args.Filters {
				if result = result || f.Check(value); result {
					return result
				}
			}
			return result
		}
		return true
	case Operation_AND:
		if args := sf.GetFArgs(); args != nil {
			result := true
			for _, f := range args.Filters {
				if result = result && f.Check(value); !result {
					return result
				}
			}
			return result
		}
		return true
	case Operation_NP:
		return true
	case Operation_EQ:
		return value == sf.GetValue()
	case Operation_NE:
		return value != sf.GetValue()
	}

	var (
		exp, val int64
		err      error
	)
	if val, err = strconv.ParseInt(value, 10, 64); err != nil {
		return true
	}
	if exp, err = strconv.ParseInt(sf.GetValue(), 10, 64); err != nil {
		return true
	}

	switch sf.Op {
	case Operation_GT:
		return val > exp
	case Operation_GE:
		return val >= exp
	case Operation_LT:
		return val < exp
	case Operation_LE:
		return val <= exp
	default:
		return true
	}
}

// Filter returns sublist of bs, satisfying f.
func (f Filter) Filter(bs ...Bucket) []Bucket {
	result := make([]Bucket, 0, len(bs))
	for _, b := range bs {
		if f.Check(b) {
			result = append(result, b)
		}
	}
	return result
}

// NewFilter constructs SimpleFilter.
func NewFilter(op Operation, value string) *SimpleFilter {
	return &SimpleFilter{
		Op:   op,
		Args: &SimpleFilter_Value{Value: value},
	}
}

// FilterIn returns filter, which checks if value is in specified list.
func FilterIn(values ...string) *SimpleFilter {
	fs := make([]*SimpleFilter, 0, len(values))
	for _, v := range values {
		fs = append(fs, FilterEQ(v))
	}
	return FilterOR(fs...)
}

// FilterNotIn returns filter, which checks if value is not in specified list.
func FilterNotIn(values ...string) *SimpleFilter {
	fs := make([]*SimpleFilter, 0, len(values))
	for _, v := range values {
		fs = append(fs, FilterNE(v))
	}
	return FilterAND(fs...)
}

// FilterOR returns OR combination of filters.
func FilterOR(fs ...*SimpleFilter) *SimpleFilter {
	return &SimpleFilter{
		Op:   Operation_OR,
		Args: &SimpleFilter_FArgs{FArgs: &SimpleFilters{Filters: fs}},
	}
}

// FilterAND returns AND combination of filters.
func FilterAND(fs ...*SimpleFilter) *SimpleFilter {
	return &SimpleFilter{
		Op:   Operation_AND,
		Args: &SimpleFilter_FArgs{FArgs: &SimpleFilters{Filters: fs}},
	}
}

// FilterEQ returns filter, which checks if value is equal to v.
func FilterEQ(v string) *SimpleFilter {
	return &SimpleFilter{
		Op:   Operation_EQ,
		Args: &SimpleFilter_Value{Value: v},
	}
}

// FilterNE returns filter, which checks if value is not equal to v.
func FilterNE(v string) *SimpleFilter {
	return &SimpleFilter{
		Op:   Operation_NE,
		Args: &SimpleFilter_Value{Value: v},
	}
}

// FilterGT returns filter, which checks if value is greater than v.
func FilterGT(v int64) *SimpleFilter {
	return &SimpleFilter{
		Op:   Operation_GT,
		Args: &SimpleFilter_Value{Value: strconv.FormatInt(v, 10)},
	}
}

// FilterGE returns filter, which checks if value is greater or equal than v.
func FilterGE(v int64) *SimpleFilter {
	return &SimpleFilter{
		Op:   Operation_GE,
		Args: &SimpleFilter_Value{Value: strconv.FormatInt(v, 10)},
	}
}

// FilterLT returns filter, which checks if value is less than v.
func FilterLT(v int64) *SimpleFilter {
	return &SimpleFilter{
		Op:   Operation_LT,
		Args: &SimpleFilter_Value{Value: strconv.FormatInt(v, 10)},
	}
}

// FilterLE returns filter, which checks if value is less or equal than v.
func FilterLE(v int64) *SimpleFilter {
	return &SimpleFilter{
		Op:   Operation_LE,
		Args: &SimpleFilter_Value{Value: strconv.FormatInt(v, 10)},
	}
}
