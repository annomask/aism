package tsm1

import (
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2/influxql/query"
	"github.com/influxdata/influxdb/v2/tsdb/engine/tsm1/aism"
	"github.com/influxdata/influxql"
)

type aggregateTestRow struct {
	Time  int64
	Value any
	Count uint32
}

func aggregateTestRows(t *testing.T, it query.Iterator) []aggregateTestRow {
	t.Helper()
	defer it.Close()
	var rows []aggregateTestRow
	for {
		switch v := it.(type) {
		case query.IntegerIterator:
			p, err := v.Next()
			if err != nil {
				t.Fatal(err)
			}
			if p == nil {
				return rows
			}
			rows = append(rows, aggregateTestRow{p.Time, p.Value, p.Aggregated})
		case query.UnsignedIterator:
			p, err := v.Next()
			if err != nil {
				t.Fatal(err)
			}
			if p == nil {
				return rows
			}
			rows = append(rows, aggregateTestRow{p.Time, p.Value, p.Aggregated})
		case query.FloatIterator:
			p, err := v.Next()
			if err != nil {
				t.Fatal(err)
			}
			if p == nil {
				return rows
			}
			rows = append(rows, aggregateTestRow{p.Time, p.Value, p.Aggregated})
		case query.BooleanIterator:
			p, err := v.Next()
			if err != nil {
				t.Fatal(err)
			}
			if p == nil {
				return rows
			}
			rows = append(rows, aggregateTestRow{p.Time, p.Value, p.Aggregated})
		default:
			t.Fatalf("unexpected aggregate iterator %T", it)
		}
	}
}

func TestAISMResultMatchesNativeReducers(t *testing.T) {
	cases := []struct {
		name   string
		kind   aism.Kind
		values []uint64
	}{
		{"integer", aism.Integer, []uint64{5, 3, 3, 12, 2, 9}},
		{"integer_overflow", aism.Integer, []uint64{math.MaxInt64, 1, 8, math.MaxInt64, 1, 2}},
		{"unsigned", aism.Unsigned, []uint64{math.MaxUint64, 1, 8, math.MaxUint64, 1, 2}},
		{"float", aism.Float, []uint64{math.Float64bits(.25), math.Float64bits(-.5), math.Float64bits(2), math.Float64bits(.25), math.Float64bits(1), math.Float64bits(-.5)}},
		{"boolean", aism.Boolean, []uint64{1, 0, 1, 0, 1, 0}},
	}
	const start = int64(24 * time.Hour)
	width := time.Hour
	for _, c := range cases {
		for name, op := range aismOps {
			if c.kind == aism.Boolean && op != aism.Count && op != aism.First && op != aism.Last {
				continue
			}
			t.Run(c.name+"/"+name, func(t *testing.T) {
				opt := query.IteratorOptions{Expr: &influxql.Call{Name: name, Args: []influxql.Expr{&influxql.VarRef{Val: "v"}}},
					Interval: query.Interval{Duration: width}, StartTime: start, EndTime: start + int64(2*width),
					Ascending: true, Fill: influxql.NoFill}
				tags := query.NewTags(map[string]string{"hostname": "h"})
				var ints []query.IntegerPoint
				var uints []query.UnsignedPoint
				var floats []query.FloatPoint
				var bools []query.BooleanPoint
				var rows []aism.Result
				for i, bits := range c.values {
					ts := start + int64(i/3)*int64(width) + int64(i%3)*int64(time.Second)
					bucket, _ := opt.Window(ts)
					if len(rows) == 0 || rows[len(rows)-1].Bucket != bucket {
						rows = append(rows, aism.Result{})
					}
					rows[len(rows)-1].Fold(c.kind, op, bucket, aism.Point(c.kind, ts, bits))
					switch c.kind {
					case aism.Integer:
						ints = append(ints, query.IntegerPoint{Name: "cpu", Tags: tags, Time: ts, Value: int64(bits)})
					case aism.Unsigned:
						uints = append(uints, query.UnsignedPoint{Name: "cpu", Tags: tags, Time: ts, Value: bits})
					case aism.Float:
						floats = append(floats, query.FloatPoint{Name: "cpu", Tags: tags, Time: ts, Value: math.Float64frombits(bits)})
					case aism.Boolean:
						bools = append(bools, query.BooleanPoint{Name: "cpu", Tags: tags, Time: ts, Value: bits != 0})
					}
				}
				raw := func() query.Iterator {
					switch c.kind {
					case aism.Integer:
						return &aismIterator[query.IntegerPoint]{points: ints}
					case aism.Unsigned:
						return &aismIterator[query.UnsignedPoint]{points: uints}
					case aism.Float:
						return &aismIterator[query.FloatPoint]{points: floats}
					default:
						return &aismIterator[query.BooleanPoint]{points: bools}
					}
				}
				native, err := query.NewCallIterator(raw(), opt)
				if err != nil {
					t.Fatal(err)
				}
				want := aggregateTestRows(t, native)
				got := aggregateTestRows(t, aismResults("cpu", tags, c.kind, op, rows))
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("native=%v indexed=%v", want, got)
				}
				// Exercise the same reducer boundary used to combine series and shards.
				first, err := query.NewCallIterator(raw(), opt)
				if err != nil {
					t.Fatal(err)
				}
				second, err := query.NewCallIterator(raw(), opt)
				if err != nil {
					t.Fatal(err)
				}
				nativeMerged, err := query.Iterators{first, second}.Merge(opt)
				if err != nil {
					t.Fatal(err)
				}
				indexMerged, err := query.Iterators{aismResults("cpu", tags, c.kind, op, rows), aismResults("cpu", tags, c.kind, op, rows)}.Merge(opt)
				if err != nil {
					t.Fatal(err)
				}
				want = aggregateTestRows(t, nativeMerged)
				got = aggregateTestRows(t, indexMerged)
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("merged native=%v indexed=%v", want, got)
				}
			})
		}
	}
}
