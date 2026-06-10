package main

import (
	"reflect"
	"regexp"
	"testing"

	"github.com/SeanKraemer/distributed-stream-processor/pkg/rainstorm"
)

func TestParseCSVLine(t *testing.T) {
	cases := []struct {
		name string
		line string
		want []string
	}{
		{
			name: "simple fields",
			line: "a,b,c",
			want: []string{"a", "b", "c"},
		},
		{
			name: "quoted field containing comma",
			line: `one,"two, with comma",three`,
			// Quote characters are consumed (toggled), not emitted.
			want: []string{"one", "two, with comma", "three"},
		},
		{
			name: "empty fields",
			line: "a,,c",
			want: []string{"a", "", "c"},
		},
		{
			name: "trailing comma yields trailing empty field",
			line: "a,b,",
			want: []string{"a", "b", ""},
		},
		{
			name: "single field no commas",
			line: "solo",
			want: []string{"solo"},
		},
		{
			name: "empty line yields one empty field",
			line: "",
			want: []string{""},
		},
		{
			name: "fully quoted field",
			line: `"hello",world`,
			want: []string{"hello", "world"},
		},
		{
			name: "leading and trailing empty fields",
			line: ",mid,",
			want: []string{"", "mid", ""},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := parseCSVLine(tc.line)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("parseCSVLine(%q) = %#v, want %#v", tc.line, got, tc.want)
			}
		})
	}
}

func collectEmissions() (func(rainstorm.Tuple), *[]rainstorm.Tuple) {
	var emitted []rainstorm.Tuple
	return func(t rainstorm.Tuple) {
		emitted = append(emitted, t)
	}, &emitted
}

func TestGrepFiltersByPattern(t *testing.T) {
	op := &GrepOp{pattern: regexp.MustCompile("Punched")}
	emit, emitted := collectEmissions()

	op.Process(rainstorm.Tuple{Key: "f:1", Value: "1,Punched Telespar,Sign A", ID: "t1"}, emit)
	op.Process(rainstorm.Tuple{Key: "f:2", Value: "2,Unpunched Pole,Sign B", ID: "t2"}, emit)
	op.Process(rainstorm.Tuple{Key: "f:3", Value: "3,Punched Again,Sign C", ID: "t3"}, emit)

	if len(*emitted) != 2 {
		t.Fatalf("emitted %d tuples, want 2: %+v", len(*emitted), *emitted)
	}
	if (*emitted)[0].ID != "t1" || (*emitted)[1].ID != "t3" {
		t.Errorf("emitted IDs = [%s %s], want [t1 t3]", (*emitted)[0].ID, (*emitted)[1].ID)
	}
	// Without columnNum, the key is passed through unchanged.
	if (*emitted)[0].Key != "f:1" {
		t.Errorf("emitted[0].Key = %q, want %q (unchanged)", (*emitted)[0].Key, "f:1")
	}
	if op.matchCount != 2 {
		t.Errorf("matchCount = %d, want 2", op.matchCount)
	}
	if op.filterCount != 1 {
		t.Errorf("filterCount = %d, want 1", op.filterCount)
	}
}

func TestGrepColumnExtractionAsKey(t *testing.T) {
	op := &GrepOp{pattern: regexp.MustCompile("Streetlight"), columnNum: 3}
	emit, emitted := collectEmissions()

	op.Process(rainstorm.Tuple{Key: "f:1", Value: `1,Streetlight,"Type, X",extra`, ID: "t1"}, emit)

	if len(*emitted) != 1 {
		t.Fatalf("emitted %d tuples, want 1", len(*emitted))
	}
	// Column 3 (1-indexed) of the CSV, with quotes consumed by the parser.
	if (*emitted)[0].Key != "Type, X" {
		t.Errorf("Key = %q, want %q", (*emitted)[0].Key, "Type, X")
	}
	// Value is passed through unchanged.
	if (*emitted)[0].Value != `1,Streetlight,"Type, X",extra` {
		t.Errorf("Value = %v, want original line", (*emitted)[0].Value)
	}
}

func TestGrepColumnOutOfRangeYieldsEmptyKey(t *testing.T) {
	op := &GrepOp{pattern: regexp.MustCompile("a"), columnNum: 10}
	emit, emitted := collectEmissions()

	op.Process(rainstorm.Tuple{Key: "orig", Value: "a,b", ID: "t1"}, emit)

	if len(*emitted) != 1 {
		t.Fatalf("emitted %d tuples, want 1", len(*emitted))
	}
	if (*emitted)[0].Key != "" {
		t.Errorf("Key = %q, want empty string for out-of-range column", (*emitted)[0].Key)
	}
}

func TestGrepAlwaysForwardsEOF(t *testing.T) {
	// The framework relies on every EOF being forwarded downstream, even
	// repeated ones (eofForwarded only changes logging, not behavior).
	op := &GrepOp{pattern: regexp.MustCompile("nevermatches")}
	emit, emitted := collectEmissions()

	eof1 := rainstorm.Tuple{Type: "tuple", Key: "EOF", Value: "end-of-stream", ID: "src-1-eof", IsEOF: true}
	eof2 := rainstorm.Tuple{Type: "tuple", Key: "EOF", Value: "end-of-stream", ID: "src-2-eof", IsEOF: true}

	op.Process(eof1, emit)
	op.Process(eof2, emit)

	if len(*emitted) != 2 {
		t.Fatalf("emitted %d tuples for 2 EOFs, want 2 (every EOF must be forwarded)", len(*emitted))
	}
	for i, e := range *emitted {
		if !e.IsEOF {
			t.Errorf("emitted[%d].IsEOF = false, want true", i)
		}
	}
	if (*emitted)[0].ID != "src-1-eof" || (*emitted)[1].ID != "src-2-eof" {
		t.Errorf("EOF IDs = [%s %s], want [src-1-eof src-2-eof]", (*emitted)[0].ID, (*emitted)[1].ID)
	}
	if !op.eofForwarded {
		t.Error("eofForwarded = false after processing EOF, want true")
	}
}
