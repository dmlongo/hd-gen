package main

import "fmt"

// Tuple represent a row in a relation
type Tuple []int
type Database map[string]Table

type Table struct {
	attrs   []string
	attrPos map[string]int
	tuples  []Tuple

	ndv    []int
	hgrams []Histogram
}

func (t *Table) Init(attrs []string) {
	if len(attrs) <= 0 {
		panic(fmt.Errorf("%v is not valid", attrs))
	}
	attrPos := make(map[string]int)
	for i, v := range attrs {
		attrPos[v] = i
	}
	t.attrs = attrs
	t.attrPos = attrPos
	t.tuples = make([]Tuple, 0)

	t.ndv = make([]int, len(attrs))
	t.hgrams = make([]Histogram, len(attrs))
}

func (t *Table) Size() int {
	return len(t.tuples)
}

func (t *Table) Attributes() []string {
	return t.attrs
}

func (t *Table) Position(attr string) (pos int, ok bool) {
	pos, ok = t.attrPos[attr]
	return
}

func (t *Table) AddTuple(vals []int) (Tuple, bool) {
	if len(t.attrs) != len(vals) {
		return nil, false
	}
	// TODO check domains?
	// TODO no check if the tuple is already here
	t.tuples = append(t.tuples, vals)
	for i, v := range vals {
		if t.hgrams[i].Update(v) {
			t.ndv[i]++
		}
	}
	return vals, true
}

type Histogram map[int]int

func (hgram Histogram) Update(val int) bool {
	var ok bool
	if _, ok = hgram[val]; !ok {
		hgram[val] = 0
	}
	hgram[val]++
	return !ok
}

func (hgram Histogram) Frequency(val int) int {
	if freq, ok := hgram[val]; ok {
		return freq
	}
	return 0
}
