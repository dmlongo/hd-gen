package db

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
)

// Tuple represent a row in a relation
type Tuple []string
type Database map[string]*Table

func Load(dbPath string) Database {
	csvfile, err := os.Open(dbPath)
	if err != nil {
		panic(fmt.Errorf("can't open %v: %v", dbPath, err))
	}

	db := make(Database)
	var currName string
	r := csv.NewReader(csvfile)
	r.FieldsPerRecord = -1
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}

		kind := record[0]
		switch kind {
		case "r":
			currName = record[1]
			attrs := record[2:]
			stats := NewStatistics(attrs)
			db[currName] = NewTable(attrs, stats)
		case "t":
			tup := record[1:]
			if _, ok := db[currName].AddTuple(tup); !ok {
				panic(fmt.Errorf("%v is not a valid tuple for %v", tup, currName))
			}
		default:
			panic(fmt.Errorf("%v is not a valid type", kind))
		}
	}

	return db
}

/*type RelationSchema interface {
	Attributes() []string
	Position(attr string) (pos int, ok bool)
}*/

type Table struct {
	attrs   []string
	attrPos map[string]int
	Tuples  []Tuple

	stats *Statistics
}

func NewTable(attrs []string, stats *Statistics) *Table {
	if len(attrs) <= 0 {
		panic(fmt.Errorf("%v is not valid", attrs))
	}

	t := Table{}
	attrPos := make(map[string]int)
	for i, v := range attrs {
		attrPos[v] = i
	}
	t.attrs = attrs
	t.attrPos = attrPos
	t.Tuples = make([]Tuple, 0)
	t.stats = stats

	return &t
}

func (t *Table) Size() int {
	return len(t.Tuples)
}

func (t *Table) Attributes() []string {
	return t.attrs
}

func (t *Table) Position(attr string) (pos int, ok bool) {
	pos, ok = t.attrPos[attr]
	return
}

func (t *Table) AddTuple(vals []string) (Tuple, bool) {
	if len(t.attrs) != len(vals) {
		return nil, false
	}
	// duplicates allowed
	t.Tuples = append(t.Tuples, vals)
	if t.stats != nil {
		t.stats.AddTuple(vals)
	}
	return vals, true
}

func (t *Table) RemoveTuples(idx []int) (bool, error) {
	if len(idx) == 0 {
		return false, nil
	}

	newSize := len(t.Tuples) - len(idx)
	if newSize < 0 {
		return false, fmt.Errorf("new size %v < 0", newSize)
	}
	newTuples := make([]Tuple, 0, newSize)
	if newSize > 0 {
		i := 0
		for _, j := range idx {
			newTuples = append(newTuples, t.Tuples[i:j]...)
			i = j + 1
		}
		newTuples = append(newTuples, t.Tuples[i:]...)
	}
	t.Tuples = newTuples

	return true, nil
}
