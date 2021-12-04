package db

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"

	"github.com/cem-okulmus/BalancedGo/lib"
)

// Tuple represent a row in a relation
type Tuple []string
type Database map[string]*Table

func Load(dbPath string, graph lib.ParseGraph) (Database, map[int]string) {
	// 1. read the csv file
	csvfile, err := os.Open(dbPath)
	if err != nil {
		panic(fmt.Errorf("can't open %v: %v", dbPath, err))
	}

	// 2. init map
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

		// 3. put the record into the map
		kind := record[0]
		switch kind {
		case "r":
			currName = record[1]
			attrs := record[2:]
			db[currName] = MakeTable(attrs)
		case "t":
			tup := record[1:]
			if _, ok := db[currName].AddTuple(tup); !ok {
				panic(fmt.Errorf("%v is not a valid tuple for %v", tup, currName))
			}
		default:
			panic(fmt.Errorf("%v is not a valid type", kind))
		}
	}

	e2t := make(map[int]string)
	for t := range db {
		e := graph.Encoding[t]
		e2t[e] = t
	}

	return db, e2t
}

/*func LoadDatabasePlanning(path string, graph lib.ParseGraph) (Database, map[int]string) {
	// 1. read the csv file
	csvfile, err := os.Open(path)
	if err != nil {
		panic(fmt.Errorf("can't open %v: %v", path, err))
	}

	// 2. init map
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

		// 3. put the record into the map
		pred := record[0]
		i := strings.Index(pred, "(")
		j := strings.Index(pred, ")")
		name := pred[:i]
		attrs := strings.Split(pred[i+1:j], ",")
		if _, ok := db[name]; !ok {
			db[name] = MakeTable(attrs)
		}
		db[name].

		//for i, j = 0, strings.Index(attrs[i:], ","); j != -1; i, j = j+1, strings.Index(attrs[i:], ",") {
		//	a := attrs[i:j]
		//}

		switch pred {
		case "r":
			currName = record[1]
			attrs := record[2:]
			db[currName] = MakeTable(attrs)
		case "t":
			tup := record[1:]
			if _, ok := db[currName].AddTuple(tup); !ok {
				panic(fmt.Errorf("%v is not a valid tuple for %v", tup, currName))
			}
		default:
			panic(fmt.Errorf("%v is not a valid type", pred))
		}
	}

	e2t := make(map[int]string)
	for t := range db {
		e := graph.Encoding[t]
		e2t[e] = t
	}

	return db, e2t
}*/

type Table struct {
	attrs   []string
	attrPos map[string]int
	tuples  []Tuple

	ndv    []int
	hgrams []Histogram
}

func MakeTable(attrs []string) *Table {
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
	t.tuples = make([]Tuple, 0)

	t.ndv = make([]int, len(attrs))
	t.hgrams = make([]Histogram, len(attrs))
	for i := range t.hgrams {
		t.hgrams[i] = make(Histogram)
	}
	return &t
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

func (t *Table) AddTuple(vals []string) (Tuple, bool) {
	if len(t.attrs) != len(vals) {
		return nil, false
	}
	// TODO no check if the tuple is already here
	t.tuples = append(t.tuples, vals)
	for i, v := range vals {
		if t.hgrams[i].Update(v) {
			t.ndv[i]++
		}
	}
	return vals, true
}

type Histogram map[string]int

func (hgram Histogram) Update(val string) bool {
	var ok bool
	if _, ok = hgram[val]; !ok {
		hgram[val] = 0
	}
	hgram[val]++
	return !ok
}

func (hgram Histogram) Frequency(val string) int {
	if freq, ok := hgram[val]; ok {
		return freq
	}
	return 0
}
