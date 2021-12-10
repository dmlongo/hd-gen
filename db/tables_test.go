package db

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"
)

var tab *Table
var del []int

const size = 5
const p = 0.3

func init() {
	tab = NewTable([]string{"a", "b", "c"}, nil)
	for i := 0; i < size; i++ {
		tab.AddTuple(Tuple{
			strconv.Itoa(rand.Intn(1000)),
			strconv.Itoa(rand.Intn(5000)),
			strconv.Itoa(rand.Intn(2000)),
		})
	}

	for i := 0; i < tab.Size(); i++ {
		if rand.Float64() < p {
			del = append(del, i)
		}
	}
	fmt.Println("T has", tab.Size(), "tuples")
	fmt.Println(len(del), "tuples to delete")
}

/*func subset(tuples1 []Tuple, tuples2 []Tuple) bool {
	for _, t1 := range tuples1 {
		found := false
		for _, t2 := range tuples2 {
			if equals(t1, t2) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func equals(t1 Tuple, t2 Tuple) bool {
	if len(t1) != len(t2) {
		return false
	}
	for k := 0; k < len(t1); k++ {
		if t1[k] != t2[k] {
			return false
		}
	}
	return true
}*/

func BenchmarkRemoveTuples(b *testing.B) {
	tups := append([]Tuple(nil), tab.Tuples...)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tab.RemoveTuples(del)
		tab.Tuples = make([]Tuple, len(tups))
		copy(tab.Tuples, tups)
	}
}
