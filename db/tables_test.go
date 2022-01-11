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
	tab = NewTable([]string{"a", "b", "c"}, false)
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

func BenchmarkRemoveTuples(b *testing.B) {
	tups := append([]Tuple(nil), tab.Tuples...)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tab.RemoveTuples(del)
		tab.Tuples = make([]Tuple, len(tups))
		copy(tab.Tuples, tups)
	}
}
