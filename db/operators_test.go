package db

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"
)

var tab1 *Table
var tab2 *Table

const size1 = 1000
const size2 = 100 * size1

const m = 100
const aNdv = 5 * m
const bNdv = 2 * m
const cNdv = 1 * m

func init() {
	tab1 = NewTable([]string{"a", "b", "c"}, false)
	for i := 0; i < size1; i++ {
		tab1.AddTuple(Tuple{
			strconv.Itoa(rand.Intn(aNdv)),
			strconv.Itoa(rand.Intn(bNdv)),
			strconv.Itoa(rand.Intn(cNdv)),
		})
	}
	tab2 = NewTable([]string{"a", "b", "c"}, false)
	for i := 0; i < size2; i++ {
		tab2.AddTuple(Tuple{
			strconv.Itoa(rand.Intn(aNdv)),
			strconv.Itoa(rand.Intn(bNdv)),
			strconv.Itoa(rand.Intn(cNdv)),
		})
	}

	jsb := Join(*tab1, *tab2)
	jbs := Join(*tab2, *tab1)
	sj1 := fakeSemijoin(*tab1, *tab2)
	sj2 := fakeSemijoin(*tab2, *tab1)
	fmt.Printf("Size(tab1) = %v\tSize(tab2) = %v\n", size1, size2)
	fmt.Println("res\tsize\tsel")
	fmt.Printf("jsb\t%v\t\t%v\n", jsb.Size(), float64(jsb.Size())/(size1*size2))
	fmt.Printf("jbs\t%v\t\t%v\n", jbs.Size(), float64(jbs.Size())/(size1*size2))
	fmt.Printf("sj1\t%v\t%v\n", sj1, float64(sj1)/(size1*size2))
	fmt.Printf("sj2\t%v\t%v\n", sj2, float64(sj2)/(size1*size2))
}

func fakeSemijoin(l Table, r Table) int {
	joinIdx := commonAttrs(l, r)
	if len(joinIdx) == 0 {
		return 0
	}

	var tupToDel []int
	for i, leftTup := range l.Tuples {
		delete := true
		for _, rightTup := range r.Tuples {
			if match(leftTup, rightTup, joinIdx) {
				delete = false
				break
			}
		}
		if delete {
			tupToDel = append(tupToDel, i)
		}
	}

	return l.Size() - len(tupToDel)
}

func BenchmarkJoinSmallBig(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Join(*tab1, *tab2)
	}
}

func BenchmarkJoinBigSmall(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Join(*tab2, *tab1)
	}
}

func BenchmarkSemijoin1(b *testing.B) {
	for i := 0; i < b.N; i++ {
		fakeSemijoin(*tab1, *tab2)
	}
}

func BenchmarkSemijoin2(b *testing.B) {
	for i := 0; i < b.N; i++ {
		fakeSemijoin(*tab2, *tab1)
	}
}
