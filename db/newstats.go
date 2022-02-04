package db

import (
	"container/heap"
	"fmt"
	"math"
)

type Histogram map[string]int

func (hgram Histogram) Update(val string, freq int) bool {
	var ok bool
	if _, ok = hgram[val]; !ok {
		hgram[val] = 0
	}
	hgram[val] = freq
	return !ok
}

func (hgram Histogram) Frequency(val string) int {
	if freq, ok := hgram[val]; ok {
		return freq
	}
	return 0
}

func (hgram Histogram) Sum() int {
	n := 0
	for _, occ := range hgram {
		n += occ
	}
	return n
}

type Statistics struct {
	attrs   []string
	attrPos map[string]int

	Size   int
	Ndv    []int
	Hgrams []Histogram
}

func NewStatistics(attrs []string) *Statistics {
	if len(attrs) <= 0 {
		panic(fmt.Errorf("%v is not valid", attrs))
	}

	var stats Statistics
	attrPos := make(map[string]int)
	for i, v := range attrs {
		attrPos[v] = i
	}
	stats.attrs = attrs
	stats.attrPos = attrPos

	stats.Size = 0
	stats.Ndv = make([]int, len(attrs))
	stats.Hgrams = make([]Histogram, len(attrs))
	for i := range stats.Hgrams {
		stats.Hgrams[i] = make(Histogram)
	}
	return &stats
}

func (s *Statistics) Attributes() []string {
	return s.attrs
}

func (s *Statistics) Position(attr string) (pos int, ok bool) {
	pos, ok = s.attrPos[attr]
	return
}

func (s *Statistics) AddTuple(vals []string) {
	s.Size++
	for i, v := range vals {
		if s.Hgrams[i].Update(v, 1) {
			s.Ndv[i]++
		}
	}
}

func (s *Statistics) SetSize(size int) {
	if size < 0 {
		panic(fmt.Errorf("size %v is not valid", size))
	}
	s.Size = size
}

func (s *Statistics) SetNdv(attr string, ndv int) {
	var p int
	var ok bool
	if p, ok = s.attrPos[attr]; !ok {
		panic(fmt.Errorf("attr %v does not exist", attr))
	}
	if ndv < 0 {
		panic(fmt.Errorf("ndv %v is not valid", ndv))
	}
	s.Ndv[p] = ndv
}

func (s *Statistics) GetNdv(attr string) int {
	var p int
	var ok bool
	if p, ok = s.attrPos[attr]; !ok {
		panic(fmt.Errorf("attr %v does not exist", attr))
	}
	return s.Ndv[p]
}

func (s *Statistics) usesHistograms() bool { // todo column as input
	return s.Size == s.Hgrams[0].Sum() // rough way
}

func classifyStatistics(stats []*Statistics) ([]*Statistics, []*Statistics) {
	var histograms []*Statistics
	var estimates []*Statistics

	for _, st := range stats {
		if st.usesHistograms() {
			histograms = append(histograms, st)
		} else {
			estimates = append(estimates, st)
		}
	}

	return histograms, estimates
}

func EstimateJoinSize(tables []*Statistics) (int, *Statistics) {
	hgrs, ests := classifyStatistics(tables)
	if len(hgrs) == len(tables) {
		return hgramJoinStats(hgrs)
	}
	if len(ests) == len(tables) {
		return naiveJoinStats(ests)
	}
	// todo mixed case
	return -1, &Statistics{}
}

func hgramJoinStats(tables []*Statistics) (int, *Statistics) {
	if len(tables) == 1 {
		return tables[0].Size, tables[0]
	}

	newAttrs, commonAttrs := JoinAttrs(StatsToRels(tables...)...)
	emptyStats := NewStatistics(newAttrs)
	newStats := emptyStats

	s := 1
	for _, t := range tables {
		s *= t.Size
		if s == 0 {
			return 0, emptyStats
		}
	}
	sizes := float64(s)

	sel := 1.0
	for attr, rels := range commonAttrs {
		if len(rels) > 1 {
			if d, empty := joinSelectivity(attr, RelsToStats(rels...), newStats); !empty {
				sel *= d
			} else {
				return 0, emptyStats
			}
		}
	}

	// todo ensure consistency between size and sum of hgrams
	newStats.Size = int(math.Round(sel * sizes))
	return newStats.Size, newStats
}

// pre: tables are not empty
func joinSelectivity(attr string, tables []*Statistics, newStats *Statistics) (float64, bool) {
	n := joinMatchingTuples(attr, tables, newStats)
	if n == 0 {
		return 0.0, true
	}
	num := float64(n)

	d := 1
	for _, t := range tables {
		d *= t.Size
	}
	den := float64(d)

	res := num / den
	if res < 1e-9 {
		return 0.0, true
	}
	return res, false
}

//pre: len(tables) >= 2
func joinMatchingTuples(attr string, tables []*Statistics, stats *Statistics) int {
	idx := make([]int, 0)
	for _, t := range tables { // TODO idx structure not really necessary
		if p, ok := t.Position(attr); ok {
			idx = append(idx, p)
		} else {
			panic(fmt.Errorf("%v not in %v", attr, t))
		}
	}
	ps, _ := stats.Position(attr)

	n := 0
	for val, freq := range tables[0].Hgrams[idx[0]] { // TODO choose the smallest hgram
		temp := freq
		for i := 1; i < len(tables); i++ {
			temp *= tables[i].Hgrams[idx[i]].Frequency(val)
		}
		if temp > 0 && stats.Hgrams[ps].Update(val, temp) {
			stats.Ndv[ps]++
		}
		n += temp
	}

	return n
}

// T(R \join S) = T(R)*T(S) / max(V(R,Y),V(S,Y))
func naiveJoinStats(tables []*Statistics) (int, *Statistics) {
	newAttrs, commonAttrs := JoinAttrs(StatsToRels(tables...)...)
	emptyStats := NewStatistics(newAttrs)
	newStats := emptyStats

	n := 1
	for _, t := range tables {
		n *= t.Size
		if n == 0 {
			return 0, emptyStats
		}
	}
	num := float64(n)

	d := 1
	for attr, rels := range commonAttrs {
		stats := RelsToStats(rels...)
		if len(rels) > 1 {
			kMax, min := joinKMax(stats, attr, len(rels))
			d *= kMax
			newStats.SetNdv(attr, min)
		} else {
			p, _ := stats[0].Position(attr)
			oldNdv := stats[0].Ndv[p]
			newStats.SetNdv(attr, oldNdv)
		}
	}
	den := float64(d)

	newStats.Size = int(math.Round(num / den))
	return newStats.Size, newStats
}

func joinKMax(rels []*Statistics, attr string, k int) (int, int) {
	var h IntHeap
	heap.Init(&h)
	for _, r := range rels {
		if i, ok := r.Position(attr); ok {
			heap.Push(&h, r.Ndv[i])
		} else {
			panic(fmt.Errorf("%v not in %v", attr, r))
		}
	}
	res := 1
	for i := 0; i < k-1; i++ {
		res *= h[i]
	}
	return res, h[k-1]
}

func EstimateSemijoinSize(left *Statistics, right *Statistics) (int, *Statistics) {
	if left.usesHistograms() && right.usesHistograms() {
		return hgramSemijoinStats(left, right)
	}
	if !left.usesHistograms() && !right.usesHistograms() {
		return naiveSemijoinStats(left, right)
	}
	// todo mixed case
	return -1, &Statistics{}
}

func hgramSemijoinStats(left *Statistics, right *Statistics) (int, *Statistics) {
	newAttrs, commonAttrs := JoinAttrs(StatsToRels(left, right)...)
	emptyStats := NewStatistics(newAttrs)
	newStats := emptyStats

	if left.Size == 0 || right.Size == 0 {
		return 0, emptyStats
	}

	sel := 1.0
	for attr, rels := range commonAttrs {
		if len(rels) > 1 {
			// rels = left,right
			if d, empty := semijoinSelectivity(attr, left, right, newStats); !empty {
				sel *= d
			} else {
				return 0, emptyStats
			}
		}
	}

	// todo ensure consistency between size and sum of hgrams
	newStats.Size = int(math.Round(sel * float64(left.Size)))
	return newStats.Size, newStats
}

func semijoinSelectivity(attr string, left *Statistics, right *Statistics, stats *Statistics) (float64, bool) {
	n := 0
	idx := []int{left.attrPos[attr], right.attrPos[attr]}
	ps, _ := stats.Position(attr)
	for val, freq := range left.Hgrams[idx[0]] {
		if right.Hgrams[idx[1]].Frequency(val) > 0 {
			n += freq
			if stats.Hgrams[ps].Update(val, freq) {
				stats.Ndv[ps]++
			}
		}
	}
	if n == 0 {
		return 0.0, true
	}
	num := float64(n)

	res := num / float64(left.Size)
	if res < 1e-9 {
		return 0.0, true
	}
	return res, false
}

func naiveSemijoinStats(left *Statistics, right *Statistics) (int, *Statistics) {
	emptyStats := NewStatistics(left.attrs)
	newStats := emptyStats

	if left.Size == 0 || right.Size == 0 {
		return 0, emptyStats
	}

	tmp := 1
	for i, attr := range left.Attributes() {
		if j, ok := right.Position(attr); ok {
			minNdv := left.Ndv[i]
			if right.Ndv[j] < minNdv {
				minNdv = right.Ndv[j]
			}
			if minNdv == 0 {
				minNdv = 1
			}
			tmp *= minNdv
			newStats.SetNdv(attr, minNdv)
		} else {
			ndv := 1
			if left.Ndv[i] > ndv {
				ndv = left.Ndv[i]
			}
			tmp *= ndv
			newStats.SetNdv(attr, left.Ndv[i])
		}
	}

	if tmp <= left.Size {
		newStats.Size = tmp
	} else {
		newStats = left
	}
	return newStats.Size, newStats
}
