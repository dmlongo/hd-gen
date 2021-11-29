package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"strconv"
	"time"

	"github.com/cem-okulmus/BalancedGo/lib"
)

var graph string
var width int
var gml string
var enum int
var complete bool
var shrink string
var evaldb string
var evaljoin string

var start time.Time
var durs []time.Duration

type Graph = lib.Graph
type Decomp = lib.Decomp

func main() {
	setFlags()

	dat, err := ioutil.ReadFile(graph)
	if err != nil {
		panic(err)
	}
	hg, parsedGraph := lib.GetGraph(string(dat))
	originalGraph := hg

	var ev Evaluator
	if evaljoin != "" {
		estimates := LoadEstimates(evaljoin, hg, parsedGraph.Encoding)
		ev = EstimateEvaluator{sizes: estimates}
	}

	var addedVertices []int
	if complete {
		addedVertices = hg.MakeEdgesDistinct()
	}

	stop := make(chan bool)
	defer close(stop)

	solver := &DetKStreamer{K: width, Graph: hg}
	fmt.Println("Starting search...")
	i := 0
	start = time.Now()
	for decomp := range solver.Stream(stop) {
		durs = append(durs, time.Since(start))
		if complete {
			decomp.Root.RemoveVertices(addedVertices)
		}
		if !reflect.DeepEqual(decomp, Decomp{}) {
			decomp.Graph = originalGraph
		}
		if shrink != "" {
			decomp = Shrink(decomp, shrink)
		}
		var gmlSeq string
		if gml != "" {
			gmlSeq = gml + "_" + strconv.Itoa(i) + ".gml"
		}
		outputStanza("DetKStreamer", i, decomp, ev, durs, originalGraph, gmlSeq, width, false)
		fmt.Print("\n\n")
		i++
		if enum > 0 && i == enum {
			break
		}
		start = time.Now()
	}
	if !(enum > 0 && i == enum) {
		durs = append(durs, time.Since(start))
	}

	fmt.Println("Time Composition: ")
	for _, t := range durs {
		fmt.Print(t, "\t")
	}
	fmt.Println()

	fmt.Println("\nSearch ended in", sumDurations(durs), "ms.")
	fmt.Println(i, "decompositions were found.")
}

func sumDurations(times []time.Duration) int64 {
	var sumTotal int64
	for _, dur := range times {
		sumTotal = sumTotal + dur.Milliseconds()
	}
	return sumTotal
}

func outputStanza(algorithm string, i int, decomp Decomp, ev Evaluator, times []time.Duration, graph Graph, gml string, K int, skipCheck bool) {
	fmt.Println("Used algorithm: " + algorithm)
	fmt.Println("Result", i, "( ran with K =", K, ")\n", decomp)

	// Print the times
	sumTotal := sumDurations(times)
	fmt.Printf("Time: %.5d ms\n", sumTotal)

	/*fmt.Println("Time Composition: ")
	for _, t := range times {
		fmt.Println(t)
	}*/

	fmt.Println("\nWidth: ", decomp.CheckWidth())
	var correct bool
	if !skipCheck {
		correct = decomp.Correct(graph)
		if !correct {
			panic("wrong decomposition!")
		}
	} else {
		correct = true
	}

	if ev != nil {
		fmt.Println("Cost: ", ev.Eval(decomp))
	}

	fmt.Println("Correct: ", correct)
	if correct && len(gml) > 0 {
		f, err := os.Create(gml)
		if err != nil {
			panic(err)
		}

		defer f.Close()
		f.WriteString(decomp.ToGML())
		f.Sync()
	}
}

func setFlags() {
	flagSet := flag.NewFlagSet("", flag.ContinueOnError)
	flagSet.SetOutput(ioutil.Discard) //todo: see what happens without this line

	flagSet.StringVar(&graph, "graph", "", "Hypergraph to decompose (for format see hyperbench.dbai.tuwien.ac.at/downloads/manual.pdf)")
	flagSet.IntVar(&width, "width", 0, "Width of the decomposition to search for (width > 0)")
	flagSet.StringVar(&gml, "gml", "", "Output the produced decomposition into the specified gml file")
	flagSet.IntVar(&enum, "enum", 0, "Number of decompositions to output (default => all; enum > 0 => min(all, enum))")
	flagSet.BoolVar(&complete, "complete", false, "Forces the computation of complete decompositions")
	flagSet.StringVar(&shrink, "shrink", "", "Remove redundant nodes from the produced decomposition (default => none; soft => bag,cover subsets; hard => bag subsets)")
	flagSet.StringVar(&evaldb, "evaldb", "", "Evaluate decompositions according to a given database") // TODO
	flagSet.StringVar(&evaljoin, "evaljoin", "", "Evaluate decompositions according to given join estimates")

	parseError := flagSet.Parse(os.Args[1:])
	if parseError != nil {
		fmt.Print("Parse Error:\n", parseError.Error(), "\n\n")
	}

	if parseError != nil || graph == "" || width <= 0 {
		out := "Usage of hd-gen (https://github.com/dmlongo/hd-gen)\n"
		flagSet.VisitAll(func(f *flag.Flag) {
			if f.Name != "graph" && f.Name != "width" {
				return
			}
			s := fmt.Sprintf("%T", f.Value) // used to get type of flag
			if s[6:len(s)-5] != "bool" {
				out += fmt.Sprintf("  -%-10s \t<%s>\n", f.Name, s[6:len(s)-5])
			} else {
				out += fmt.Sprintf("  -%-10s \n", f.Name)
			}
			out += fmt.Sprintln("\t" + f.Usage)
		})
		out += fmt.Sprintln("\nOptional Arguments: ")
		flagSet.VisitAll(func(f *flag.Flag) {
			if f.Name == "graph" || f.Name == "width" {
				return
			}
			s := fmt.Sprintf("%T", f.Value) // used to get type of flag
			if s[6:len(s)-5] != "bool" {
				out += fmt.Sprintf("  -%-10s \t<%s>\n", f.Name, s[6:len(s)-5])
			} else {
				out += fmt.Sprintf("  -%-10s \n", f.Name)
			}
			out += fmt.Sprintln("\t" + f.Usage)
		})
		fmt.Fprintln(os.Stderr, out)

		if shrink != "" && shrink != soft && shrink != hard {
			panic(fmt.Errorf("shrink must be either %v or %v", soft, hard))
		}

		os.Exit(1)
	}
}
