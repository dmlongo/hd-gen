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
	"github.com/dmlongo/hd-gen/db"
	"github.com/dmlongo/hd-gen/decomp"
)

var graph string
var width int
var gml string
var enum int
var complete bool
var shrink string
var evaldb string
var evaljoin string
var mode string
var timeout int

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

	var ev decomp.Evaluator
	if evaldb != "" {
		db := db.Load(evaldb)
		e2t := make(map[int]string)
		for t := range db {
			e := parsedGraph.Encoding[t]
			e2t[e] = t
		}
		ev = decomp.InformedEvaluator{Db: db, Edge2Table: e2t}
	} else if evaljoin != "" {
		estimates := decomp.LoadEstimates(evaljoin, hg, parsedGraph.Encoding)
		ev = decomp.EstimateEvaluator{Sizes: estimates}
	}

	var addedVertices []int
	if complete {
		addedVertices = hg.MakeEdgesDistinct()
	}

	var solver decomp.Streamer
	switch mode {
	case "enum":
		solver = &decomp.DetKStreamer{K: width, Graph: hg}
	case "best":
		detk := &decomp.DetKStreamer{K: width, Graph: hg}
		solver = &decomp.BestDetKStreamer{DetK: detk, Ev: ev}
	case "bnb":
		solver = &decomp.BnbDetKStreamer{K: width, Graph: hg, Ev: ev}
	default:
		panic(fmt.Errorf("mode %v unknown", mode))
	}

	stop := make(chan bool)
	if timeout == 0 {
		defer close(stop)
	} else {
		go func() {
			<-time.After(1 * time.Second)
			close(stop)
			// todo problems if program ends for -enum limit before timeout
		}()
	}

	fmt.Println("Starting search...")
	i := 0
	start = time.Now()
	for dec := range solver.Stream(stop) {
		durs = append(durs, time.Since(start))
		if complete {
			dec.Root.RemoveVertices(addedVertices)
		}
		if !reflect.DeepEqual(dec, Decomp{}) {
			dec.Graph = originalGraph
		}
		if shrink != "" {
			tree := decomp.MakeSearchTree(dec)
			tree.Shrink(shrink)
			dec = decomp.MakeDecomp(*tree)
		}
		var gmlSeq string
		if gml != "" {
			gmlSeq = gml + "_" + strconv.Itoa(i) + ".gml"
		}
		outputStanza(solver.Name(), i, dec, ev, durs, originalGraph, gmlSeq, width, false)
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

func outputStanza(algorithm string, i int, decomp Decomp, ev decomp.Evaluator, times []time.Duration, graph Graph, gml string, K int, skipCheck bool) {
	fmt.Println("Used algorithm: " + algorithm)
	fmt.Println("Result", i, "( ran with K =", K, ")\n", decomp)

	// Print the times
	sumTotal := sumDurations(times)
	fmt.Printf("Time: %.5d ms\n", sumTotal)

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
	flagSet.StringVar(&mode, "mode", "enum", "Mode of the generator (enum, best, bnb)")
	flagSet.StringVar(&gml, "gml", "", "Output the produced decomposition into the specified gml file")
	flagSet.IntVar(&enum, "enum", 0, "Number of decompositions to output (default => all; enum > 0 => min(all, enum))")
	flagSet.BoolVar(&complete, "complete", false, "Forces the computation of complete decompositions")
	flagSet.StringVar(&shrink, "shrink", "", "Remove redundant nodes from the produced decomposition (default => none; soft => bag,cover subsets; hard => bag subsets)")
	flagSet.StringVar(&evaldb, "evaldb", "", "Evaluate decompositions according to a given database") // TODO
	flagSet.StringVar(&evaljoin, "evaljoin", "", "Evaluate decompositions according to given join estimates")
	flagSet.IntVar(&timeout, "timeout", 0, "Set a timeout in milliseconds")

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

		if shrink != "" && shrink != decomp.ShrinkSoftly && shrink != decomp.ShrinkHardly {
			panic(fmt.Errorf("shrink must be either %v or %v", decomp.ShrinkSoftly, decomp.ShrinkHardly))
		}

		if evaldb != "" && evaljoin != "" {
			panic(fmt.Errorf("choose only one between evaldb and evaljoin"))
		}

		if mode != "enum" && mode != "best" && mode != "bnb" {
			panic(fmt.Errorf("mode %v unknown, choose between enum, best, bnb", mode))
		}

		if (mode == "best" || mode == "bnb") && (evaldb == "" && evaljoin == "") {
			panic(fmt.Errorf("mode %v requires either evaldb or evaljoin", mode))
		}

		os.Exit(1)
	}
}
