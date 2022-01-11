package decomp

import (
	"testing"

	"github.com/dmlongo/hd-gen/db"
)

func TestYannakSeq1(t *testing.T) {
	input, joined, partial, output, sols := test1Data()
	y := input
	if sat := y.computeNodes(y.root); !sat || !equals(y.root, joined.root) {
		if !sat {
			t.Error("y(input) is unsat!")
		}
		t.Error("y(input) != joined")
	}
	if sat := y.reduce(y.root); !sat || !equals(y.root, partial.root) {
		if !sat {
			t.Error("y(joined) is unsat!")
		}
		t.Error("y(joined) != partial")
	}
	y.fullyReduce(y.root)
	if !equals(y.root, output.root) {
		t.Error("y(partial) != output")
	}
	if !db.TablesDeepEqual(*y.AllAnswers(), *sols) {
		t.Error("y(output) != solutions")
	}
}

func TestYannakSeq2(t *testing.T) {
	input, joined, partial, output, sols := test2Data()
	y := input
	if sat := y.computeNodes(y.root); !sat || !equals(y.root, joined.root) {
		if !sat {
			t.Error("y(input) is unsat!")
		}
		t.Error("y(input) != joined")
	}
	if sat := y.reduce(y.root); !sat || !equals(y.root, partial.root) {
		if !sat {
			t.Error("y(joined) is unsat!")
		}
		t.Error("y(joined) != partial")
	}
	y.fullyReduce(y.root)
	if !equals(y.root, output.root) {
		t.Error("y(partial) != output")
	}
	if !db.TablesDeepEqual(*y.AllAnswers(), *sols) {
		t.Error("y(output) != solutions")
	}
}

func TestYannakSeq3(t *testing.T) {
	input := test3Data()
	y := input
	if sat := y.computeNodes(y.root) && y.reduce(y.root); sat {
		t.Error("y(input) is sat!")
	}
}

func equals(node1 *yNode, node2 *yNode) bool {
	if !db.TablesEqual(*node1.join, *node2.join) {
		return false
	}
	for i := range node1.children {
		if !equals(node1.children[i], node2.children[i]) {
			return false
		}
	}
	return true
}

func test1Data() (*yTree, *yTree, *yTree, *yTree, *db.Table) {
	//creating input
	dAttrs := []string{"Y", "P"}
	dRel := []db.Tuple{{"3", "8"}, {"3", "7"}, {"5", "7"}, {"6", "7"}}
	dTable := db.NewTable(dAttrs, true)
	dTable.AddTuples(dRel)
	dInput := &yNode{tables: []*db.Table{dTable}}

	rAttrs := []string{"Y", "Z", "U"}
	rRel := []db.Tuple{{"3", "8", "9"}, {"9", "3", "8"}, {"8", "3", "8"}, {"3", "8", "4"}, {"3", "8", "3"}, {"8", "9", "4"}, {"9", "4", "7"}}
	rTable := db.NewTable(rAttrs, true)
	rTable.AddTuples(rRel)
	rInput := &yNode{tables: []*db.Table{rTable}}

	sAttrs := []string{"Z", "U", "W"}
	sRel := []db.Tuple{{"3", "8", "9"}, {"9", "3", "8"}, {"8", "3", "8"}, {"3", "8", "4"}, {"3", "8", "3"}, {"8", "9", "4"}, {"9", "4", "7"}}
	sTable := db.NewTable(sAttrs, true)
	sTable.AddTuples(sRel)
	sInput := &yNode{tables: []*db.Table{sTable}}

	tAttrs := []string{"V", "Z"}
	tRel := []db.Tuple{{"9", "8"}, {"9", "3"}, {"9", "5"}}
	tTable := db.NewTable(tAttrs, true)
	tTable.AddTuples(tRel)
	tInput := &yNode{tables: []*db.Table{tTable}}

	dInput.children = append(dInput.children, rInput)
	rInput.children = append(rInput.children, sInput)
	rInput.children = append(rInput.children, tInput)

	// creating joined nodes
	dJNodes := &yNode{tables: []*db.Table{dTable}, join: dTable}
	rJNodes := &yNode{tables: []*db.Table{rTable}, join: rTable}
	sJNodes := &yNode{tables: []*db.Table{sTable}, join: sTable}
	tJNodes := &yNode{tables: []*db.Table{tTable}, join: tTable}

	dJNodes.children = append(dJNodes.children, rJNodes)
	rJNodes.children = append(rJNodes.children, sJNodes)
	rJNodes.children = append(rJNodes.children, tJNodes)

	// creating partially reduced
	dPartRel := []db.Tuple{{"3", "8"}, {"3", "7"}}
	dPartTable := db.NewTable(dAttrs, false)
	dPartTable.AddTuples(dPartRel)
	dPartial := &yNode{tables: dInput.tables, join: dPartTable}

	rPartRel := []db.Tuple{{"3", "8", "9"}, {"9", "3", "8"}, {"8", "3", "8"}, {"3", "8", "3"}}
	rPartTable := db.NewTable(rAttrs, false)
	rPartTable.AddTuples(rPartRel)
	rPartial := &yNode{tables: rInput.tables, join: rPartTable}

	sPartRel := []db.Tuple{{"3", "8", "9"}, {"9", "3", "8"}, {"8", "3", "8"}, {"3", "8", "4"}, {"3", "8", "3"}, {"8", "9", "4"}, {"9", "4", "7"}}
	sPartTable := db.NewTable(sAttrs, false)
	sPartTable.AddTuples(sPartRel)
	sPartial := &yNode{tables: sInput.tables, join: sPartTable}

	tPartRel := []db.Tuple{{"9", "8"}, {"9", "3"}, {"9", "5"}}
	tPartTable := db.NewTable(tAttrs, false)
	tPartTable.AddTuples(tPartRel)
	tPartial := &yNode{tables: tInput.tables, join: tPartTable}

	dPartial.children = append(dPartial.children, rPartial)
	rPartial.children = append(rPartial.children, sPartial)
	rPartial.children = append(rPartial.children, tPartial)

	//creating output
	dOutRel := []db.Tuple{{"3", "8"}, {"3", "7"}}
	dOutTable := db.NewTable(dAttrs, false)
	dOutTable.AddTuples(dOutRel)
	dOutput := &yNode{tables: dInput.tables, join: dOutTable}

	rOutRel := []db.Tuple{{"3", "8", "9"}, {"3", "8", "3"}}
	rOutTable := db.NewTable(rAttrs, false)
	rOutTable.AddTuples(rOutRel)
	rOutput := &yNode{tables: rInput.tables, join: rOutTable}

	sOutRel := []db.Tuple{{"8", "3", "8"}, {"8", "9", "4"}}
	sOutTable := db.NewTable(sAttrs, false)
	sOutTable.AddTuples(sOutRel)
	sOutput := &yNode{tables: sInput.tables, join: sOutTable}

	tOutRel := []db.Tuple{{"9", "8"}}
	tOutTable := db.NewTable(tAttrs, false)
	tOutTable.AddTuples(tOutRel)
	tOutput := &yNode{tables: tInput.tables, join: tOutTable}

	dOutput.children = append(dOutput.children, rOutput)
	rOutput.children = append(rOutput.children, sOutput)
	rOutput.children = append(rOutput.children, tOutput)

	answers := db.NewTable([]string{"P", "U", "V", "W", "Y", "Z"}, false)
	answers.AddTuples([]db.Tuple{
		{"7", "9", "9", "4", "3", "8"},
		{"8", "9", "9", "4", "3", "8"},
		{"7", "3", "9", "8", "3", "8"},
		{"8", "3", "9", "8", "3", "8"},
	})

	return &yTree{dInput}, &yTree{dJNodes}, &yTree{dPartial}, &yTree{dOutput}, answers
}

func test2Data() (*yTree, *yTree, *yTree, *yTree, *db.Table) {
	//creating input
	dAttrs := []string{"Y", "P"}
	dRel := []db.Tuple{{"3", "8"}, {"3", "7"}, {"5", "7"}, {"6", "7"}}
	dTable := db.NewTable(dAttrs, true)
	dTable.AddTuples(dRel)
	dInput := &yNode{tables: []*db.Table{dTable}}

	rAttrs := []string{"Y", "Z", "U"}
	rRel := []db.Tuple{{"3", "8", "9"}, {"9", "3", "8"}, {"8", "3", "8"}, {"3", "8", "4"}, {"3", "8", "3"}, {"8", "9", "4"}, {"9", "4", "7"}}
	rTable := db.NewTable(rAttrs, true)
	rTable.AddTuples(rRel)
	rInput := &yNode{tables: []*db.Table{rTable}}

	aAttrs := []string{"P", "C"}
	aRel := []db.Tuple{{"8", "4"}, {"8", "7"}, {"4", "9"}, {"3", "5"}}
	aTable := db.NewTable(aAttrs, true)
	aTable.AddTuples(aRel)
	aInput := &yNode{tables: []*db.Table{aTable}}

	sAttrs := []string{"Z", "U", "W"}
	sRel := []db.Tuple{{"3", "8", "9"}, {"9", "3", "8"}, {"8", "3", "8"}, {"3", "8", "4"}, {"3", "8", "3"}, {"8", "9", "4"}, {"9", "4", "7"}}
	sTable := db.NewTable(sAttrs, true)
	sTable.AddTuples(sRel)
	sInput := &yNode{tables: []*db.Table{sTable}}

	tAttrs := []string{"V", "Z"}
	tRel := []db.Tuple{{"9", "8"}, {"9", "3"}, {"9", "5"}}
	tTable := db.NewTable(tAttrs, true)
	tTable.AddTuples(tRel)
	tInput := &yNode{tables: []*db.Table{tTable}}

	bAttrs := []string{"C", "A"}
	bRel := []db.Tuple{{"4", "1"}, {"3", "2"}, {"5", "4"}}
	bTable := db.NewTable(bAttrs, true)
	bTable.AddTuples(bRel)
	bInput := &yNode{tables: []*db.Table{bTable}}

	dInput.children = append(dInput.children, rInput)
	dInput.children = append(dInput.children, aInput)
	rInput.children = append(rInput.children, sInput)
	rInput.children = append(rInput.children, tInput)
	aInput.children = append(aInput.children, bInput)

	// creating joined nodes
	dJNodes := &yNode{tables: []*db.Table{dTable}, join: dTable}
	rJNodes := &yNode{tables: []*db.Table{rTable}, join: rTable}
	aJNodes := &yNode{tables: []*db.Table{aTable}, join: aTable}
	sJNodes := &yNode{tables: []*db.Table{sTable}, join: sTable}
	tJNodes := &yNode{tables: []*db.Table{tTable}, join: tTable}
	bJNodes := &yNode{tables: []*db.Table{bTable}, join: bTable}

	dJNodes.children = append(dJNodes.children, rJNodes)
	dJNodes.children = append(dJNodes.children, aJNodes)
	rJNodes.children = append(rJNodes.children, sJNodes)
	rJNodes.children = append(rJNodes.children, tJNodes)
	aJNodes.children = append(aJNodes.children, bJNodes)

	// creating partially reduced
	dPartRel := []db.Tuple{{"3", "8"}}
	dPartTable := db.NewTable(dAttrs, false)
	dPartTable.AddTuples(dPartRel)
	dPartial := &yNode{tables: dInput.tables, join: dPartTable}

	rPartRel := []db.Tuple{{"3", "8", "9"}, {"9", "3", "8"}, {"8", "3", "8"}, {"3", "8", "3"}}
	rPartTable := db.NewTable(rAttrs, false)
	rPartTable.AddTuples(rPartRel)
	rPartial := &yNode{tables: rInput.tables, join: rPartTable}

	aPartRel := []db.Tuple{{"8", "4"}, {"3", "5"}}
	aPartTable := db.NewTable(aAttrs, false)
	aPartTable.AddTuples(aPartRel)
	aPartial := &yNode{tables: aInput.tables, join: aPartTable}

	sPartRel := []db.Tuple{{"3", "8", "9"}, {"9", "3", "8"}, {"8", "3", "8"}, {"3", "8", "4"}, {"3", "8", "3"}, {"8", "9", "4"}, {"9", "4", "7"}}
	sPartTable := db.NewTable(sAttrs, false)
	sPartTable.AddTuples(sPartRel)
	sPartial := &yNode{tables: sInput.tables, join: sPartTable}

	tPartRel := []db.Tuple{{"9", "8"}, {"9", "3"}, {"9", "5"}}
	tPartTable := db.NewTable(tAttrs, false)
	tPartTable.AddTuples(tPartRel)
	tPartial := &yNode{tables: tInput.tables, join: tPartTable}

	bPartRel := []db.Tuple{{"4", "1"}, {"3", "2"}, {"5", "4"}}
	bPartTable := db.NewTable(bAttrs, false)
	bPartTable.AddTuples(bPartRel)
	bPartial := &yNode{tables: bInput.tables, join: bPartTable}

	dPartial.children = append(dPartial.children, rPartial)
	dPartial.children = append(dPartial.children, aPartial)
	rPartial.children = append(rPartial.children, sPartial)
	rPartial.children = append(rPartial.children, tPartial)
	aPartial.children = append(aPartial.children, bPartial)

	//creating output
	dOutRel := []db.Tuple{{"3", "8"}}
	dOutTable := db.NewTable(dAttrs, false)
	dOutTable.AddTuples(dOutRel)
	dOutput := &yNode{tables: dInput.tables, join: dOutTable}

	rOutRel := []db.Tuple{{"3", "8", "9"}, {"3", "8", "3"}}
	rOutTable := db.NewTable(rAttrs, false)
	rOutTable.AddTuples(rOutRel)
	rOutput := &yNode{tables: rInput.tables, join: rOutTable}

	aOutRel := []db.Tuple{{"8", "4"}}
	aOutTable := db.NewTable(aAttrs, false)
	aOutTable.AddTuples(aOutRel)
	aOutput := &yNode{tables: aInput.tables, join: aOutTable}

	sOutRel := []db.Tuple{{"8", "3", "8"}, {"8", "9", "4"}}
	sOutTable := db.NewTable(sAttrs, false)
	sOutTable.AddTuples(sOutRel)
	sOutput := &yNode{tables: sInput.tables, join: sOutTable}

	tOutRel := []db.Tuple{{"9", "8"}}
	tOutTable := db.NewTable(tAttrs, false)
	tOutTable.AddTuples(tOutRel)
	tOutput := &yNode{tables: tInput.tables, join: tOutTable}

	bOutRel := []db.Tuple{{"4", "1"}}
	bOutTable := db.NewTable(bAttrs, false)
	bOutTable.AddTuples(bOutRel)
	bOutput := &yNode{tables: bInput.tables, join: bOutTable}

	dOutput.children = append(dOutput.children, rOutput)
	dOutput.children = append(dOutput.children, aOutput)
	rOutput.children = append(rOutput.children, sOutput)
	rOutput.children = append(rOutput.children, tOutput)
	aOutput.children = append(aOutput.children, bOutput)

	answers := db.NewTable([]string{"A", "C", "P", "U", "V", "W", "Y", "Z"}, false)
	answers.AddTuples([]db.Tuple{
		{"1", "4", "8", "9", "9", "4", "3", "8"},
		{"1", "4", "8", "3", "9", "8", "3", "8"},
	})

	return &yTree{dInput}, &yTree{dJNodes}, &yTree{dPartial}, &yTree{dOutput}, answers
}

func test3Data() *yTree {
	//creating input
	dAttrs := []string{"Y", "P"}
	dRel := []db.Tuple{{"3", "8"}, {"3", "7"}, {"5", "7"}, {"6", "7"}}
	dTable := db.NewTable(dAttrs, true)
	dTable.AddTuples(dRel)
	dInput := &yNode{tables: []*db.Table{dTable}}

	rAttrs := []string{"Y", "Z", "U"}
	rRel := []db.Tuple{{"3", "8", "9"}, {"9", "3", "8"}, {"8", "3", "8"}, {"3", "8", "4"}, {"3", "8", "3"}, {"8", "9", "4"}, {"9", "4", "7"}}
	rTable := db.NewTable(rAttrs, true)
	rTable.AddTuples(rRel)
	rInput := &yNode{tables: []*db.Table{rTable}}

	sAttrs := []string{"Z", "U", "W"}
	sRel := []db.Tuple{{"3", "8", "9"}, {"9", "3", "8"}, {"8", "3", "8"}, {"3", "8", "4"}, {"3", "8", "3"}, {"8", "9", "4"}, {"9", "4", "7"}}
	sTable := db.NewTable(sAttrs, true)
	sTable.AddTuples(sRel)
	sInput := &yNode{tables: []*db.Table{sTable}}

	tAttrs := []string{"V", "Z"}
	tRel := []db.Tuple{{"9", "7"}, {"9", "6"}, {"9", "5"}}
	tTable := db.NewTable(tAttrs, true)
	tTable.AddTuples(tRel)
	tInput := &yNode{tables: []*db.Table{tTable}}

	dInput.children = append(dInput.children, rInput)
	rInput.children = append(rInput.children, sInput)
	rInput.children = append(rInput.children, tInput)

	return &yTree{dInput}
}
