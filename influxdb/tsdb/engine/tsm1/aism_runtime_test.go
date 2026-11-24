package tsm1

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/influxdata/influxdb/v2/tsdb/engine/tsm1/aism"
)

func TestAISMCompactionDamagedSourceSummaryFallsBack(t *testing.T) {
	dir := t.TempDir()
	data, err := os.Create(filepath.Join(dir, "native"))
	if err != nil {
		t.Fatal(err)
	}
	defer data.Close()
	if _, err := data.WriteString("native immutable data"); err != nil {
		t.Fatal(err)
	}
	state := aism.Point(aism.Integer, 1, 7)
	tree, err := aism.Build(aism.Integer, []aism.Node{{State: state}}, []aism.Block{{Offset: 5, Size: 4}})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "index")
	if err := aism.Save(path, data, aism.Index{"key": tree}); err != nil {
		t.Fatal(err)
	}
	index, err := aism.Load(path, data)
	if err != nil {
		t.Fatal(err)
	}
	aism.UseCache(index, aism.NewPageCache(0))
	if err := os.Truncate(path, 0); err != nil {
		t.Fatal(err)
	}
	if _, found := existingLeaf([]aism.Index{index}, "key", IndexEntry{MinTime: 1, MaxTime: 1}, [32]byte{}); found {
		t.Fatal("used a damaged source index")
	}
}
