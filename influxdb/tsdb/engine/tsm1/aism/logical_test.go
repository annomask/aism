package aism

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestLogicalCatalogSurvivesPhysicalRelocation(t *testing.T) {
	old := BuildCatalog([]Source{source(t, "1/a.tsm", 0, 30, 0), source(t, "2/b.tsm", 30, 40, 0)}, nil)
	path := filepath.Join(t.TempDir(), "cross-shard.aism")
	if err := SaveCatalog(path, old); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	stamp := time.Unix(123456789, 0)
	if err := os.Chtimes(path, stamp, stamp); err != nil {
		t.Fatal(err)
	}
	src := []Source{source(t, "1/new.tsm", 0, 50, 0), source(t, "2/new.tsm", 50, 20, 0)}
	next := BuildCatalog(src, old)
	if next.Series(testKey).logical != old.Series(testKey).logical {
		t.Fatal("relocation replaced upper records")
	}
	if next.Series(testKey).forests != nil {
		t.Fatal("published physical builder references")
	}
	if err := SaveCatalog(path, next); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !info.ModTime().Equal(stamp) {
		t.Fatal("relocation rewrote the logical catalog")
	}
	other := filepath.Join(t.TempDir(), "copy.aism")
	if err := SaveCatalog(other, next); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(other)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("physical addresses leaked into upper catalog")
	}
	reopened, err := LoadCatalog(path, src)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := collect(t, src, reopened.Series(testKey), 1, 274, 19, 7, MaxLevels)
	want, _ := collect(t, src, reopened.Series(testKey), 1, 274, 19, 7, 0)
	if !reflect.DeepEqual(got, want) {
		t.Fatal("recovery used obsolete locators")
	}
	if _, err := LoadCatalog(path, src[:1]); err == nil {
		t.Fatal("accepted missing coverage")
	}
	changed := []Source{src[0], source(t, "2/new.tsm", 50, 20, 1)}
	if _, err := LoadCatalog(path, changed); err == nil {
		t.Fatal("accepted changed content")
	}
}

func TestLogicalCatalogEmptyCoverageRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cross-shard.aism")
	catalog := BuildCatalog(nil, nil)
	if err := SaveCatalog(path, catalog); err != nil {
		t.Fatal(err)
	}
	got, err := LoadCatalog(path, nil)
	if err != nil || len(got.series) != 0 {
		t.Fatal("empty catalog recovery", err)
	}
}
