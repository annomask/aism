// Command aism-index builds optional aggregate sidecars in an offline snapshot.
package main

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/influxdata/influxdb/v2/tsdb/engine/tsm1"
)

func main() {
	if len(os.Args) == 3 && os.Args[1] == "-inventory" {
		if err := inventory(os.Args[2]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: aism-index OFFLINE-SNAPSHOT-DIRECTORY")
		os.Exit(2)
	}
	shards := map[string]bool{}
	n := 0
	err := filepath.WalkDir(os.Args[1], func(path string, e fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !e.Type().IsRegular() || !strings.HasSuffix(e.Name(), ".tsm") {
			return nil
		}
		shards[filepath.Dir(path)] = true
		return nil
	})
	if err == nil {
		dirs := make([]string, 0, len(shards))
		for dir := range shards {
			dirs = append(dirs, dir)
		}
		sort.Strings(dirs)
		for _, dir := range dirs {
			var k int
			k, err = tsm1.BuildAISMShard(dir)
			if err != nil {
				break
			}
			n += k
		}
	}
	if err == nil && n == 0 {
		err = fmt.Errorf("no TSM files found")
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("indexed %d TSM files\n", n)
}
