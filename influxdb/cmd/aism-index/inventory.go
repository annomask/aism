package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type allocation struct {
	Logical   int64  `json:"logical"`
	Allocated *int64 `json:"allocated"`
}

func inventory(root string) error {
	root, err := filepath.Abs(root)
	if err != nil {
		return err
	}
	stats := map[string]*allocation{}
	for _, key := range []string{"tsm", "aism", "other"} {
		zero := int64(0)
		stats[key] = &allocation{Allocated: &zero}
	}
	var tsm, tombstones [][]any
	var dirs, regular, total int64
	known := true
	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("unexpected symlink: %s", path)
		}
		st, err := d.Info()
		if err != nil {
			return err
		}
		allocated, ok := allocatedBytes(st)
		known = known && ok
		if d.IsDir() {
			dirs += allocated
			return nil
		}
		if !st.Mode().IsRegular() {
			return nil
		}
		kind := "other"
		if strings.HasSuffix(path, ".tsm") {
			kind = "tsm"
		} else if strings.HasSuffix(path, ".tsm.aism") || d.Name() == "shard.aism" {
			kind = "aism"
		}
		stats[kind].Logical += st.Size()
		*stats[kind].Allocated += allocated
		regular += allocated
		total += st.Size()
		if kind == "tsm" || strings.HasSuffix(path, ".tombstone") {
			f, err := os.Open(path)
			if err != nil {
				return err
			}
			h := sha256.New()
			_, err = io.Copy(h, f)
			closeErr := f.Close()
			if err != nil {
				return err
			}
			if closeErr != nil {
				return closeErr
			}
			rel, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			row := []any{filepath.ToSlash(rel), st.Size(), hex.EncodeToString(h.Sum(nil))}
			if kind == "tsm" {
				tsm = append(tsm, row)
			} else {
				tombstones = append(tombstones, row)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	if len(tsm) == 0 {
		return fmt.Errorf("no TSM files found")
	}
	fingerprint := func(rows [][]any) string {
		if rows == nil {
			rows = [][]any{}
		}
		sort.Slice(rows, func(i, j int) bool { return rows[i][0].(string) < rows[j][0].(string) })
		b, _ := json.Marshal(rows)
		sum := sha256.Sum256(b)
		return hex.EncodeToString(sum[:])
	}
	result := map[string]any{"source_root": root, "tsm_sha256": fingerprint(tsm), "tombstones_sha256": fingerprint(tombstones), "tsm_files": len(tsm), "bytes": stats, "total_logical": total}
	if known {
		result["directory_allocated"] = dirs
		result["regular_allocated"] = regular
		result["total_allocated"] = dirs + regular
	} else {
		for _, v := range stats {
			v.Allocated = nil
		}
		result["directory_allocated"] = nil
		result["regular_allocated"] = nil
		result["total_allocated"] = nil
	}
	return json.NewEncoder(os.Stdout).Encode(result)
}
