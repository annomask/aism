package main

import (
	"os"
	"syscall"
)

func allocatedBytes(info os.FileInfo) (int64, bool) {
	s, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, false
	}
	return s.Blocks * 512, true
}
