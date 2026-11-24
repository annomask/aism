//go:build !linux

package main

import "os"

func allocatedBytes(os.FileInfo) (int64, bool) { return 0, false }
