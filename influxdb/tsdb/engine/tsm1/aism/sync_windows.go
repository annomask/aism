//go:build windows

package aism

// Windows does not expose POSIX directory fsync through os.File.
func syncDirectory(string) error { return nil }
