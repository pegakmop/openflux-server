//go:build !linux

package sysinfo

import "errors"

type statfsT struct {
	Bsize  int64
	Blocks uint64
	Bfree  uint64
}

func statfsPath(_ string, _ *statfsT) error {
	return errors.New("statfs not supported on this platform")
}
