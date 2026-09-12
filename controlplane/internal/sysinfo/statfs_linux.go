//go:build linux

package sysinfo

import "syscall"

type statfsT struct {
	Bsize  int64
	Blocks uint64
	Bfree  uint64
}

func statfsPath(path string, out *statfsT) error {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return err
	}
	out.Bsize = int64(st.Bsize)
	out.Blocks = st.Blocks
	out.Bfree = st.Bfree
	return nil
}
