//go:build !windows

package main

import (
	"fmt"
	"os"
	"syscall"
)

// filesystemIdentity returns the kernel device ID for the filesystem containing
// path. It detects bind mounts and alternate device paths that have different
// mount source strings but still refer to the same mounted filesystem.
func filesystemIdentity(path string) (string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return "", err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return "", fmt.Errorf("无法读取文件系统设备号: %s", path)
	}
	return fmt.Sprintf("dev:%d", uint64(stat.Dev)), nil
}
