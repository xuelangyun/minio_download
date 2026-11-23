//go:build windows

package main

// Windows volume identity is derived from the volume device reported by
// gopsutil's partition table. The caller also compares normalized device names;
// filepath.VolumeName alone is intentionally not used because a volume can be
// mounted beneath a directory on a different drive.
func filesystemIdentity(path string) (string, error) {
	return "", nil
}
