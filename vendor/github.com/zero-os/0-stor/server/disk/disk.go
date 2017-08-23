package disk

import "syscall"

// FreeSpace return the space available on a disk in bytes
func FreeSpace(path string) (uint64, error) {
	var stat syscall.Statfs_t

	if err := syscall.Statfs(path, &stat); err != nil {
		return 0, err
	}

	// Available blocks * size per block = available space in bytes
	return uint64(stat.Bavail * uint64(stat.Bsize)), nil
}
