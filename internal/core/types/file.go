package types

import "time"

type FileInfo struct {
	Path    string
	Size    Bytes
	ModTime time.Time
	ETag    string
}

func NewFileInfo(path string, size Bytes, modTime time.Time, eTag string) *FileInfo {
	return &FileInfo{
		Path:    path,
		Size:    size,
		ModTime: modTime,
		ETag:    eTag,
	}
}
