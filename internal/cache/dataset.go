package cache

import (
	"fmt"
	"sync"
)

type DataSet struct {
	name  string
	mu    sync.RWMutex
	files map[string]*FileEntry // file path -> file entry
}

func NewDataSet(name string) *DataSet {
	return &DataSet{
		name:  name,
		files: make(map[string]*FileEntry),
	}
}

func (d *DataSet) Name() string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.name
}

func (d *DataSet) AddFile(entry *FileEntry) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.files[entry.Path()] = entry
}

func (d *DataSet) GetFile(path string) (*FileEntry, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	entry, ok := d.files[path]
	if !ok {
		return nil, fmt.Errorf("file not found: %s", path)
	}
	return entry, nil
}
