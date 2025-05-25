package cache

import (
	"fmt"
	"sync"
)

type Cache struct {
	mu       sync.RWMutex
	datasets map[string]*DataSet // dataset name -> dataset
}

func NewCache() *Cache {
	return &Cache{
		datasets: make(map[string]*DataSet),
	}
}

func (c *Cache) AddDataset(dataset *DataSet) (*DataSet, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.datasets[dataset.Name()]; ok {
		return nil, fmt.Errorf("dataset already exists: %s", dataset.Name())
	}
	c.datasets[dataset.Name()] = dataset
	return dataset, nil
}

func (c *Cache) GetDataset(name string) (*DataSet, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	dataset, ok := c.datasets[name]
	if !ok {
		return nil, fmt.Errorf("dataset not found: %s", name)
	}
	return dataset, nil
}
