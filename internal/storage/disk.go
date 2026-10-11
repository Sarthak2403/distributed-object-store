package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type DiskStorage struct {
	basePath string
}

func NewDiskStorage(basePath string) (*DiskStorage, error) {
	err := os.MkdirAll(basePath, 0755)
	if err != nil {
		return nil, err
	}

	return &DiskStorage{
		basePath: basePath,
	}, nil
}

func (d *DiskStorage) StoreChunk(chunkID string, data []byte) error {
	path, err := d.chunkPath(chunkID)
	if err != nil {
		return err
	}

	return os.WriteFile(path, data, 0644)
}

func (d *DiskStorage) GetChunk(chunkID string) ([]byte, error) {
	path, err := d.chunkPath(chunkID)
	if err != nil {
		return nil, err
	}

	return os.ReadFile(path)
}

func (d *DiskStorage) chunkPath(chunkID string) (string, error) {
	if chunkID == "" || chunkID == "." || chunkID == ".." ||
		filepath.Base(chunkID) != chunkID ||
		strings.ContainsAny(chunkID, `/\`) {
		return "", fmt.Errorf("invalid chunk ID: %q", chunkID)
	}

	return filepath.Join(d.basePath, chunkID), nil
}
