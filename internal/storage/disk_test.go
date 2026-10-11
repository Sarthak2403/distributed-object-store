package storage

import (
	"bytes"
	"testing"
)

func TestDiskStorage(t *testing.T) {
	// Create a temporary directory for testing
	dir := t.TempDir()

	// Initialize our storage
	store, err := NewDiskStorage(dir)
	if err != nil {
		t.Fatal(err)
	}

	// Create a sample chunk
	chunkID := "chunk-001"
	originalData := []byte("Hello distributed storage!")

	// Store the chunk
	err = store.StoreChunk(chunkID, originalData)
	if err != nil {
		t.Fatal(err)
	}

	// Retrieve the chunk
	retrievedData, err := store.GetChunk(chunkID)
	if err != nil {
		t.Fatal(err)
	}

	// Verify the retrieved data matches the original
	if !bytes.Equal(originalData, retrievedData) {
		t.Fatal("Retrieved data does not match original data")
	}
}

func TestInvalidChunkIDs(t *testing.T) {
	disk, err := NewDiskStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	invalidIDs := []string{
		"",
		".",
		"..",
		"../secret",
		"folder/chunk",
		`folder\chunk`,
	}

	for _, id := range invalidIDs {
		if err := disk.StoreChunk(id, []byte("test")); err == nil {
			t.Errorf("Expected StoreChunk to reject %q", id)
		}

		if _, err := disk.GetChunk(id); err == nil {
			t.Errorf("Expected GetChunk to reject %q", id)
		}
	}
}
