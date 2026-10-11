package metadata

import "testing"

func TestMetadataStore(t *testing.T) {
	store, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	object := ObjectInfo{
		ID: "test-file",
		Chunks: []ChunkInfo{
			{
				ID:       "test-file-0",
				Node:     "localhost:50052",
				Checksum: "abc123",
				Size:     1024,
			},
		},
	}

	if err := store.SaveObject(object); err != nil {
		t.Fatal(err)
	}

	result, err := store.GetObject("test-file")
	if err != nil {
		t.Fatal(err)
	}

	if result.ID != object.ID {
		t.Fatal("Object ID mismatch")
	}

	if len(result.Chunks) != 1 {
		t.Fatal("Incorrect chunk count")
	}

	if result.Chunks[0].Node != "localhost:50052" {
		t.Fatal("Incorrect storage node")
	}
}
