package chunk

import (
	"bytes"
	"distributed-object-store/internal/checksum"
	"testing"
)

func TestSplit(t *testing.T) {
	// Create 2.5 MB of sample data
	data := bytes.Repeat([]byte("A"), 2*ChunkSize+ChunkSize/2)

	chunks, err := Split(bytes.NewReader(data), "test-object")
	if err != nil {
		t.Fatal(err)
	}

	if len(chunks) != 3 {
		t.Fatalf("Expected 3 chunks, got %d", len(chunks))
	}

	if len(chunks[0].Data) != ChunkSize {
		t.Fatal("First chunk has incorrect size")
	}

	if len(chunks[2].Data) != ChunkSize/2 {
		t.Fatal("Last chunk has incorrect size")
	}

	for _, c := range chunks {
		if !checksum.Verify(c.Data, c.Checksum) {
			t.Fatalf("Checksum verification failed for chunk %s", c.ID)
		}
	}

	// Reconstruct original data
	var reconstructed []byte

	for _, chunk := range chunks {
		reconstructed = append(reconstructed, chunk.Data...)
	}

	if !bytes.Equal(data, reconstructed) {
		t.Fatal("Reconstructed data does not match original")
	}
}
