package checksum

import "testing"

func TestChecksum(t *testing.T) {
	data := []byte("Hello distributed storage!")

	hash := Calculate(data)

	if !Verify(data, hash) {
		t.Fatal("Checksum verification failed")
	}

	corrupted := []byte("Hello corrupted storage!")

	if Verify(corrupted, hash) {
		t.Fatal("Corrupted data passed verification")
	}
}
