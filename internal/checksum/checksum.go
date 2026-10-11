package checksum

import (
	"crypto/sha256"
	"encoding/hex"
)

func Calculate(data []byte) string {
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}

func Verify(data []byte, expected string) bool {
	return Calculate(data) == expected
}
