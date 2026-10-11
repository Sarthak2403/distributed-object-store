package chunk

import (
	"fmt"
	"io"

	"distributed-object-store/internal/checksum"
)

const ChunkSize = 1024 * 1024 // 1 MB

type Chunk struct {
	ID       string
	Index    int
	Data     []byte
	Checksum string
}

func Split(reader io.Reader, objectID string) ([]Chunk, error) {
	var chunks []Chunk
	buffer := make([]byte, ChunkSize)

	for index := 0; ; index++ {
		n, err := io.ReadFull(reader, buffer)

		if err == io.EOF {
			break
		}

		if err != nil && err != io.ErrUnexpectedEOF {
			return nil, err
		}

		data := make([]byte, n)
		copy(data, buffer[:n])

		chunks = append(chunks, Chunk{
			ID:       fmt.Sprintf("%s-%d", objectID, index),
			Index:    index,
			Data:     data,
			Checksum: checksum.Calculate(data),
		})

		if err == io.ErrUnexpectedEOF {
			break
		}
	}

	return chunks, nil
}
