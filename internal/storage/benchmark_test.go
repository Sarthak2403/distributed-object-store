package storage

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	pb "distributed-object-store/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func BenchmarkConcurrentUploads(b *testing.B) {
	nodes := []string{
		"localhost:50052",
		"localhost:50053",
		"localhost:50054",
	}

	clients := make([]pb.StorageServiceClient, 0, len(nodes))

	for _, address := range nodes {
		conn, err := grpc.NewClient(
			address,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			b.Fatal(err)
		}
		defer conn.Close()

		clients = append(clients, pb.NewStorageServiceClient(conn))
	}

	data := bytes.Repeat([]byte("A"), 1024*1024)

	for _, concurrency := range []int{1, 5, 10, 20} {
		b.Run(fmt.Sprintf("clients-%d", concurrency), func(b *testing.B) {
			b.SetBytes(int64(len(data) * concurrency))

			for i := 0; i < b.N; i++ {
				var wg sync.WaitGroup
				errors := make(chan error, concurrency)

				// start := time.Now()

				for j := 0; j < concurrency; j++ {
					wg.Add(1)

					go func(index int) {
						defer wg.Done()

						nodeIndex := index % len(clients)

						ctx, cancel := context.WithTimeout(
							context.Background(),
							30*time.Second,
						)
						defer cancel()

						_, err := clients[nodeIndex].StoreChunk(
							ctx,
							&pb.StoreChunkRequest{
								ChunkId: fmt.Sprintf(
									"benchmark-%d-%d-%d",
									concurrency, i, index,
								),
								Data: data,
							},
						)

						if err != nil {
							errors <- err
						}
					}(j)
				}

				wg.Wait()
				close(errors)

				for err := range errors {
					b.Fatal(err)
				}

				// _ = time.Since(start)
			}
		})
	}
}
