//go:build integration

package storage_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"distributed-object-store/internal/checksum"
	"distributed-object-store/internal/chunk"
	pb "distributed-object-store/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestDistributedUploadDownload(t *testing.T) {
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
			t.Fatal(err)
		}
		defer conn.Close()

		clients = append(clients, pb.NewStorageServiceClient(conn))
	}

	metaConn, err := grpc.NewClient(
		"localhost:50055",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer metaConn.Close()

	metaClient := pb.NewMetadataServiceClient(metaConn)

	// Generate a unique object ID for this test.
	objectID := fmt.Sprintf("integration-%d", time.Now().UnixNano())

	original := bytes.Repeat([]byte("A"), 2*chunk.ChunkSize+100)

	chunks, err := chunk.Split(bytes.NewReader(original), objectID)
	if err != nil {
		t.Fatal(err)
	}

	object := &pb.ObjectMetadata{Id: objectID}

	// Upload chunks.
	for i, c := range chunks {
		nodeIndex := i % len(clients)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_, err := clients[nodeIndex].StoreChunk(ctx, &pb.StoreChunkRequest{
			ChunkId: c.ID,
			Data:    c.Data,
		})
		cancel()

		if err != nil {
			t.Fatal(err)
		}

		object.Chunks = append(object.Chunks, &pb.ChunkMetadata{
			Id:       c.ID,
			Node:     nodes[nodeIndex],
			Checksum: c.Checksum,
			Size:     int64(len(c.Data)),
		})
	}

	// Persist metadata.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	_, err = metaClient.SaveObject(ctx, &pb.SaveObjectRequest{Object: object})
	cancel()

	if err != nil {
		t.Fatal(err)
	}

	// Retrieve metadata.
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	result, err := metaClient.GetObject(ctx, &pb.GetObjectRequest{
		ObjectId: objectID,
	})
	cancel()

	if err != nil {
		t.Fatal(err)
	}

	// Download and reconstruct.
	var reconstructed []byte

	for _, c := range result.GetObject().GetChunks() {
		nodeIndex := -1

		for i, address := range nodes {
			if address == c.GetNode() {
				nodeIndex = i
				break
			}
		}

		if nodeIndex == -1 {
			t.Fatalf("Unknown node: %s", c.GetNode())
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		response, err := clients[nodeIndex].GetChunk(ctx, &pb.GetChunkRequest{
			ChunkId: c.GetId(),
		})
		cancel()

		if err != nil {
			t.Fatal(err)
		}

		if !checksum.Verify(response.Data, c.GetChecksum()) {
			t.Fatalf("Checksum mismatch: %s", c.GetId())
		}

		reconstructed = append(reconstructed, response.Data...)
	}

	if !bytes.Equal(original, reconstructed) {
		t.Fatal("Reconstructed file does not match original")
	}

	t.Logf("Successfully uploaded and reconstructed %d chunks", len(chunks))
}
