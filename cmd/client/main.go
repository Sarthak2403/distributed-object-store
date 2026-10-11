package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"distributed-object-store/internal/checksum"
	"distributed-object-store/internal/chunk"
	pb "distributed-object-store/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"os"
	"path/filepath"
)

func main() {
	nodes := []string{
		"localhost:50052",
		"localhost:50053",
		"localhost:50054",
	}

	// Connect to storage nodes
	var clients []pb.StorageServiceClient

	for _, address := range nodes {
		conn, err := grpc.NewClient(
			address,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			log.Fatal(err)
		}
		defer conn.Close()

		clients = append(clients, pb.NewStorageServiceClient(conn))
	}

	// Connect to metadata server
	metaConn, err := grpc.NewClient(
		"localhost:50055",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer metaConn.Close()

	metaClient := pb.NewMetadataServiceClient(metaConn)

	// Simulate a 2.5 MB file
	if len(os.Args) < 3 {
		log.Fatal("Usage: client upload <file> | download <object-id> <output-file>")
	}

	switch os.Args[1] {
	case "upload":
		if err := upload(os.Args[2], nodes, clients, metaClient); err != nil {
			log.Fatal(err)
		}

	case "download":
		if len(os.Args) < 4 {
			log.Fatal("Usage: client download <object-id> <output-file>")
		}

		if err := download(os.Args[2], os.Args[3], metaClient); err != nil {
			log.Fatal(err)
		}

	default:
		log.Fatal("Unknown command")
	}
}

func upload(
	filePath string,
	nodes []string,
	clients []pb.StorageServiceClient,
	metaClient pb.MetadataServiceClient,
) error {
	if len(nodes) != len(clients) {
		return fmt.Errorf("storage node/client count mismatch")
	}
	unique := make(map[string]bool)
	for _, node := range nodes {
		if node != "" {
			unique[node] = true
		}
	}
	if len(unique) < 2 {
		return fmt.Errorf("at least 2 distinct storage nodes are required")
	}
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	objectID := filepath.Base(filePath)
	// Check whether the object already exists.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

	_, err = metaClient.GetObject(ctx, &pb.GetObjectRequest{
		ObjectId: objectID,
	})
	cancel()

	if err == nil {
		return fmt.Errorf("object %q already exists", objectID)
	}

	if status.Code(err) != codes.NotFound {
		return fmt.Errorf("checking existing object: %w", err)
	}

	chunks, err := chunk.Split(file, objectID)
	if err != nil {
		return err
	}

	objectInfo := &pb.ObjectMetadata{Id: objectID}

	for i, c := range chunks {
		var replicas []string
		seen := make(map[string]bool)
		for offset := 0; offset < len(nodes) && len(replicas) < 2; offset++ {
			nodeIndex := (i + offset) % len(nodes)
			address := nodes[nodeIndex]
			if address == "" || seen[address] {
				continue
			}
			seen[address] = true
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			response, err := clients[nodeIndex].StoreChunk(ctx, &pb.StoreChunkRequest{
				ChunkId: c.ID,
				Data:    c.Data,
			})
			cancel()
			if err != nil || !response.GetSuccess() {
				continue
			}
			replicas = append(replicas, address)
		}
		if len(replicas) != 2 {
			return fmt.Errorf("chunk %s: required 2 replica writes, succeeded %d", c.ID, len(replicas))
		}
		objectInfo.Chunks = append(objectInfo.Chunks, &pb.ChunkMetadata{
			Id: c.ID, Node: replicas[0], Replicas: replicas,
			Checksum: c.Checksum, Size: int64(len(c.Data)),
		})
		fmt.Printf("Uploaded %s to %v\n", c.ID, replicas)
	}

	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = metaClient.SaveObject(ctx, &pb.SaveObjectRequest{
		Object: objectInfo,
	})
	if err != nil {
		return err
	}

	fmt.Println("File uploaded successfully!")
	return nil
}

func download(
	objectID string,
	outputPath string,
	metaClient pb.MetadataServiceClient,
) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	response, err := metaClient.GetObject(ctx, &pb.GetObjectRequest{
		ObjectId: objectID,
	})
	cancel()

	if err != nil {
		return err
	}

	file, err := os.CreateTemp(filepath.Dir(outputPath), ".download-*")
	if err != nil {
		return err
	}

	tempPath := file.Name()
	defer os.Remove(tempPath)
	defer file.Close()

	for _, c := range response.GetObject().GetChunks() {
		data, node, err := fetchChunk(c)
		if err != nil {
			return err
		}
		if _, err := file.Write(data); err != nil {
			return err
		}
		fmt.Printf("Downloaded %s from %s\n", c.GetId(), node)
	}

	if err := file.Sync(); err != nil {
		return err
	}

	if err := file.Close(); err != nil {
		return err
	}

	if err := os.Rename(tempPath, outputPath); err != nil {
		return err
	}

	fmt.Println("File downloaded successfully!")
	return nil
}

// Legacy metadata has only Node; keep those objects readable.
func fetchChunk(c *pb.ChunkMetadata) ([]byte, string, error) {
	nodes := append([]string{c.GetNode()}, c.GetReplicas()...)
	seen := make(map[string]bool)
	var lastErr error
	for _, node := range nodes {
		if node == "" || seen[node] {
			continue
		}
		seen[node] = true
		conn, err := grpc.NewClient(node, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			lastErr = err
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		response, err := pb.NewStorageServiceClient(conn).GetChunk(ctx, &pb.GetChunkRequest{ChunkId: c.GetId()})
		cancel()
		conn.Close()
		if err != nil {
			lastErr = err
			continue
		}
		if !checksum.Verify(response.GetData(), c.GetChecksum()) {
			lastErr = fmt.Errorf("checksum mismatch at %s", node)
			continue
		}
		return response.GetData(), node, nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no replica locations")
	}
	return nil, "", fmt.Errorf("chunk %s: all replicas failed: %w", c.GetId(), lastErr)
}
