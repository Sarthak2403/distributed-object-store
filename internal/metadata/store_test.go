package metadata

import (
	"context"
	pb "distributed-object-store/proto"
	"testing"
)

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

func TestReplicaPersistence(t *testing.T) {
	path := t.TempDir()
	store, err := NewStore(path)
	if err != nil {
		t.Fatal(err)
	}
	object := ObjectInfo{ID: "replicated", Chunks: []ChunkInfo{{ID: "chunk", Node: "node1", Replicas: []string{"node1", "node2"}}}}
	if err := store.SaveObject(object); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = NewStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	server := NewMetadataServer(store)
	response, err := server.GetObject(context.Background(), &pb.GetObjectRequest{ObjectId: object.ID})
	if err != nil {
		t.Fatal(err)
	}
	replicas := response.GetObject().GetChunks()[0].GetReplicas()
	if len(replicas) != 2 || replicas[0] != "node1" || replicas[1] != "node2" {
		t.Fatalf("replicas lost: %v", replicas)
	}
	if _, err := server.SaveObject(context.Background(), &pb.SaveObjectRequest{Object: response.GetObject()}); err != nil {
		t.Fatal(err)
	}
	saved, err := store.GetObject(object.ID)
	if err != nil || len(saved.Chunks[0].Replicas) != 2 {
		t.Fatalf("RPC lost replicas: %v", err)
	}
}
