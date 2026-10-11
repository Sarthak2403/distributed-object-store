package main

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"

	"distributed-object-store/internal/checksum"
	"distributed-object-store/internal/chunk"
	"distributed-object-store/internal/metadata"
	pb "distributed-object-store/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type testStorage struct {
	pb.UnimplementedStorageServiceServer
	data                  map[string][]byte
	fail, reject, corrupt bool
}

func (s *testStorage) StoreChunk(_ context.Context, r *pb.StoreChunkRequest) (*pb.StoreChunkResponse, error) {
	if s.fail {
		return nil, status.Error(codes.Unavailable, "offline")
	}
	if s.reject {
		return &pb.StoreChunkResponse{}, nil
	}
	s.data[r.ChunkId] = append([]byte(nil), r.Data...)
	return &pb.StoreChunkResponse{Success: true}, nil
}
func (s *testStorage) GetChunk(_ context.Context, r *pb.GetChunkRequest) (*pb.GetChunkResponse, error) {
	if s.fail {
		return nil, status.Error(codes.Unavailable, "offline")
	}
	if s.corrupt {
		return &pb.GetChunkResponse{Data: []byte("corrupt")}, nil
	}
	return &pb.GetChunkResponse{Data: s.data[r.ChunkId]}, nil
}
func serve(t *testing.T, register func(*grpc.Server)) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := grpc.NewServer()
	register(s)
	go s.Serve(l)
	t.Cleanup(s.Stop)
	return l.Addr().String()
}
func connect(t *testing.T, address string) *grpc.ClientConn {
	t.Helper()
	c, e := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { c.Close() })
	return c
}
func TestReplicationAndFailover(t *testing.T) {
	store, e := metadata.NewStore(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { store.Close() })
	address := serve(t, func(s *grpc.Server) { pb.RegisterMetadataServiceServer(s, metadata.NewMetadataServer(store)) })
	meta := pb.NewMetadataServiceClient(connect(t, address))
	var nodes []string
	var clients []pb.StorageServiceClient
	var servers []*testStorage
	for i := 0; i < 3; i++ {
		storage := &testStorage{data: make(map[string][]byte)}
		servers = append(servers, storage)
		address := serve(t, func(s *grpc.Server) { pb.RegisterStorageServiceServer(s, storage) })
		nodes = append(nodes, address)
		clients = append(clients, pb.NewStorageServiceClient(connect(t, address)))
	}
	source := filepath.Join(t.TempDir(), "object")
	data := make([]byte, chunk.ChunkSize+7)
	for i := range data {
		data[i] = byte(i)
	}
	if e := os.WriteFile(source, data, 0600); e != nil {
		t.Fatal(e)
	}
	if e := upload(source, nodes, clients, meta); e != nil {
		t.Fatal(e)
	}
	object, e := store.GetObject("object")
	if e != nil {
		t.Fatal(e)
	}
	for _, c := range object.Chunks {
		if len(c.Replicas) != 2 || c.Replicas[0] == c.Replicas[1] {
			t.Fatalf("invalid replicas: %v", c.Replicas)
		}
		for _, address := range c.Replicas {
			for i, node := range nodes {
				if node == address && !checksum.Verify(servers[i].data[c.ID], c.Checksum) {
					t.Fatal("replica missing")
				}
			}
		}
	}
	output := filepath.Join(t.TempDir(), "output")
	for _, mode := range []string{"offline", "corrupt", "all-failed"} {
		t.Run(mode, func(t *testing.T) {
			servers[0].fail = mode == "offline" || mode == "all-failed"
			servers[0].corrupt = mode == "corrupt"
			servers[1].fail = mode == "all-failed"
			if e := os.WriteFile(output, []byte("existing"), 0600); e != nil {
				t.Fatal(e)
			}
			e := download("object", output, meta)
			got, _ := os.ReadFile(output)
			if mode == "all-failed" {
				if e == nil || string(got) != "existing" {
					t.Fatalf("failed download modified output: %v", e)
				}
				return
			}
			if e != nil {
				t.Fatal(e)
			}
			if string(got) != string(data) {
				t.Fatal("download differs")
			}
		})
	}
	servers[0].fail = true
	servers[1].fail = true
	source2 := filepath.Join(t.TempDir(), "failed")
	os.WriteFile(source2, []byte("data"), 0600)
	if e := upload(source2, nodes, clients, meta); e == nil {
		t.Fatal("upload succeeded without two writes")
	}
	if _, e := store.GetObject("failed"); e == nil {
		t.Fatal("metadata saved after failed writes")
	}
	servers[1].fail = false
	servers[1].reject = true
	if e := upload(source2, nodes, clients, meta); e == nil {
		t.Fatal("false success accepted")
	}
	if _, e := store.GetObject("failed"); e == nil {
		t.Fatal("metadata saved after rejected write")
	}
	servers[1].reject = false
	if e := upload(source2, nodes, clients, meta); e != nil {
		t.Fatalf("upload did not use healthy alternatives: %v", e)
	}
	if e := upload(source2, []string{nodes[2], nodes[2]}, clients[:2], meta); e == nil {
		t.Fatal("duplicate nodes accepted")
	}
}

func TestLegacyDownload(t *testing.T) {
	data := []byte("legacy")
	s := &testStorage{data: map[string][]byte{"old": data}}
	address := serve(t, func(server *grpc.Server) { pb.RegisterStorageServiceServer(server, s) })
	got, _, e := fetchChunk(&pb.ChunkMetadata{Id: "old", Node: address, Checksum: checksum.Calculate(data)})
	if e != nil || string(got) != string(data) {
		t.Fatalf("legacy read: %v", e)
	}
}
