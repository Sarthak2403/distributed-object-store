package storage

import (
	"context"

	pb "distributed-object-store/proto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type StorageServer struct {
	pb.UnimplementedStorageServiceServer
	disk *DiskStorage
}

func NewStorageServer(disk *DiskStorage) *StorageServer {
	return &StorageServer{disk: disk}
}

func (s *StorageServer) StoreChunk(
	ctx context.Context,
	req *pb.StoreChunkRequest,
) (*pb.StoreChunkResponse, error) {

	if req.ChunkId == "" {
		return nil, status.Error(codes.InvalidArgument, "chunk ID is required")
	}

	if err := s.disk.StoreChunk(req.ChunkId, req.Data); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to store chunk: %v", err)
	}

	return &pb.StoreChunkResponse{Success: true}, nil
}

func (s *StorageServer) GetChunk(
	ctx context.Context,
	req *pb.GetChunkRequest,
) (*pb.GetChunkResponse, error) {

	if req.ChunkId == "" {
		return nil, status.Error(codes.InvalidArgument, "chunk ID is required")
	}

	data, err := s.disk.GetChunk(req.ChunkId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to retrieve chunk: %v", err)
	}

	return &pb.GetChunkResponse{Data: data}, nil
}