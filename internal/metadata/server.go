package metadata

import (
	"context"

	pb "distributed-object-store/proto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type MetadataServer struct {
	pb.UnimplementedMetadataServiceServer
	store *Store
}

func NewMetadataServer(store *Store) *MetadataServer {
	return &MetadataServer{store: store}
}

func (s *MetadataServer) SaveObject(
	ctx context.Context,
	req *pb.SaveObjectRequest,
) (*pb.SaveObjectResponse, error) {
	if req.GetObject() == nil || req.GetObject().GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "object ID is required")
	}

	object := ObjectInfo{
		ID: req.GetObject().GetId(),
	}

	for _, c := range req.GetObject().GetChunks() {
		object.Chunks = append(object.Chunks, ChunkInfo{
			Replicas: append([]string(nil), c.GetReplicas()...),
			ID:       c.GetId(),
			Node:     c.GetNode(),
			Checksum: c.GetChecksum(),
			Size:     int(c.GetSize()),
		})
	}

	if err := s.store.SaveObject(object); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &pb.SaveObjectResponse{Success: true}, nil
}

func (s *MetadataServer) GetObject(
	ctx context.Context,
	req *pb.GetObjectRequest,
) (*pb.GetObjectResponse, error) {
	if req.GetObjectId() == "" {
		return nil, status.Error(codes.InvalidArgument, "object ID is required")
	}

	object, err := s.store.GetObject(req.GetObjectId())
	if err != nil {
		return nil, status.Error(codes.NotFound, "object not found")
	}

	result := &pb.ObjectMetadata{
		Id: object.ID,
	}

	for _, c := range object.Chunks {
		result.Chunks = append(result.Chunks, &pb.ChunkMetadata{
			Replicas: append([]string(nil), c.Replicas...),
			Id:       c.ID,
			Node:     c.Node,
			Checksum: c.Checksum,
			Size:     int64(c.Size),
		})
	}

	return &pb.GetObjectResponse{Object: result}, nil
}
