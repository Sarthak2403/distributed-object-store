package main

import (
	"flag"
	"log"
	"net"

	"distributed-object-store/internal/storage"
	pb "distributed-object-store/proto"

	"google.golang.org/grpc"
)

func main() {
	port := flag.String("port", "50051", "gRPC server port")
	dataDir := flag.String("data", "./data/node1", "chunk storage directory")
	flag.Parse()

	disk, err := storage.NewDiskStorage(*dataDir)
	if err != nil {
		log.Fatal(err)
	}

	listener, err := net.Listen("tcp", ":"+*port)
	if err != nil {
		log.Fatal(err)
	}

	grpcServer := grpc.NewServer()

	pb.RegisterStorageServiceServer(
		grpcServer,
		storage.NewStorageServer(disk),
	)

	log.Printf("Storage node listening on port %s", *port)

	if err := grpcServer.Serve(listener); err != nil {
		log.Fatal(err)
	}
}
