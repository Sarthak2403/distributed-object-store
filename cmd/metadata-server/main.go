package main

import (
	"flag"
	"log"
	"net"

	"distributed-object-store/internal/metadata"
	pb "distributed-object-store/proto"

	"google.golang.org/grpc"
)

func main() {
	port := flag.String("port", "50055", "Metadata server port")
	dataDir := flag.String("data", "./data/metadata", "Metadata database directory")
	flag.Parse()

	store, err := metadata.NewStore(*dataDir)
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()

	listener, err := net.Listen("tcp", ":"+*port)
	if err != nil {
		log.Fatal(err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterMetadataServiceServer(
		grpcServer,
		metadata.NewMetadataServer(store),
	)

	log.Printf("Metadata server listening on port %s", *port)

	if err := grpcServer.Serve(listener); err != nil {
		log.Fatal(err)
	}
}
