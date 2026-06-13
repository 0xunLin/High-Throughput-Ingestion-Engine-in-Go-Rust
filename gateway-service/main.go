package main

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net"
	"time"

	"gateway-service/pb"

	"github.com/segmentio/kafka-go"
	"google.golang.org/grpc"
)

type server struct {
	pb.UnimplementedIngestionServiceServer
	kafkaWriter *kafka.Writer
}

// StreamTransactions implements the streaming gRPC endpoint
func (s *server) StreamTransactions(stream pb.IngestionService_StreamTransactionsServer) error {
	var count int64 = 0

	for {
		// Read a transaction from the incoming stream
		tx, err := stream.Recv()
		if err == io.EOF {
			// Stream ended by client, send back final status report
			return stream.SendAndClose(&pb.IngestionResponse{
				Success:        true,
				ProcessedCount: count,
			})
		}
		if err != nil {
			log.Printf("Error reading stream: %v", err)
			return err
		}

		// Convert protobuf data payload directly to a standard JSON format string for universal language consumption
		jsonData, err := json.Marshal(tx)
		if err != nil {
			log.Printf("Failed to marshal JSON payload: %v", err)
			continue
		}

		// Emit the message safely onto our running Kafka cluster topic
		err = s.kafkaWriter.WriteMessages(context.Background(), kafka.Message{
			Key:   []byte(tx.Signature),
			Value: jsonData,
		})
		if err != nil {
			log.Printf("Failed to write transaction message to Kafka cluster: %v", err)
			return err
		}

		count++
	}
}

func main() {
	log.Println("Starting Ingestion Gateway Service...")

	// Configure Kafka producer connection configs pointing to our docker broker mapping
	writer := &kafka.Writer{
		Addr:     kafka.TCP("127.0.0.1:9092"),
		Topic:    "raw-transactions",
		Balancer: &kafka.LeastBytes{},
		AllowAutoTopicCreation: true,
		Async:        true,
		BatchTimeout: 5 * time.Millisecond,
	}
	defer writer.Close()

	// Initialize network TCP listener framework on Port 50051
	listener, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to bind port 50051: %v", err)
	}

	// Spin up standard secure gRPC server pipeline configurations
	grpcServer := grpc.NewServer()
	s := &server{kafkaWriter: writer}

	pb.RegisterIngestionServiceServer(grpcServer, s)

	log.Println("gRPC Server actively running and listening on port :50051...")
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("gRPC server runtime failure: %v", err)
	}
}