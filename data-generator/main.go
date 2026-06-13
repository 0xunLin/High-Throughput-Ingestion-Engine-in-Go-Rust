package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log"
	"math/big"
	"time"

	"gateway-service/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func generateRandomHex(length int) string {
	b := make([]byte, length)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func main() {
	log.Println("Starting 5-Minute Steady-State Data Generator...")

	conn, err := grpc.Dial("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to gateway server: %v", err)
	}
	defer conn.Close()

	client := pb.NewIngestionServiceClient(conn)

	stream, err := client.StreamTransactions(context.Background())
	if err != nil {
		log.Fatalf("Failed to open transaction stream: %v", err)
	}

	// Configuration for a 5-minute steady load
	duration := 5 * time.Minute
	recordsPerSecond := 1000
	totalExpected := int(duration.Seconds()) * recordsPerSecond

	log.Printf("Commencing 5-minute streaming sequence. Sending %d TPS (Total: %d payloads)...", recordsPerSecond, totalExpected)

	startTime := time.Now()
	// Create a ticker that fires exactly once per second
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	count := 0

	// Run the loop strictly for the 5-minute duration
	for time.Since(startTime) < duration {
		<-ticker.C // Block until the next second ticks

		// Fire off the batch for this specific second
		for i := 0; i < recordsPerSecond; i++ {
			randomSlot, _ := rand.Int(rand.Reader, big.NewInt(10000000))
			randomFee, _ := rand.Int(rand.Reader, big.NewInt(5000))

			tx := &pb.Transaction{
				Signature: "sig_" + generateRandomHex(32),
				Timestamp: time.Now().UnixMilli(),
				FeePayer:  "pubkey_" + generateRandomHex(16),
				Fee:       randomFee.Uint64(),
				Slot:      randomSlot.String(),
				Instructions: []*pb.Instruction{
					{
						ProgramId: "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
						Accounts:  []string{"acc_src_" + generateRandomHex(8), "acc_dst_" + generateRandomHex(8)},
						Data:      []byte{2, 0, 0, 0, 100, 0, 0, 0},
					},
				},
			}

			if err := stream.Send(tx); err != nil {
				log.Fatalf("Stream send failure: %v", err)
			}
			count++
		}
		
		// Print a progress update to the terminal
		log.Printf("Streamed %d / %d records...", count, totalExpected)
	}

	// Close the stream once 5 minutes is up
	response, err := stream.CloseAndRecv()
	if err != nil {
		log.Fatalf("Failed to receive stream execution acknowledgement: %v", err)
	}

	log.Printf("✅ 5-Minute Stream Complete!")
	log.Printf("Server Response - Success: %v, Processed: %d", response.Success, response.ProcessedCount)
}