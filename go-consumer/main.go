package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"runtime"
	"strings"
	"sync"
	"time"

	_ "github.com/lib/pq"
	"github.com/segmentio/kafka-go"
)

// Data structures matching our Protobuf JSON output
type Instruction struct {
	ProgramId string   `json:"program_id"`
	Accounts  []string `json:"accounts"`
	Data      string   `json:"data"` 
}

type Transaction struct {
	Signature    string        `json:"signature"`
	Timestamp    int64         `json:"timestamp"`
	FeePayer     string        `json:"fee_payer"`
	Fee          uint64        `json:"fee"`
	Slot         string        `json:"slot"`
	Instructions []Instruction `json:"instructions"`
}

const (
	BatchSize   = 1000
	DbConnStr   = "postgres://ingestion_admin:supersecretpassword@localhost:5432/ingestion_db?sslmode=disable"
	KafkaBroker = "localhost:9092"
	Topic       = "raw-transactions"
)

func main() {
	log.Println("Starting High-Throughput Optimized Go Consumer...")

	// 1. Connect to PostgreSQL
	db, err := sql.Open("postgres", DbConnStr)
	if err != nil {
		log.Fatalf("Failed to open database connection: %v", err)
	}
	defer db.Close()

	// Configure connection pooling
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(25)

	// 2. Configure the Kafka Reader
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{KafkaBroker},
		GroupID:  "go-consumer-group",
		Topic:    Topic,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})
	defer reader.Close()

	// 3. Set up Fan-Out Multi-Core Worker Pool
	numCores := runtime.NumCPU()
	messages := make(chan []byte, BatchSize*numCores)
	var wg sync.WaitGroup

	// Spin up worker Goroutines bound to the number of system CPU cores
	for i := 1; i <= numCores; i++ {
		wg.Add(1)
		go worker(i, db, messages, &wg)
	}

	log.Printf("Spun up %d worker goroutines across all %d CPU cores. Consuming...", numCores, numCores)

	// 4. Main Consumption Loop (Zero parsing overhead here)
	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Error reading from Kafka: %v", err)
			continue
		}

		// Push raw bytes directly to workers to parallelize CPU intensive JSON parsing
		messages <- m.Value
	}
}

// worker accepts raw bytes channel, parallelizing CPU parsing and database batching
func worker(id int, db *sql.DB, messages <-chan []byte, wg *sync.WaitGroup) {
	defer wg.Done()

	batch := make([]Transaction, 0, BatchSize)
	ticker := time.NewTicker(2 * time.Second) 
	defer ticker.Stop()

	for {
		select {
		case rawMsg, ok := <-messages:
			if !ok {
				return
			}

			// Perform CPU-heavy unmarshaling here inside the parallel worker threads
			var tx Transaction
			if err := json.Unmarshal(rawMsg, &tx); err != nil {
				log.Printf("[Worker %d] Failed to unmarshal JSON payload: %v", id, err)
				continue
			}

			batch = append(batch, tx)
			if len(batch) >= BatchSize {
				flushBatch(db, id, batch)
				batch = batch[:0] 
			}
		case <-ticker.C:
			if len(batch) > 0 {
				flushBatch(db, id, batch)
				batch = batch[:0]
			}
		}
	}
}

// flushBatch executes multi-row SQL statements for maximum database performance
func flushBatch(db *sql.DB, workerId int, batch []Transaction) {
	if len(batch) == 0 {
		return
	}

	valueStrings := make([]string, 0, len(batch))
	valueArgs := make([]interface{}, 0, len(batch)*6)
	i := 1

	for _, tx := range batch {
		valueStrings = append(valueStrings, fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d)", i, i+1, i+2, i+3, i+4, i+5))
		instructionsJSON, _ := json.Marshal(tx.Instructions)
		valueArgs = append(valueArgs, tx.Signature, tx.Timestamp, tx.FeePayer, tx.Fee, tx.Slot, string(instructionsJSON))
		i += 6
	}

	stmt := fmt.Sprintf("INSERT INTO transactions (signature, timestamp, fee_payer, fee, slot, instructions) VALUES %s ON CONFLICT (signature) DO NOTHING", strings.Join(valueStrings, ","))

	start := time.Now()
	_, err := db.Exec(stmt, valueArgs...)
	if err != nil {
		log.Printf("[Worker %d] Failed to flush batch: %v", workerId, err)
		return
	}

	log.Printf("[Worker %d] Flushed batch of %d records in %v", workerId, len(batch), time.Since(start))
}