#!/bin/bash
# Ensure strict error handling
set -e

TARGET_RECORDS=50000 

# Intercept manual Ctrl+C to cleanly kill absolutely everything
cleanup() {
    echo -e "\n🛑 Cleaning up background processes..."
    # FIX: Use -x to match exact process names, avoiding the 'time' wrapper
    pkill -TERM -x "go-consumer-bin" 2>/dev/null || true
    pkill -TERM -x "rust-consumer" 2>/dev/null || true
    pkill -TERM -x "data-gen-bin" 2>/dev/null || true
}
trap 'cleanup; exit 1' SIGINT SIGTERM

echo "========================================="
echo "🚀 OLake Automated Benchmark Suite"
echo "========================================="

# ---------------------------------------------------------
# 0. COMPILE GENERATOR & SAFETY WIPE
# ---------------------------------------------------------
echo "⚙️  Compiling Data Generator..."
cd data-generator
go build -o data-gen-bin main.go
cd ..

echo -e "\n🧹 INITIALIZATION: Ensuring a completely empty database..."
docker exec ingestion-postgres psql -U ingestion_admin -d ingestion_db -c "TRUNCATE transactions;" > /dev/null

# ---------------------------------------------------------
# 1. GO BENCHMARK
# ---------------------------------------------------------
echo -e "\n🔵 RUNNING GO CONSUMER BENCHMARK"

echo "⚙️  Compiling Go binary..."
cd go-consumer
go build -o go-consumer-bin main.go

echo "⏳ Starting Go Consumer in background..."
# Start the wrapper and explicitly capture its Process ID
/usr/bin/time -f "%e,%M,%P,%w,%R" -o ../go_metrics.csv ./go-consumer-bin > /dev/null 2>&1 &
GO_TIME_PID=$!
cd ..

echo "🔥 Firing Data Generator..."
cd data-generator
./data-gen-bin > /dev/null 2>&1 &
GEN_PID=$!
cd ..

# Real-time Database Polling Loop
while true; do
    COUNT=$(docker exec ingestion-postgres psql -U ingestion_admin -d ingestion_db -tA -c "SELECT count(*) FROM transactions;" 2>/dev/null | tr -d '[:space:]')
    if [ -z "$COUNT" ]; then COUNT=0; fi
    
    echo -ne "   Database Rows Processed: $COUNT / $TARGET_RECORDS\r"
    
    if [ "$COUNT" -ge "$TARGET_RECORDS" ]; then
        echo -e "\n✅ Target reached! Stopping Go pipeline..."
        break
    fi
    sleep 0.5
done

# Step 1: Kill the generator to stop the flood of data
kill -TERM $GEN_PID 2>/dev/null || true
# FIX: Tell the consumer to shut down using EXACT process name (-x)
pkill -TERM -x "go-consumer-bin" || true

echo "⏳ Waiting for Go metrics to be saved to disk..."
# Step 3: Block the script until /usr/bin/time confirms the file is written
wait $GO_TIME_PID 2>/dev/null || true

# ---------------------------------------------------------
# 2. RUST BENCHMARK
# ---------------------------------------------------------
echo -e "\n🟠 RUNNING RUST CONSUMER BENCHMARK"
echo "🧹 Wiping Postgres database for Rust baseline..."
docker exec ingestion-postgres psql -U ingestion_admin -d ingestion_db -c "TRUNCATE transactions;" > /dev/null

echo "⚙️  Compiling Rust release binary..."
cd rust-consumer
cargo build --release

echo "⏳ Starting Rust Consumer in background..."
/usr/bin/time -f "%e,%M,%P,%w,%R" -o ../rust_metrics.csv ./target/release/rust-consumer > /dev/null 2>&1 &
RUST_TIME_PID=$!
cd ..

echo "🔥 Firing Data Generator..."
cd data-generator
./data-gen-bin > /dev/null 2>&1 &
GEN_PID=$!
cd ..

# Real-time Database Polling Loop
while true; do
    COUNT=$(docker exec ingestion-postgres psql -U ingestion_admin -d ingestion_db -tA -c "SELECT count(*) FROM transactions;" 2>/dev/null | tr -d '[:space:]')
    if [ -z "$COUNT" ]; then COUNT=0; fi
    
    echo -ne "   Database Rows Processed: $COUNT / $TARGET_RECORDS\r"
    
    if [ "$COUNT" -ge "$TARGET_RECORDS" ]; then
        echo -e "\n✅ Target reached! Stopping Rust pipeline..."
        break
    fi
    sleep 0.5
done

kill -TERM $GEN_PID 2>/dev/null || true
# FIX: Target the exact process name
pkill -TERM -x "rust-consumer" || true

echo "⏳ Waiting for Rust metrics to be saved to disk..."
wait $RUST_TIME_PID 2>/dev/null || true

# ---------------------------------------------------------
# 3. VISUALIZATION & TEARDOWN
# ---------------------------------------------------------
echo -e "\n📈 FEEDING DATA TO PYTHON VISUALIZER..."
python3 visualize_benchmarks.py

echo -e "\n🧹 TEARDOWN: Wiping database to leave a clean environment..."
docker exec ingestion-postgres psql -U ingestion_admin -d ingestion_db -c "TRUNCATE transactions;" > /dev/null

echo "========================================="
echo "🎉 Benchmark Suite Complete & Environment Cleaned!"
echo "========================================="