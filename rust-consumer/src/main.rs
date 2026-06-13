use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::{ClientConfig, Message};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, QueryBuilder};
use std::time::Duration;
use tokio::time;

#[derive(Debug, Deserialize, Serialize)]
struct Instruction {
    #[serde(default)]
    program_id: String,
    #[serde(default)]
    accounts: Vec<String>,
    #[serde(default)]
    data: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct Transaction {
    #[serde(default)]
    signature: String,
    #[serde(default)]
    timestamp: i64,
    #[serde(default)]
    fee_payer: String,
    #[serde(default)]
    fee: u64,
    #[serde(default)]
    slot: String,
    #[serde(default)]
    instructions: Vec<Instruction>,
}

const BATCH_SIZE: usize = 1000;
const DB_CONN_STR: &str = "postgres://ingestion_admin:supersecretpassword@localhost:5432/ingestion_db";
// FIX: Force IPv4 loopback to prevent librdkafka IPv6 resolving hangs
const KAFKA_BROKER: &str = "127.0.0.1:9092";
const TOPIC: &str = "raw-transactions";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting High-Throughput Diagnostic Rust Consumer...");

    let pool = PgPool::connect(DB_CONN_STR).await?;
    println!("Successfully connected to PostgreSQL.");

    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", "rust-consumer-diagnostic-v1") // Force a fresh read
        .set("bootstrap.servers", KAFKA_BROKER)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        .set("auto.offset.reset", "earliest")
        .create()?;

    consumer.subscribe(&[TOPIC])?;

    let num_cores = num_cpus::get();
    let (tx, rx) = async_channel::bounded::<Vec<u8>>(BATCH_SIZE * num_cores);

    for id in 1..=num_cores {
        let pool_clone = pool.clone();
        let rx_clone = rx.clone();
        tokio::spawn(async move {
            worker(id, pool_clone, rx_clone).await;
        });
    }

    println!("Spun up {} worker tasks. Waiting for messages...", num_cores);

    let mut msg_count = 0;
    loop {
        match consumer.recv().await {
            Err(e) => println!("Kafka error: {}", e),
            Ok(m) => {
                msg_count += 1;
                if msg_count == 1 {
                    println!("✅ SUCCESS: First message received from Kafka!");
                }
                
                if let Some(payload) = m.payload() {
                    if let Err(e) = tx.send(payload.to_vec()).await {
                        println!("Failed to send to worker channel: {}", e);
                    }
                }
            }
        };
    }
}

async fn worker(id: usize, pool: PgPool, rx: async_channel::Receiver<Vec<u8>>) {
    let mut batch: Vec<Transaction> = Vec::with_capacity(BATCH_SIZE);
    let mut interval = time::interval(Duration::from_secs(2));

    loop {
        tokio::select! {
            msg = rx.recv() => {
                match msg {
                    Ok(raw_bytes) => {
                        // FIX: Expose any parsing errors loudly
                        match serde_json::from_slice::<Transaction>(&raw_bytes) {
                            Ok(transaction) => {
                                batch.push(transaction);
                                if batch.len() >= BATCH_SIZE {
                                    flush_batch(&pool, id, &mut batch).await;
                                }
                            }
                            Err(e) => {
                                println!("❌ [Worker {}] JSON Parse Error: {}", id, e);
                                // Print the raw string to see exactly what failed
                                println!("Raw payload: {}", String::from_utf8_lossy(&raw_bytes));
                            }
                        }
                    }
                    Err(_) => break,
                }
            }
            _ = interval.tick() => {
                if !batch.is_empty() {
                    flush_batch(&pool, id, &mut batch).await;
                }
            }
        }
    }
}

async fn flush_batch(pool: &PgPool, worker_id: usize, batch: &mut Vec<Transaction>) {
    if batch.is_empty() {
        return;
    }

    let start = std::time::Instant::now();
    let batch_size = batch.len();

    let mut query_builder = QueryBuilder::new(
        "INSERT INTO transactions (signature, timestamp, fee_payer, fee, slot, instructions) "
    );

    query_builder.push_values(batch.iter(), |mut b, tx| {
        let instr_json = serde_json::to_value(&tx.instructions).unwrap_or(serde_json::Value::Null);
        b.push_bind(&tx.signature)
            .push_bind(tx.timestamp)
            .push_bind(&tx.fee_payer)
            .push_bind(tx.fee as i64)
            .push_bind(&tx.slot)
            .push_bind(instr_json);
    });

    query_builder.push(" ON CONFLICT (signature) DO NOTHING");

    match query_builder.build().execute(pool).await {
        Ok(_) => {
            println!("[Worker {}] Flushed batch of {} records in {:?}", worker_id, batch_size, start.elapsed());
        }
        Err(e) => println!("[Worker {}] Failed to flush batch: {}", worker_id, e),
    }

    batch.clear();
}