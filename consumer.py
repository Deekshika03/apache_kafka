from kafka import KafkaConsumer, KafkaProducer
import json, time

# ── Kafka setup ──────────────────────────────────────────────
consumer = KafkaConsumer(
    'orders',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',
    group_id='order-group'
)

dlq_producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# ── State ────────────────────────────────────────────────────
processed_ids = set()   # idempotency tracker
MAX_RETRIES   = 3

# ── Processing logic ─────────────────────────────────────────
def process_event(event):
    """Simulate processing; raise error for 'FAIL' orders."""
    if event['order'] == 'FAIL':
        raise ValueError(f"Cannot process order: {event['order']}")
    print(f"[Consumer] ✅ Processed: {event}")

def handle_event(event):
    event_id = event['event_id']

    # 1. Idempotency check
    if event_id in processed_ids:
        print(f"[Consumer] ⚠️  Duplicate skipped: event_id={event_id}")
        return

    # 2. Retry with exponential backoff
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            process_event(event)
            processed_ids.add(event_id)   # mark as done only on success
            return
        except Exception as e:
            wait = 2 ** attempt           # 2s, 4s, 8s
            print(f"[Consumer] ❌ Attempt {attempt} failed: {e}. Retrying in {wait}s...")
            time.sleep(wait)

    # 3. Send to DLQ after all retries exhausted
    dlq_producer.send('orders-dlq', event)
    dlq_producer.flush()
    print(f"[Consumer] 🚨 Sent to DLQ: {event}")

# ── Main loop ────────────────────────────────────────────────
print("[Consumer] Listening for events...")
for msg in consumer:
    handle_event(msg.value)
