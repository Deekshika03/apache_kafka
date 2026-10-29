from kafka import KafkaProducer
import json, time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Sending 5 events; event_id=3 will simulate a failure in consumer
events = [
    {"event_id": "1", "order": "Pizza"},
    {"event_id": "2", "order": "Burger"},
    {"event_id": "3", "order": "FAIL"},   # This will trigger retries → DLQ
    {"event_id": "2", "order": "Burger"}, # Duplicate → idempotency check
    {"event_id": "4", "order": "Pasta"},
]

for event in events:
    producer.send('orders', event)
    print(f"[Producer] Sent: {event}")
    time.sleep(0.5)

producer.flush()
print("[Producer] All events sent.")
