
import json, time, uuid, random
from kafka import KafkaProducer

SAMPLE_TEXTS = [
    "Breaking: Federal Reserve signals potential rate cut in Q4",
    "Tech stocks rally as AI spending forecasts revised upward",
    "New climate agreement signed by 40 nations at summit",
    "Breakthrough in battery technology extends EV range by 40%",
    "Global chip shortage easing as TSMC expands capacity",
]

def produce_events(topic: str = "events", bootstrap_servers: str = "localhost:9092",
                   rate_per_sec: int = 1000, duration_sec: int = 60):
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode()
    )
    start = time.time()
    count = 0
    interval = 1.0 / rate_per_sec

    while time.time() - start < duration_sec:
        event = {
            "id": str(uuid.uuid4()),
            "timestamp": time.time(),
            "text": random.choice(SAMPLE_TEXTS),
            "source": random.choice(["reuters", "bloomberg", "ap", "wsj"]),
            "score": round(random.uniform(0.5, 1.0), 3)
        }
        producer.send(topic, event)
        count += 1
        time.sleep(interval)

    producer.flush()
    print(f"Produced {count} events in {duration_sec}s ({count/duration_sec:.0f}/s)")

if __name__ == "__main__":
    produce_events()
