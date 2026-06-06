"""Faust stream processor: consume Kafka events, embed, store in Qdrant"""
import faust
from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient
from qdrant_client.models import PointStruct
import uuid

app = faust.App("streammind", broker="kafka://localhost:9092", value_serializer="json")
model = SentenceTransformer("all-MiniLM-L6-v2")
qdrant = QdrantClient("http://localhost:6333")

events_topic = app.topic("events", value_type=dict)

@app.agent(events_topic)
async def process_events(events):
    batch = []
    async for event in events.take(100, within=0.5):  # micro-batch
        embedding = model.encode(event.get("text", "")).tolist()
        batch.append(PointStruct(
            id=str(uuid.uuid4()),
            vector=embedding,
            payload=event
        ))
    if batch:
        qdrant.upsert(collection_name="stream_events", points=batch)

if __name__ == "__main__":
    app.main()
