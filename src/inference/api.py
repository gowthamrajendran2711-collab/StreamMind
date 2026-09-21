import asyncio, json, time
from fastapi import FastAPI, Query
from fastapi.responses import StreamingResponse
from qdrant_client import QdrantClient
from openai import AsyncOpenAI
from sentence_transformers import SentenceTransformer
from prometheus_client import Counter, Histogram, make_asgi_app

app = FastAPI(title="StreamMind Inference API", version="1.0.0")
app.mount("/metrics", make_asgi_app())

STREAM_QUERIES = Counter("streammind_queries_total", "Total streaming queries")
QUERY_LATENCY  = Histogram("streammind_query_latency_seconds", "Query latency")

qdrant = QdrantClient("http://localhost:6333")
openai = AsyncOpenAI()
embedder = SentenceTransformer("all-MiniLM-L6-v2")
@app.get("/stream/search")
async def stream_search(q: str = Query(..., description="Search query"), top_k: int = 5):
    """Search over live streaming data and stream back an LLM-generated answer."""
    STREAM_QUERIES.inc()
    start = time.time()

    async def generate():
        # 1. Embed query
        emb = embedder.encode(q).tolist()

        # 2. Search Qdrant (live data)
        hits = qdrant.search("stream_events", emb, limit=top_k)
        contexts = [h.payload.get("text", "") for h in hits]
        yield f"data: {json.dumps({'type': 'contexts', 'count': len(contexts)})}\n\n"

        # 3. Stream LLM answer
        context_text = "\n".join(contexts)
        stream = await openai.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "Answer based on the real-time data provided."},
                {"role": "user", "content": f"Data:\n{context_text}\n\nQuestion: {q}"}
            ],
            stream=True, temperature=0
        )
        async for chunk in stream:
            delta = chunk.choices[0].delta.content or ""
            if delta:
                yield f"data: {json.dumps({'type': 'token', 'content': delta})}\n\n"

        latency_ms = (time.time()-start)*1000
        QUERY_LATENCY.observe(latency_ms/1000)
        yield f"data: {json.dumps({'type': 'done', 'latency_ms': latency_ms})}\n\n"

    return StreamingResponse(generate(), media_type="text/event-stream")

@app.get("/health")
async def health(): return {"status": "ok"}
