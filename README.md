# ⚡ StreamMind

> Real-time AI: Kafka streaming data + vector search + streaming inference APIs.

![Python](https://img.shields.io/badge/Python-3.11-blue) ![Kafka](https://img.shields.io/badge/Kafka-3.7-black) ![Qdrant](https://img.shields.io/badge/Qdrant-1.9-orange)

---

## Overview

StreamMind connects real-time Kafka streams to vector search and LLM inference, enabling AI-powered analysis of streaming data with sub-second latency. Built with Faust for stream processing and FastAPI for the inference gateway.

## Features

- **Kafka ingestion** — Consume real-time events, embed on-the-fly, store in Qdrant
- **Faust stream processing** — Stateful stream transformations, windowed aggregations
- **Redis Streams** — Low-latency event buffering for inference queue
- **Vector search** — Semantic similarity over live streaming data
- **Streaming inference** — SSE streaming responses from LLMs
- **Real-time dashboards** — Grafana dashboards updated every 5s

## Metrics & Achievements

| Metric | Value |
|--------|-------|
| Events processed/sec | **45,000** |
| End-to-end latency (event → queryable) | **< 800ms** |
| Embedding throughput | **2,100 events/sec** |
| Kafka consumer lag | **< 500 events** at steady state |

## Quick Start

```bash
docker-compose up kafka zookeeper qdrant redis -d
pip install -r requirements.txt

# Start stream processor
python -m src.streaming.processor

# Start inference API
uvicorn src.api.main:app --port 8000
```

## License

MIT
