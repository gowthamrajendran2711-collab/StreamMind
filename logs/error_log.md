# StreamMind Error Log

## [ERR-001] Faust worker rebalance storm
**Date:** 2024-04-08 | **Severity:** High | **Status:** Resolved

**Description:** Restarting any worker caused full Kafka consumer group rebalance, pausing processing for 40-60s.
**Root Cause:** Default session.timeout.ms=10s too short for embedding-heavy workers under GC pressure.
**Fix:** Set session_timeout_ms=60000, heartbeat_interval_ms=20000.
**Impact:** Rebalance pauses reduced to < 5s. Zero storm events in 30-day monitoring.

---

## [ERR-002] Qdrant upsert throughput bottleneck
**Date:** 2024-04-25 | **Severity:** Medium | **Status:** Resolved

**Description:** Single Qdrant client was bottleneck at ~500 upserts/sec, far below embedding throughput.
**Root Cause:** Synchronous upsert calls blocking the async event loop.
**Fix:** Switched to async qdrant-client with connection pool size=20. Batched upserts to 100 points.
**Impact:** Upsert throughput increased to 2,100/sec, matching embedding throughput.
