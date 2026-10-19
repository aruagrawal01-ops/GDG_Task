# Load-test report: 10,000 readings in one second

All numbers below were measured on the machine described here. Nothing is estimated.

## Environment

| Item | Value |
|---|---|
| Machine | Apple M4, 10 cores, 16 GB RAM, macOS (Darwin 27.2) |
| Runtime | Python 3.14.2, Flask 3.1.3, gunicorn 26.2.0 |
| Database | PostgreSQL 14.23, local, default settings (`synchronous_commit = on`) |
| Server settings | 1 gunicorn worker, `gthread`, 16 threads |
| Queue | capacity 50,000 readings, flush up to 5,000 readings per transaction, 100 ms max wait |
| Load generator | `simulator/loadtest.py` (aiohttp), on the same machine, so it competes for CPU |

## Test design

- 10,000 `temperature` readings from 200 sensors. Each run uses fresh sensor ids, so the expected alert count is known exactly.
- Requests are started evenly across one second (`--spread 1`), or all at once (`--spread 0`).
- Two rules are active: a single-reading rule (`temperature > 39`) and a window rule (`avg(temperature) over 60 s > 39.5`, at least 5 samples), so every reading also passes through the window code.
- "Drained" is the time from the first request until `/stats` shows every accepted reading written to PostgreSQL and the queue empty.
- One warm-up run, then three measured runs for each main scenario.

## Results: the required burst (10,000 readings in one second)

| Scenario | Requests | Accepted | p50 | p95 | p99 | 429s | Drained after | Peak queue | Alerts (expected) |
|---|---|---|---|---|---|---|---|---|---|
| 20 x 500, spread over 1 s | 20 | 10,000 (all 3 runs) | 8.0 to 8.1 ms | 9.4 to 9.6 ms | 9.7 to 10.3 ms | 0 | 0.965 to 0.968 s | 500 | 178/178, 168/168, 167/167 |
| 20 x 500, all at once | 20 | 10,000 (all 3 runs) | 16.7 to 32.8 ms | 37.6 to 53.7 ms | 39.1 to 54.9 ms | 0 | 0.165 to 0.177 s | 6,000 to 8,000 | 164/164, 172/172, 171/171 |
| 100 x 100, spread over 1 s | 100 | 10,000 (all 3 runs) | 2.0 to 2.2 ms | 2.6 to 3.1 ms | 3.3 to 4.0 ms | 0 | 1.000 to 1.010 s | 100 | 173/173, 170/170, 164/164 |
| 10,000 x 1, spread over 1 s | 10,000 | 10,000 | 2,618 ms | 4,837 ms | 5,033 ms | 0 | 6.08 s | 48 | 177/177 |

When the burst arrives in batches, every reading is accepted, every reading is in PostgreSQL within about 15 ms of the last request, and the alert count matches exactly. With all 20 requests sent at once, the queue absorbs up to 8,000 readings and the whole burst is stored in 0.18 s.

## Finding the limit

| Offered load (over 1 s) | Accepted | 429 responses | p99 | Drained after | Peak queue |
|---|---|---|---|---|---|
| 50,000 readings (100 x 500) | 50,000 | 0 | 24.4 ms | 1.11 s | 4,000 |
| 100,000 readings (200 x 500) | 100,000 | 0 | 19.6 ms | 1.53 s | 32,000 |
| 200,000 readings (200 x 1,000) | 110,000 | 90 | 26.8 ms | 1.81 s | 50,000 (full) |

- With batches, the service sustained about 100,000 readings per second. Above roughly 110,000 readings in one second, the queue fills and later batches get `429` with `Retry-After: 1`. No accepted reading was lost, and no request failed with an error.
- The bottleneck at that point is the single consumer thread writing to PostgreSQL (the slowest flush of up to 5,000 rows took 98 ms).
- With one reading per HTTP request, the bottleneck is HTTP handling in one Python process. It handles about 1,600 requests per second, so 10,000 separate requests take about 6 s. Nothing was rejected, because those requests waited in the socket backlog, but they did not finish within one second.

## Graceful shutdown under load

60,000 readings were sent as 60 batches of 1,000 at once (55,000 accepted, 5 batches got `429`). SIGTERM was sent while the queue was full (50,000 queued and 5,000 in flight). After the process exited, PostgreSQL held all 55,000 accepted readings.

## Conclusion

The requirement is met for batched ingestion. 10,000 readings in one second are accepted with p99 below 11 ms when spread over the second, and below 55 ms when sent at once. All of them are stored and evaluated within 0.2 s of the last request. There is headroom of about 10x before backpressure starts.

Known limits:

- A crash (`kill -9`, power loss) loses readings that were acknowledged with `202` but are still in the in-memory queue, which is at most 50,000 readings. SIGTERM drains the queue first.
- 10,000 separate single-reading HTTP requests in one second need more than one process. See the scaling notes in [architecture.md](architecture.md).

## Reproduce

```bash
python simulator/loadtest.py --runs 3 --with-rules
python simulator/loadtest.py --runs 3 --spread 0 --no-warmup
python simulator/loadtest.py --runs 3 --batch 100 --no-warmup
python simulator/loadtest.py --runs 1 --batch 1 --connections 200 --no-warmup
python simulator/loadtest.py --runs 1 --readings 200000 --batch 1000 --no-warmup
```
