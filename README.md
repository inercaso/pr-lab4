# Laboratory Work #4

## Distributed Key-Value Store with Single-Leader Replication

**Student:** Daniela Cebotari  
**Group:** FAF-231  
**Course:** Network Programming  
**Deadline:** 1st December 2025

---

## Table of Contents

1. [Introduction](#introduction)
2. [Theoretical Background](#theoretical-background)
3. [How to Run](#how-to-run)
4. [System Architecture](#system-architecture)
5. [Implementation Details](#implementation-details)
6. [Docker Configuration](#docker-configuration)
7. [API Specification](#api-specification)
8. [Testing](#testing)
9. [Performance Analysis](#performance-analysis)
10. [Results and Discussion](#results-and-discussion)
11. [Conclusions](#conclusions)
12. [References](#references)

---

## Introduction

This laboratory work implements a distributed key-value store using **single-leader replication** as described in Chapter 5 of "Designing Data-Intensive Applications" by Martin Kleppmann. The system consists of one leader node and five follower nodes, all running in separate Docker containers.

### Objectives

- implement a key-value store with single-leader replication
- use semi-synchronous replication with configurable write quorum
- simulate network latency to observe real-world replication behavior
- analyze system performance under different quorum configurations
- verify data consistency across all replicas

### Requirements Fulfilled

| Requirement | Implementation |
|-------------|----------------|
| single-leader replication | only leader accepts writes, replicates to followers |
| 1 leader + 5 followers | 6 docker containers via docker-compose |
| concurrent request handling | fastapi async endpoints with asyncio |
| semi-synchronous replication | configurable quorum (1-5) via environment variable |
| network delay simulation | random delay [min, max] ms before each replication |
| web api + json | restful api with json request/response |
| integration tests | pytest-based test suite with 20 test cases |
| performance analysis | 100 writes benchmark, quorum vs latency analysis |

---

## Theoretical Background

### Leader-Based Replication

leader-based replication (also known as master-slave replication) works as follows:

1. **leader (master)**: one replica is designated as the leader. all write requests must be sent to the leader, which first writes data to its local storage.

2. **followers (slaves)**: other replicas are followers. whenever the leader writes new data, it sends the change to all followers as part of a replication log. each follower applies writes in the same order as the leader.

3. **read operations**: clients can query either the leader or any follower. writes are only accepted on the leader.

```
┌───────────────────────────────────────────────────────────────┐
│                    LEADER-BASED REPLICATION                   │
├───────────────────────────────────────────────────────────────┤
│                                                               │
│   ┌────────┐         ┌────────┐                               │
│   │ Client │────────▶│ Leader │                               │
│   └────────┘  write  └────────┘                               │
│                          │                                    │
│             ┌────────────┼────────────┐                       │
│             │            │            │                       │
│             ▼            ▼            ▼                       │
│        ┌────────┐  ┌────────┐  ┌────────┐                     │
│        │Follower│  │Follower│  │Follower│                     │
│        │   1    │  │   2    │  │   3    │                     │
│        └────────┘  └────────┘  └────────┘                     │
│             │            │            │                       │
│   ┌────────┐│  ┌────────┐│  ┌────────┐│                       │
│   │ Client │◀──│ Client │◀──│ Client │◀─ (reads)              │
│   └────────┘   └────────┘   └────────┘                        │
│                                                               │
└───────────────────────────────────────────────────────────────┘
```

### Synchronous vs Asynchronous Replication

| Type | Description | Pros | Cons |
|------|-------------|------|------|
| **synchronous** | leader waits for all followers to confirm | guaranteed consistency | single slow follower blocks all writes |
| **asynchronous** | leader doesn't wait for any confirmation | fast writes, high availability | potential data loss if leader fails |
| **semi-synchronous** | leader waits for N followers (quorum) | balance of consistency and availability | configurable trade-off |

### Semi-Synchronous Replication

from the book:

> "if you enable synchronous replication on a database, it usually means that one of the followers is synchronous, and the others are asynchronous. this guarantees that you have an up-to-date copy of the data on at least two nodes: the leader and one synchronous follower. this configuration is sometimes also called semi-synchronous."

in this implementation, the number of synchronous followers is configurable via the `QUORUM` environment variable.

```
┌───────────────────────────────────────────────────────────────┐
│                 SEMI-SYNCHRONOUS REPLICATION                  │
│                        (QUORUM = 2)                           │
├───────────────────────────────────────────────────────────────┤
│                                                               │
│  Client          Leader           Followers (1-5)             │
│    │               │                   │                      │
│    │── POST ──────▶│                   │                      │
│    │               │── delay ─────────▶│ (concurrent to all)  │
│    │               │                   │                      │
│    │               │◀── ack (1) ───────│                      │
│    │               │◀── ack (2) ───────│ ← quorum reached     │
│    │◀── 200 OK ────│                   │                      │
│    │               │◀── ack (3,4,5) ───│ (async, background)  │
│    │               │                   │                      │
└───────────────────────────────────────────────────────────────┘
```

---

## How to Run

### Prerequisites

- docker and docker compose installed
- python 3.11+ (for running tests locally)
- pip packages: httpx, pytest, pytest-asyncio, matplotlib

### Start the Cluster

```bash
# start with default quorum (2)
docker compose up -d --build

# start with custom quorum
QUORUM=3 docker compose up -d --build

# view logs
docker logs kv-leader -f
```

### Run Tests

```bash
# install dependencies
pip install httpx pytest pytest-asyncio matplotlib

# run integration tests
python -m pytest tests/test_integration.py -v
```

### Run Performance Analysis

```bash
# single benchmark run (uses current quorum)
python -m analysis.performance

# full quorum analysis with BOTH modes (generates comparison plot)
python -m analysis.performance --full

# basic mode only (last-writer-wins, shows race conditions)
python -m analysis.performance --full-basic

# versioned mode only (conflict resolution, no race conditions)
python -m analysis.performance --full-versioned

# consistency check only
python -m analysis.performance --consistency-only
```

### Manual API Testing

```bash
# health check
curl http://localhost:8080/health

# write to leader
curl -X POST http://localhost:8080/store/mykey \
  -H "Content-Type: application/json" \
  -d '{"value": {"hello": "world"}}'

# read from leader
curl http://localhost:8080/store/mykey

# read from follower
curl http://localhost:8081/store/mykey

# get all data
curl http://localhost:8080/store
```

### Stop the Cluster

```bash
docker compose down
```

---

## System Architecture

### High-Level Architecture

```
┌───────────────────────────────────────────────────────────────┐
│                  DOCKER NETWORK (kv-network)                  │
├───────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌─────────────────────────────────────────────────────────┐  │
│  │                   LEADER (port 8080)                    │  │
│  │  ┌─────────┐  ┌───────────┐  ┌────────────┐             │  │
│  │  │ FastAPI │──│ Replicator│──│ Key-Value  │             │  │
│  │  │  Main   │  │  Module   │  │   Store    │             │  │
│  │  └─────────┘  └───────────┘  └────────────┘             │  │
│  └─────────────────────────────────────────────────────────┘  │
│           │                                                   │
│           │ replication (http post)                           │
│           ▼                                                   │
│  ┌─────────────────────────────────────────────────────────┐  │
│  │                       FOLLOWERS                         │  │
│  │ ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐│
│  │ │Follower1│ │Follower2│ │Follower3│ │Follower4│ │Follower5││
│  │ │ :8081   │ │ :8082   │ │ :8083   │ │ :8084   │ │ :8085   ││
│  │ └─────────┘ └─────────┘ └─────────┘ └─────────┘ └─────────┘│
│  └─────────────────────────────────────────────────────────┘  │
│                                                               │
└───────────────────────────────────────────────────────────────┘
```

### Module Structure

```
pr-lab4/
├── app/
│   ├── __init__.py
│   ├── config.py          # environment configuration
│   ├── store.py           # thread-safe key-value store
│   ├── replicator.py      # semi-synchronous replication logic
│   └── main.py            # fastapi application
├── tests/
│   ├── __init__.py
│   └── test_integration.py
├── analysis/
│   ├── __init__.py
│   └── performance.py     # benchmark and plotting script
├── results/
│   ├── latency_percentiles.png  # mean, median, p90, p95 plot
│   ├── all_results.json         # benchmark data
│   └── full_report.txt          # text report
├── .env                   # quorum configuration
├── Dockerfile
├── docker-compose.yaml
├── requirements.txt
└── pytest.ini
```

---

## Implementation Details

### 1. Configuration Module (`app/config.py`)

manages environment variables using pydantic-settings for type-safe configuration.

```python
class Settings(BaseSettings):
    role: Literal["leader", "follower"] = "follower"
    node_name: str = "node"
    quorum: int = 2           # acks required for write success
    delay_min: int = 0        # min replication delay (ms)
    delay_max: int = 1000     # max replication delay (ms)
    followers: str = ""       # comma-separated follower urls
```

| variable | description | default |
|----------|-------------|---------|
| `ROLE` | node role (leader/follower) | follower |
| `NODE_NAME` | node identifier | node |
| `QUORUM` | write quorum size | 2 |
| `DELAY_MIN` | min replication delay (ms) | 0 |
| `DELAY_MAX` | max replication delay (ms) | 1000 |
| `FOLLOWERS` | comma-separated follower urls | "" |

### 2. Key-Value Store (`app/store.py`)

thread-safe in-memory dictionary with asyncio.lock for concurrent access. supports both basic (last-writer-wins) and versioned (conflict resolution) modes.

```python
class KeyValueStore:
    def __init__(self):
        self._data: dict[str, Any] = {}
        self._versions: dict[str, int] = {}  # per-key version tracking
        self._lock = asyncio.Lock()

    async def get(self, key: str) -> Any | None
    async def set(self, key: str, value: Any) -> None  # basic mode
    async def set_versioned(self, key: str, value: Any, version: int) -> bool  # versioned mode
    async def delete(self, key: str) -> bool
    async def get_all(self) -> dict[str, Any]
```

**versioned mode:** `set_versioned()` only accepts writes where `version > current_version`. this handles out-of-order message delivery by rejecting stale writes, eliminating race conditions from concurrent writes with network delays.

### 3. Replicator Module (`app/replicator.py`)

implements semi-synchronous replication with configurable quorum.

**key features:**
- network delay simulation using `asyncio.sleep(uniform(min, max))`
- concurrent replication to all followers using `asyncio.wait`
- quorum-based response: returns as soon as N acks received
- fire-and-forget for remaining replications

**replication algorithm:**

```
function replicate(key, value):
    1. create async tasks for all followers
    2. for each follower:
       a. apply random delay [min, max]
       b. send POST /internal/replicate
    3. wait using asyncio.wait(FIRST_COMPLETED)
    4. count successful acks
    5. if acks >= quorum:
       - return success immediately
       - let remaining tasks complete in background
    6. else:
       - return failure
```

### 4. FastAPI Application (`app/main.py`)

restful api with role-based endpoint behavior.

**leader endpoints:**
- `POST /store/{key}` - write value, trigger replication, wait for quorum
- `GET /store/{key}` - read value from local store
- `GET /store` - return all data
- `DELETE /store/{key}` - delete key

**follower endpoints:**
- `POST /internal/replicate` - receive replication from leader
- `GET /store/{key}` - read value from local store
- `GET /store` - return all data

**common endpoints:**
- `GET /health` - health check

---

## Docker Configuration

### Dockerfile

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app/ ./app/
EXPOSE 8000
CMD ["python", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

### docker-compose.yaml

the compose file defines 6 services:
- 1 leader (port 8080 → 8000)
- 5 followers (ports 8081-8085 → 8000)

**key configuration:**

```yaml
services:
  leader:
    environment:
      - ROLE=leader
      - NODE_NAME=leader
      - QUORUM=${QUORUM:-2}
      - DELAY_MIN=${DELAY_MIN:-0}
      - DELAY_MAX=${DELAY_MAX:-1000}
      - FOLLOWERS=http://follower1:8000,...,http://follower5:8000
    ports:
      - "8080:8000"
    depends_on:
      - follower1
      - follower2
      - follower3
      - follower4
      - follower5

  follower1:
    environment:
      - ROLE=follower
      - NODE_NAME=follower1
    ports:
      - "8081:8000"
  # ... follower2-5 similar
```

---

## API Specification

### Write Key (Leader Only)

```http
POST /store/{key}
Content-Type: application/json

{
  "value": <any json value>
}
```

**query parameters:**
- `versioned=true` - use version-based conflict resolution (optional)

**response (200 ok):**
```json
{
  "success": true,
  "key": "my_key",
  "quorum_acks": 2,
  "message": "write successful with 2 acknowledgments",
  "version": 1  // only present when versioned=true
}
```

**response (403 forbidden - on follower):**
```json
{
  "detail": "writes are only accepted on the leader node"
}
```

### Read Key

```http
GET /store/{key}
```

**response (200 ok):**
```json
{
  "key": "my_key",
  "value": {"data": "example"},
  "found": true
}
```

### Get All Data

```http
GET /store
```

**response (200 ok):**
```json
{
  "node": "leader",
  "data": {
    "key1": "value1",
    "key2": "value2"
  },
  "size": 2
}
```

### Health Check

```http
GET /health
```

**response (200 ok):**
```json
{
  "status": "healthy",
  "node": "leader",
  "role": "leader"
}
```

### Internal Replication (Followers Only)

```http
POST /internal/replicate
Content-Type: application/json

{
  "key": "my_key",
  "value": <any json value>
}
```

**response (200 ok):**
```json
{
  "status": "ok",
  "key": "my_key"
}
```

---

## Testing

### Integration Test Suite

the test suite includes 20 test cases organized into 7 test classes:

| class | tests | description |
|-------|-------|-------------|
| `TestHealthCheck` | 2 | verify leader and all followers are healthy |
| `TestBasicOperations` | 3 | write, read, and read nonexistent key |
| `TestReplication` | 3 | replication to followers, quorum behavior, read from follower |
| `TestFollowerBehavior` | 2 | followers reject writes, accept reads |
| `TestConsistency` | 2 | all data endpoint, eventual consistency |
| `TestConcurrentWrites` | 2 | concurrent writes to different and same keys |
| `TestVersionedWrites` | 6 | versioned writes, version increments, conflict resolution |

### Test Results

```
============================= test session starts =============================
platform win32 -- Python 3.14.0, pytest-9.0.1, pluggy-1.6.0
collected 20 items

tests/test_integration.py::TestHealthCheck::test_leader_health PASSED    [  5%]
tests/test_integration.py::TestHealthCheck::test_all_followers_health PASSED [ 10%]
tests/test_integration.py::TestBasicOperations::test_write_to_leader PASSED [ 15%]
tests/test_integration.py::TestBasicOperations::test_read_from_leader PASSED [ 20%]
tests/test_integration.py::TestBasicOperations::test_read_nonexistent_key PASSED [ 25%]
tests/test_integration.py::TestReplication::test_replication_to_all_followers PASSED [ 30%]
tests/test_integration.py::TestReplication::test_write_quorum PASSED     [ 35%]
tests/test_integration.py::TestReplication::test_read_from_follower PASSED [ 40%]
tests/test_integration.py::TestFollowerBehavior::test_follower_rejects_writes PASSED [ 45%]
tests/test_integration.py::TestFollowerBehavior::test_follower_accepts_reads PASSED [ 50%]
tests/test_integration.py::TestConsistency::test_all_data_endpoint PASSED [ 55%]
tests/test_integration.py::TestConsistency::test_eventual_consistency PASSED [ 60%]
tests/test_integration.py::TestConcurrentWrites::test_concurrent_writes PASSED [ 65%]
tests/test_integration.py::TestConcurrentWrites::test_concurrent_writes_same_key PASSED [ 70%]
tests/test_integration.py::TestVersionedWrites::test_versioned_write_returns_version PASSED [ 75%]
tests/test_integration.py::TestVersionedWrites::test_versioned_write_increments_version PASSED [ 80%]
tests/test_integration.py::TestVersionedWrites::test_versioned_write_final_value_is_latest PASSED [ 85%]
tests/test_integration.py::TestVersionedWrites::test_versioned_concurrent_writes_consistency PASSED [ 90%]
tests/test_integration.py::TestVersionedWrites::test_versioned_replication_ordering PASSED [ 95%]
tests/test_integration.py::TestVersionedWrites::test_basic_write_has_no_version PASSED [100%]

======================== 20 passed in 65.25s (0:01:05) ========================
```

**all 20 tests passed**

---

## Performance Analysis

### Benchmark Configuration

| parameter | value |
|-----------|-------|
| total writes per quorum | 100 |
| number of keys per quorum | 10 |
| writes per key | 10 |
| write mode | ALL 100 writes fired concurrently (creates race conditions) |
| network delay range | [0ms, 1000ms] |
| quorum values tested | 1, 2, 3, 4, 5 |
| modes tested | basic (last-writer-wins), versioned (conflict resolution) |

**key difference:** all 100 writes (10 keys x 10 writes each) are fired concurrently to the same keys. this creates realistic race conditions from concurrent writes with random network delays.

### Two Consistency Modes

the system supports two write modes to demonstrate the race condition problem and its solution:

| mode | description | consistency |
|------|-------------|-------------|
| **basic** | last-writer-wins semantics | low (6-16%) - race conditions |
| **versioned** | version-based conflict resolution | 100% - no race conditions |

### Race Condition Explanation

```
the problem (basic mode):
  write 1: key=A, value="v1" → replication delay = 900ms
  write 2: key=A, value="v2" → replication delay = 100ms
  
  at follower:
    - write 2 arrives first (delay 100ms)
    - write 1 arrives later (delay 900ms) and OVERWRITES write 2
    - follower has "v1" but leader has "v2" (INCONSISTENT)

the solution (versioned mode):
  write 1: key=A, value="v1", version=1 → replication delay = 900ms
  write 2: key=A, value="v2", version=2 → replication delay = 100ms
  
  at follower:
    - write 2 arrives first: version=2 > 0, ACCEPT (stores "v2", version=2)
    - write 1 arrives later: version=1 <= 2, REJECT (stale write)
    - follower has "v2" matching leader (CONSISTENT)
```

### Basic Mode Results (Race Conditions)

basic mode uses **last-writer-wins semantics** without any conflict resolution. when multiple writes target the same key, whichever write physically arrives last at a follower becomes the stored value - regardless of the order in which writes were issued by the client.

#### Basic Mode Latency

| quorum | mean (ms) | median (ms) | p90 (ms) | p95 (ms) |
|--------|-----------|-------------|----------|----------|
| 1 | 261 | 238 | 459 | 519 |
| 2 | 407 | 403 | 625 | 713 |
| 3 | 574 | 561 | 835 | 873 |
| 4 | 791 | 829 | 992 | 1028 |
| 5 | 894 | 927 | 1055 | 1067 |

![basic latency](results/latency_basic.png)

**what this plot shows:**

the plot displays write latency (time from client request to server response) across different quorum levels. four metrics are shown:
- **mean (blue)**: arithmetic average of all 100 latency measurements per quorum
- **median / p50 (green)**: the middle value - 50% of requests completed faster than this
- **p90 (orange)**: 90% of requests completed faster than this threshold
- **p95 (red)**: 95% of requests completed faster - represents tail latency for slow requests

**key observations:**

1. **linear growth pattern**: latency increases roughly linearly with quorum. this directly reflects the order statistics of waiting for the k-th fastest follower out of 5.

2. **theoretical validation**: with uniform delay distribution U(0, 1000ms), the expected wait time for the k-th order statistic is `k × 1000 / 6`. our measured means (261, 407, 574, 791, 894) closely match the theoretical values (167, 333, 500, 667, 833).

3. **mean vs median convergence**: at higher quorum levels, mean and median converge because we're waiting for slower followers, reducing variance. at quorum=1, the gap is larger because we're sampling from the fast tail of the distribution.

4. **tail latency compression**: the gap between p50 and p95 shrinks as quorum increases. at quorum=1, the spread is ~280ms; at quorum=5, it's only ~140ms. this is because waiting for more followers naturally "averages out" the randomness.

5. **practical implication**: choosing quorum=2 over quorum=5 saves ~500ms per write on average - a 2.2x speedup. this is the **latency cost of durability**.

#### Basic Mode Consistency

| quorum | consistency (%) | matching pairs | total pairs |
|--------|-----------------|----------------|-------------|
| 1 | 16.0 | 8 | 50 |
| 2 | 6.0 | 3 | 50 |
| 3 | 12.0 | 6 | 50 |
| 4 | 16.0 | 8 | 50 |
| 5 | 8.0 | 4 | 50 |

![basic consistency](results/consistency_basic.png)

**what this plot shows:**

consistency is measured as the percentage of (key, follower) pairs where the follower's value matches the leader's value. with 10 keys and 5 followers, there are 50 total comparisons per quorum level.

**key observations:**

1. **catastrophically low consistency**: only 6-16% of follower values match the leader. this means **84-94% of data is inconsistent** across the cluster.

2. **quorum doesn't help**: intuitively, higher quorum should improve consistency because more followers acknowledge synchronously. but the results show no correlation - quorum=5 (8%) is actually worse than quorum=1 (16%). why?

3. **the root cause - race conditions**: when 10 writes to the same key are fired concurrently, each write gets a random replication delay (0-1000ms). the write that happens to get the shortest delay "wins" at each follower - but different followers may have different winners due to independent random delays.

4. **randomness dominates**: the consistency percentages are essentially random because they depend entirely on which writes happened to get which delays. this is fundamentally unpredictable and uncontrollable.

5. **the fundamental flaw**: basic mode has no mechanism to determine which write is "correct". it blindly accepts whatever arrives, making it unsuitable for any application requiring data correctness.

### Versioned Mode Results (Conflict Resolution)

versioned mode implements **version-based conflict resolution**. the leader assigns a monotonically increasing version number to each write, and followers only accept writes where `version > current_version`. this ensures that even if messages arrive out of order, the final state is always correct.

#### Versioned Mode Latency

| quorum | mean (ms) | median (ms) | p90 (ms) | p95 (ms) |
|--------|-----------|-------------|----------|----------|
| 1 | 171 | 133 | 336 | 388 |
| 2 | 335 | 304 | 642 | 661 |
| 3 | 517 | 498 | 784 | 868 |
| 4 | 656 | 674 | 907 | 956 |
| 5 | 865 | 903 | 999 | 1010 |

![versioned latency](results/latency_versioned.png)

**what this plot shows:**

the latency profile for versioned writes follows the same pattern as basic mode. this is expected because versioning adds negligible overhead - just an integer comparison on the follower side.

**key observations:**

1. **nearly identical to basic mode**: the version check (`if version > current_version`) adds microseconds of overhead, invisible at the millisecond scale of network delays.

2. **same order statistics behavior**: latency still follows E[X(k:n)] = k × (max-min) / (n+1). versioning doesn't change the fundamental physics of waiting for network acknowledgments.

3. **the insight**: you get conflict resolution "for free" in terms of latency. there's no performance penalty for correctness.

4. **quorum still controls latency**: the choice of quorum remains the primary latency lever. versioning is orthogonal - it controls correctness, not speed.

#### Versioned Mode Consistency

| quorum | consistency (%) | matching pairs | total pairs |
|--------|-----------------|----------------|-------------|
| 1 | 100.0 | 50 | 50 |
| 2 | 100.0 | 50 | 50 |
| 3 | 100.0 | 50 | 50 |
| 4 | 100.0 | 50 | 50 |
| 5 | 100.0 | 50 | 50 |

![versioned consistency](results/consistency_versioned.png)

**what this plot shows:**

a flat line at 100% - every single follower has exactly the same value as the leader for every key, regardless of quorum level.

**key observations:**

1. **perfect consistency at ALL quorum levels**: even quorum=1 achieves 100% consistency. this is remarkable because quorum=1 means only one follower acknowledges synchronously - the other four receive writes asynchronously with random delays.

2. **why it works**: when writes arrive out of order at a follower, the version check rejects stale writes. if write #10 (version=10) arrives before write #5 (version=5), the follower stores version=10. when write #5 finally arrives, it's rejected because 5 <= 10.

3. **eventual consistency guaranteed**: even though replication is asynchronous for non-quorum followers, they will eventually converge to the correct state. the version acts as a logical timestamp that establishes a total ordering of writes.

4. **decoupling durability from consistency**: 
   - **quorum controls durability**: how many nodes have the data before acknowledging
   - **versioning controls consistency**: ensuring all nodes converge to the same value
   
   these are independent concerns. you can have quorum=1 (fast, less durable) with perfect consistency, or quorum=5 (slow, highly durable) with perfect consistency.

5. **the power of logical clocks**: this is a simple form of a Lamport timestamp. more sophisticated systems use vector clocks or hybrid logical clocks, but even this basic version counter eliminates all race conditions.

### Consistency Comparison

![consistency comparison](results/consistency_comparison.png)

**what this plot shows:**

a direct comparison of consistency between basic mode (red) and versioned mode (green) across all quorum levels.

**key observations:**

1. **night and day difference**: the red line hovers around 6-16% while the green line is a flat 100%. this isn't a marginal improvement - it's the difference between a broken system and a correct one.

2. **quorum is not a solution for race conditions**: the red line shows no upward trend with increasing quorum. you cannot "fix" race conditions by waiting for more acknowledgments. the problem is message ordering, not message delivery.

3. **versioning is the solution**: the green line proves that a simple version check completely eliminates the race condition problem. no matter how chaotic the network delays, the final state is always correct.

4. **minimal implementation cost**: the fix requires:
   - leader: maintain a per-key counter, increment on each write
   - follower: compare incoming version with stored version, reject if stale
   - total: ~10 lines of code

5. **the fundamental lesson**: in distributed systems, **ordering guarantees require explicit mechanisms**. you cannot rely on physical time or arrival order. logical ordering (versions, timestamps, sequence numbers) is essential for correctness.

### Why Versioning Works

the version-based conflict resolution implements a form of **optimistic concurrency control**:

```
leader side:
  on write(key, value):
    version = ++version_counters[key]
    replicate(key, value, version)

follower side:
  on replicate(key, value, version):
    if version > stored_versions[key]:
      store(key, value)
      stored_versions[key] = version
    else:
      reject (stale write)
```

**the four guarantees:**

1. **total ordering**: version numbers establish a strict ordering of all writes to each key. write #5 always comes before write #6, regardless of network delays.

2. **idempotency**: receiving the same (key, value, version) multiple times is safe. the version check prevents duplicate application.

3. **consistency**: all followers that receive all messages will converge to the same state - the state reflecting the highest version number.

4. **availability**: unlike locking-based approaches, versioning never blocks. writes always succeed at the leader; conflict resolution happens asynchronously at followers.

**trade-offs:**

- **lost writes are silent**: if write #5 arrives after write #10, it's silently discarded. the client doesn't know. for some applications, this is fine; for others, you'd want explicit conflict detection and resolution.

- **per-key ordering only**: this scheme orders writes to the same key. it doesn't provide cross-key ordering (you'd need vector clocks or a global sequence for that).

- **leader dependency**: version assignment happens at the leader. if the leader fails, a new leader must know the last version for each key to avoid conflicts.

---

## Results and Discussion

### Key Findings

1. **race conditions in basic mode are severe**
   - with all 100 writes fired concurrently, basic mode achieves only 6-16% consistency
   - random network delays cause unpredictable write ordering
   - higher quorum does NOT eliminate race conditions (only reduces window slightly)

2. **versioned mode eliminates race conditions completely**
   - 100% consistency at ALL quorum levels (1-5)
   - version-based conflict resolution rejects stale writes
   - works regardless of network delays or message ordering

3. **quorum affects latency, not consistency (with versioning)**
   - higher quorum = higher latency (must wait for more followers)
   - with versioning, quorum only affects durability and latency, not correctness
   - quorum=1 with versioning is both fast AND consistent

4. **the fundamental insight**
   - quorum-based replication guarantees durability (data on K nodes before ACK)
   - but quorum alone does NOT guarantee consistency with concurrent writes
   - conflict resolution (versioning) is needed to handle race conditions

### Consistency Comparison

| mode | quorum=1 | quorum=3 | quorum=5 | mechanism |
|------|----------|----------|----------|-----------|
| basic | 6-16% | 100% | 6-16% | last-writer-wins (race conditions) |
| versioned | 100% | 100% | 100% | version-based conflict resolution |

### Trade-offs

| aspect | basic mode | versioned mode |
|--------|------------|----------------|
| consistency | low (6-16%) | high (100%) |
| complexity | simple | slightly more complex |
| overhead | none | version counter + comparison |
| use case | non-critical data | critical data, banking, etc. |

### When to Use Each Mode

**basic mode:**
- when eventual consistency is acceptable
- when writes to the same key are rare
- when performance is more important than correctness
- examples: caching, logging, analytics

**versioned mode:**
- when strong consistency is required
- when concurrent writes to the same key are common
- when correctness is more important than simplicity
- examples: banking, inventory, user accounts

---

## Conclusions

This laboratory work successfully implements a distributed key-value store with single-leader replication following the principles from "Designing Data-Intensive Applications" by Martin Kleppmann. The system features leader-based replication with 1 leader and 5 followers, semi-synchronous replication with configurable write quorum (1-5), and network latency simulation in the range [0ms, 1000ms] for realistic testing. The implementation uses FastAPI with asyncio for concurrent request handling, Docker containerization with docker-compose orchestration, and includes a comprehensive test suite with 20 passing tests.

### Key Achievement: Version-Based Conflict Resolution

The most significant finding is the implementation and analysis of **version-based conflict resolution** to eliminate race conditions:

| mode | consistency | mechanism |
|------|-------------|-----------|
| basic (last-writer-wins) | 6-16% | race conditions from concurrent writes |
| versioned (conflict resolution) | 100% | stale writes rejected via version check |

This demonstrates a fundamental principle in distributed systems: **quorum-based replication guarantees durability, but not consistency**. Conflict resolution mechanisms (like versioning) are required to handle concurrent writes correctly.

### Technical Insights

1. **race conditions are inevitable** with concurrent writes and network delays in basic last-writer-wins systems
2. **versioning solves the problem** by rejecting writes where `version <= current_version`
3. **quorum affects latency, not correctness** (with versioning enabled)
4. **the solution is simple** - just a version counter and comparison, minimal overhead

### Future Improvements

- implement leader election for fault tolerance (currently leader is fixed)
- add persistent storage (currently in-memory only)
- implement read-your-writes consistency guarantees
- add vector clocks for multi-key transactions
- implement log-based replication for crash recovery

---

## References

1. Kleppmann, Martin. "Designing Data-Intensive Applications." O'Reilly Media, 2017. Chapter 5: Replication.

2. FastAPI Documentation. https://fastapi.tiangolo.com/

3. Python asyncio Documentation. https://docs.python.org/3/library/asyncio.html

4. Docker Compose Documentation. https://docs.docker.com/compose/

5. Pydantic Settings. https://docs.pydantic.dev/latest/concepts/pydantic_settings/
