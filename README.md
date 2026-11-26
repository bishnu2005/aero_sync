# AeroSync

Real-time, event-driven flight tracking for Indian airspace. Live state vectors from the
[OpenSky Network](https://opensky-network.org) REST API (or a clearly labelled synthetic fallback) stream through
Kafka into a Valkey cache, a PostgreSQL archive and a WebSocket feed rendered on a Leaflet map.

**Live demo:** _frontend URL (Vercel) goes here_ · **API health:** _backend URL_/api/health
· **Engineering write-up:** [ENGINEERING_DEEP_DIVE.md](ENGINEERING_DEEP_DIVE.md)

```
 OpenSky REST ──► TelemetryIngestionService ─┐               ┌─► Valkey  (active_flights hash)
                  (quota-gated, 60 s poll)   ├─► Kafka ──► TelemetryConsumer ─┼─► PostgreSQL (throttled history)
 FlightSimulator ────────────────────────────┘  telemetry.flights.live       └─► TelemetryStream ─► /ws/telemetry ─► React
 (when in SIMULATION mode, 3 s tick)
                                                  REST: /api/flights/active · /api/flights/{id} · /api/system/{status,metrics} · /api/health
                                                  Actuator: /actuator/health · /actuator/metrics · /actuator/prometheus
```

| Layer    | Stack |
|----------|-------|
| Backend  | Java 21, Spring Boot 4.1.1 (WebFlux / Netty), Spring Kafka, R2DBC PostgreSQL, Reactive Redis (Lettuce → Valkey) |
| Observability | Spring Boot Actuator, Micrometer (custom counters/gauges), Prometheus exposition format, optional ECS JSON logs |
| Frontend | React 19, Vite 8, Leaflet + react-leaflet, native WebSockets |
| Infra    | Kafka (KRaft), PostgreSQL 16, Valkey 8, Nginx, Docker Compose |
| Tests    | JUnit 5, Mockito, Reactor StepVerifier, WebTestClient, embedded Kafka; Vitest + Testing Library |

## Features

### Flight ID / Callsign lookup
Search by callsign (`AIC860`) or ICAO24 transponder address (`800a01`), case-insensitive.

* The sidebar filters the live feed while you type.
* Pressing Enter selects an exact live match right away. Otherwise the UI calls
  `GET /api/flights/{identifier}`, which checks the **Valkey cache** first (`status: ACTIVE`,
  `source: VALKEY_CACHE`) and then falls back to the **PostgreSQL archive** (`status: HISTORICAL`,
  `source: POSTGRES_ARCHIVE`). If Valkey is down, the lookup goes straight to PostgreSQL.
* Identifiers are trimmed and must match `^[A-Za-z0-9_-]{1,32}$`. Anything else gets a structured `400`.
* The detail card shows altitude, speed (m/s and knots), heading with cardinal direction, position and
  last contact. For active flights it keeps updating from the WebSocket while it is open.

### Automatic OpenSky quota failover
`TelemetryModeManager` holds the daily OpenSky budget and switches between `LIVE` and `SIMULATION`:

| Trigger | Reason | Back to LIVE |
|---|---|---|
| Daily budget used up (`OPENSKY_DAILY_LIMIT`, default **400**) | `DAILY_QUOTA_EXHAUSTED` | at 00:00 UTC |
| OpenSky answers HTTP 429 | `UPSTREAM_RATE_LIMITED_429` | after the cool-down (15 min) or OpenSky's retry-after hint, whichever is longer |
| `OPENSKY_CONSECUTIVE_ERROR_THRESHOLD` (3) failures in a row (timeouts, 4xx/5xx, bad JSON) | `UPSTREAM_UNAVAILABLE` | after the cool-down (15 min) |
| `OPENSKY_ENABLED=false` | `MANUAL_SIMULATION_CONFIG` | never |

The app uses the full configured quota; it does not throttle itself to a fraction. Every poll reserves
budget atomically before calling OpenSky, so the limit holds even under concurrent callers. In
simulation mode eight aircraft fly real Indian corridors at realistic altitude and speed, and their
positions go through the same Kafka pipeline as live data.

**The UI never passes synthetic data off as real.** Every event carries `origin` (`OPENSKY` or `SIMULATOR`), and both the
API and the UI only show aircraft from the feed named on the badge:

| Badge | Meaning |
|---|---|
| 🟢 `LIVE FEED` | Real OpenSky data, plus `used/limit` polls today |
| 🟠 `SIMULATED FALLBACK` | Switched automatically, with the reason (quota reached, HTTP 429, API unreachable) |
| 🟠 `SIMULATED FEED` | Simulator chosen by configuration (`OPENSKY_ENABLED=false`) |

### Observability
* **Actuator:** `/actuator/health` (component status for R2DBC and Redis, without connection details),
  `/actuator/health/liveness`, `/actuator/health/readiness`, `/actuator/info`, `/actuator/metrics`, `/actuator/prometheus`.
* **Custom Micrometer meters**
  * Counters:
    * `aerosync.telemetry.published{source}`
    * `aerosync.telemetry.consumed`
    * `aerosync.telemetry.dropped{reason}`
    * `aerosync.kafka.publish{result}`
    * `aerosync.cache.writes{result}`
    * `aerosync.cache.lookups{result=hit|miss|error}`
    * `aerosync.db.writes{result=success|failure|throttled}`
    * `aerosync.opensky.requests{outcome}`
  * Gauges:
    * `aerosync.websocket.subscribers`
    * `aerosync.opensky.quota.used` and `aerosync.opensky.quota.limit`
    * `aerosync.mode.simulation`
* **Dashboard health bar:** fed by `GET /api/system/metrics`. It shows API up/down, events/s (60 s average), events
  processed, cache hit ratio, WebSocket clients, upstream errors and uptime.
* **Structured logs:** set `LOGGING_STRUCTURED_FORMAT_CONSOLE=ecs` for JSON (ECS) console logs.
* **Not included:** no Prometheus server or Grafana is bundled. Point any Prometheus-compatible scraper at `/actuator/prometheus`.

### Built for free tiers
* PostgreSQL writes are throttled to one per aircraft every `DB_PERSIST_INTERVAL_SECONDS` (15 s). Valkey and
  the WebSocket still update on every message.
* History older than `DB_RETENTION_HOURS` (24 h) is pruned hourly.
* Valkey entries older than `ACTIVE_FLIGHT_STALE_SECONDS` (300 s) are hidden and evicted every minute.
  Corrupted entries are skipped and then evicted.
* `schema.sql` is idempotent and runs on every boot, so a fresh database sets itself up.
* The JVM is tuned to run in 512 MB (`MaxRAMPercentage=70`, SerialGC).

### Fault isolation
* The Kafka consumer never throws. A Valkey failure doesn't block PostgreSQL or the WebSocket, and the
  reverse holds too. Malformed messages and blank callsigns are dropped.
* `/api/flights/active` falls back to recent PostgreSQL positions if Valkey is down. `/api/system/status`
  reports `activeFlightCount: 0` instead of failing.
* Every error comes back as `{status, error, message, path, timestamp}`, including 404 for unknown
  routes and 405 for wrong methods. Stack traces, SQL and Redis details only go to the server log.
* CORS is configured in one place (`CORS_ALLOWED_ORIGINS`, patterns like `https://*.vercel.app` work).
  The same policy also checks the WebSocket handshake's `Origin`.
* The map defaults to Esri's keyless Dark/Light Gray Canvas tiles. CARTO Dark Matter and Positron now
  require an API key (without one they serve "API KEY REQUIRED" watermark tiles). To use CARTO, Stadia or
  another keyed provider, set `VITE_MAP_TILE_DARK` / `VITE_MAP_TILE_LIGHT` at build time.
* The UI reconnects the WebSocket with exponential back-off (1 s up to 30 s). It shows a
  "Waking up AeroSync server..." banner on slow cold starts and "Reconnecting to telemetry stream..." on drops.

## API

| Method | Path | Description |
|---|---|---|
| GET | `/api/health` | Liveness: `{"status":"UP","service":"aerosync-backend","timestamp":...}` (touches no dependency) |
| GET | `/api/system/status` | `mode`, `reason`, `feed`, `message`, `requestsUsedToday`, `dailyLimit`, `windowResetAt`, `activeFlightCount` |
| GET | `/api/system/metrics` | Pipeline snapshot: `eventsPerSecond`, `eventsConsumed`, `cacheHitRatio`, `websocketSubscribers`, `upstreamErrors`, DB write and throttle counts, `uptimeSeconds` |
| GET | `/actuator/health` (`/liveness`, `/readiness`) | Spring Boot health, including R2DBC and Redis |
| GET | `/actuator/metrics`, `/actuator/prometheus` | All Micrometer meters (JVM, HTTP, Kafka client and the `aerosync.*` meters) |
| GET | `/api/flights/active` | Active, non-stale flights sorted by callsign |
| GET | `/api/flights/{identifier}` | Callsign / ICAO24 lookup (`200`, `400 INVALID_IDENTIFIER`, `404 FLIGHT_NOT_FOUND`, `503 SERVICE_UNAVAILABLE`) |
| WS  | `/ws/telemetry` | One JSON `FlightTelemetry` frame per position update |

`FlightTelemetry`: `callsign`, `icao24`, `latitude`, `longitude`, `altitude` (ft), `velocity` (m/s),
`heading` (degrees true), `timestamp` (epoch ms), `origin` (`OPENSKY` or `SIMULATOR`).

## Local quickstart (Topology A: Docker)

**Prerequisites:** [Docker Desktop](https://www.docker.com/products/docker-desktop) (running) and Git. You don't
need Java, Node, Kafka or PostgreSQL installed locally.

```bash
git clone https://github.com/bishnu2005/aero_sync.git && cd aero_sync
```

```bash
cp .env.example .env    # optional, every value has a default
```

```bash
docker compose up --build -d
```

Open http://localhost:5173. The backend is on http://localhost:8080. Compose waits for Kafka, PostgreSQL
and Valkey health checks before starting the backend.

To run without calling OpenSky at all (simulator only):

```bash
OPENSKY_ENABLED=false docker compose up --build -d
```

### Running pieces outside Docker

```bash
docker compose up -d kafka postgres valkey
```

```bash
cd backend && ./mvnw spring-boot:run
```

```bash
cd frontend && npm install && npm run dev
```

## Tests

156 backend tests and 37 frontend tests. None of them call `opensky-network.org` or need Docker. The OpenSky client is tested against a
stubbed `ExchangeFunction`, and the pipeline test uses an in-memory Kafka broker.

```bash
cd backend && ./mvnw test
```

```bash
cd frontend && npm test
```

```bash
cd frontend && npm run build
```

| Suite | Covers |
|---|---|
| `AviationApiClientTest` | Parsing, null/empty/truncated state vectors, 429 + retry-after, 4xx/5xx, malformed JSON, timeouts, network errors, Basic Auth |
| `TelemetryModeManagerTest` | 400 default, exact quota exhaustion, 32-thread race test, UTC-day reset with an injected clock, 429 / error-streak failover, cool-down recovery, manual mode |
| `TelemetryIngestionServiceTest` | No API call in simulation mode, 429 handling, success path, Kafka failures not counted as upstream errors, reactive `@Scheduled` |
| `TelemetryProducerTest` | Kafka send runs on `boundedElastic` (never on the Netty event loop), keying, failures counted |
| `SystemMetricsServiceTest` | 60 s throughput window with bursty live data, cache hit ratio, feed label, gauge names |
| `TelemetryConsumerTest` | Blank/null callsigns, trimming, per-callsign PostgreSQL throttle, Valkey/PostgreSQL/WebSocket fault isolation, malformed JSON |
| `FlightControllerApiContractTest` | Every endpoint and error path: lookup fallbacks, validation and injection strings, 404/405/503/500, CORS allow/deny |
| `TelemetryStreamAndWebSocketTest` | Multicast, cancellation, surviving zero subscribers, 10k-message bursts, slow consumers, WebSocket handler lifecycle |
| `KafkaPipelineIntegrationTest` | Full context: producer → Kafka → consumer → Valkey/PostgreSQL → real WebSocket client; Actuator health, probes and Prometheus; WebSocket Origin rejection |
| `FlightSimulatorTest`, `DataRetentionServiceTest`, `KafkaSecuritySupportTest` | Corridors and realism, pruning and eviction, PEM/SASL mapping |
| `frontend/src/test/*` | Rendering, feed badges, live/simulated separation, health bar, live filtering, search (live hit, archive hit, 404, 400, client validation), WebSocket updates in the open card, marker click, sidebar collapse, theme toggle, zoom, cold-start and reconnect banners, URL resolution |



### OpenSky credentials
Anonymous access gives 400 requests a day, which is the default. `OPENSKY_USERNAME` / `OPENSKY_PASSWORD`
are sent as HTTP Basic Auth when set, and you can raise `OPENSKY_DAILY_LIMIT` up to 4000. OpenSky has
been moving accounts to OAuth2 client credentials. AeroSync does not implement that flow yet, so leave
the credentials blank if your account only supports OAuth2.

## Known limitations
* **Quota counter resets on restart:** it lives in memory, and Render's free tier restarts often.
* **Single instance:** a second replica would poll OpenSky twice.
* **Kafka is thread-based:** Spring Kafka, not Reactor Kafka. The blocking send runs on `boundedElastic`, and the consumer's
  Valkey and PostgreSQL writes are fire-and-forget.
* **Data limits:** live data is a 60 s poll of one public API (India bounding box, up to 150 aircraft per poll). No ADS-B receivers are involved.
* **Synthetic fallback:** it generates flights; it does not replay recorded data.

Details and next steps: [ENGINEERING_DEEP_DIVE.md](ENGINEERING_DEEP_DIVE.md) §0 and §5.

## Configuration reference

All settings are environment variables; see [`.env.example`](.env.example) and
[`backend/src/main/resources/application.properties`](backend/src/main/resources/application.properties)
for the full list and defaults. Frontend build variables are documented in
[`frontend/.env.example`](frontend/.env.example).
