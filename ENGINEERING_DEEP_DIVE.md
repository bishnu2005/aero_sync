# AeroSync — Engineering Deep Dive

This document describes the code as it is, not as a pitch. Every claim points at a file (and usually a line) in
this repository. Where the system is simpler than a production tracker, it says so.

**At a glance**

| Concern | What is actually implemented |
|---|---|
| Upstream data | **OpenSky Network REST API** (`GET /api/states/all`, India bounding box), polled every 60 s |
| Fallback | A **synthetic flight simulator** (8 aircraft on real Indian corridors). It generates data rather than replaying recordings, and is always labelled as simulated in the UI and API. |
| Messaging | **Apache Kafka** through **Spring Kafka** (`KafkaTemplate` + `@KafkaListener`), topic `telemetry.flights.live`. Not Reactor Kafka. |
| Hot state | **Valkey 8** hash `active_flights`, read and written with Spring Data Redis Reactive (Lettuce) |
| Archive | **PostgreSQL 16** via **R2DBC** (reactive driver), table `flight_telemetry_history` |
| Push to browser | **WebSocket** (`/ws/telemetry`, Spring WebFlux `WebSocketHandler`). Not SSE. |
| HTTP stack | **Spring Boot 4.1.1 WebFlux on Netty**, Java 21 |
| Frontend | React 19 + Vite 8 + Leaflet / react-leaflet, native `WebSocket` |
| Observability | **Spring Boot Actuator** (`/actuator/health` with liveness/readiness, `/actuator/metrics`, `/actuator/prometheus`), **Micrometer** custom counters and gauges, a dashboard health bar fed by `/api/system/metrics`, optional ECS JSON logs. There is no Prometheus server or Grafana in the repo; the endpoint is ready to be scraped. |
| Tests | 156 backend tests (JUnit 5, Mockito, Reactor `StepVerifier`, `WebTestClient`, embedded Kafka) and 37 frontend tests (Vitest + Testing Library). Zero calls to the real OpenSky API. |

---

## 0. Audit findings (and what was done about them)

| # | Finding | Status |
|---|---|---|
| 1 | `KafkaTemplate.send()` blocks the caller for up to `max.block.ms` when topic metadata is missing. In the live path the caller was the **Netty event loop** that delivered the OpenSky response. | **Fixed.** The send is moved to `Schedulers.boundedElastic()` (`TelemetryProducer.java:40`), and a test asserts the thread name (`TelemetryProducerTest`). |
| 2 | Four `@Scheduled` jobs used `.block()` (scheduler threads, not Netty). | **Fixed.** Spring 6.1+ reactive `@Scheduled`: the jobs return `Mono<Void>` and nothing parks a thread. |
| 3 | "Observability" was console logs plus `/api/health`. | **Fixed.** Actuator, Micrometer and the Prometheus registry are wired in, with 8 custom meters, 4 gauges, a summary API and the dashboard bar. |
| 4 | After a live/simulated switch, the other feed's aircraft stayed visible for up to 5 minutes under the new badge. | **Fixed.** Every event carries `origin` (`OPENSKY` or `SIMULATOR`), reads are filtered by the current feed on both server and client, and the leftovers are evicted. |
| 5 | Events/s read 0.0 between once-per-minute live bursts. | **Fixed.** 60 s sliding average, never divided by less than 60 s. |
| 6 | Unused Vite template files. | **Removed.** |
| 7 | Kafka uses thread-based Spring Kafka, not Reactor Kafka. The listener thread fires-and-forgets reactive Valkey/R2DBC writes. | **Documented, not changed** (see §4.2). The listener thread is never blocked, but store latency does not apply backpressure to Kafka consumption. |
| 8 | The OpenSky quota counter is in memory, so a restart resets it (Render's free tier sleeps and restarts). | **Known limitation.** Persisting it in Valkey (`INCR` plus `EXPIREAT` at midnight UTC) is the natural fix. |
| 9 | Single-instance design: the quota, the Postgres throttle map and the simulator are per JVM. | **Known limitation.** Two replicas would double OpenSky polling. That's fine for a demo; HA would need a shared lock or a leader-elected poller. |
| 10 | No authentication on the API; Actuator metrics are public. | **By design for a public demo.** Health details are hidden (`show-details=never`), and `env`, `beans` and `heapdump` are not exposed. |

---

## 1. End-to-end lifecycle of one telemetry event

The trace below follows one aircraft, `ABY800` (icao24 `896558`), through the live path.

```
 t=0      TelemetryIngestionService.ingestFlights()       reactive @Scheduled, fixed delay 60 s
            └─ TelemetryModeManager.tryAcquireLiveRequest()   quota check (synchronized), 1 of 400 used
 t+0      AviationApiClient.getLiveFlights()
            └─ WebClient GET https://opensky-network.org/api/states/all?lamin=8.0&lomin=68.0&lamax=37.0&lomax=97.0
               8 s connect/response timeout, Basic Auth only if configured
 t+~1s    HTTP 200 JSON {"time":..., "states":[["896558","ABY800  ","India",..., 80.71, 14.06, 10668.0, false, 244.0, 117.0, ...]]}
            └─ parseStates(): JsonNode walk, skip rows without callsign/lat/lon,
               metres -> feet, last_contact -> epoch ms, origin = "OPENSKY", cap 150 per poll
 t+~1s    collectList() -> recordUpstreamSuccess(), metric aerosync.opensky.requests{outcome=success}
            └─ concatMap(TelemetryProducer::publish)            one event after another, in order
                 └─ serialise to JSON, KafkaTemplate.send(topic, key=callsign, json)  on boundedElastic
 Kafka    topic telemetry.flights.live, partition chosen by hash(key "ABY800")
 t+~1.1s  TelemetryConsumer.consume(String json)                 @KafkaListener thread, group aerosync-group
            ├─ parse; drop if malformed or blank callsign        metric aerosync.telemetry.dropped{reason}
            ├─ trim callsign, re-serialise
            ├─ Valkey: HSET active_flights ABY800 <json>         reactive, fire-and-forget, isolated
            ├─ PostgreSQL: INSERT flight_telemetry_history       only if >= 15 s since the last write for ABY800
            └─ TelemetryStream.broadcast(json)                   multicast sink
 t+~1.1s  TelemetryWebSocketHandler -> session.send(...)          every open /ws/telemetry connection
 browser  useTelemetrySocket.onmessage -> handleTelemetry()       batched into one React update per 100 ms
            └─ matchesFeed(flight, "LIVE_FEED") -> Sidebar card + Leaflet marker (rotated SVG, heading 117 deg)
```

Side paths for the same event:

* **Cold load / reload:** the browser calls `GET /api/flights/active`. `ActiveFlightService` runs `HVALS active_flights`,
  drops stale entries (over 300 s old), corrupted entries and entries from the other feed, sorts by callsign and returns JSON.
* **Lookup:** `GET /api/flights/ABY800` checks Valkey first (`ACTIVE` / `VALKEY_CACHE`). If Valkey misses or errors, it
  falls back to the newest Postgres row (`HISTORICAL` / `POSTGRES_ARCHIVE`). If both miss it returns `404`, and if the archive
  is unreachable `503`.
* **Simulated path:** when `TelemetryModeManager` is in `SIMULATION`, `FlightSimulator.tick()` (every 3 s) produces 8 events
  with `origin = "SIMULATOR"` into the **same** Kafka topic. Everything downstream is identical.

---

## 2. Directory and file atlas

### Root
| File | Purpose |
|---|---|
| `README.md` | Overview, quickstart, API and observability reference, deployment summary |
| `ENGINEERING_DEEP_DIVE.md` | This document |
| `docker-compose.yml` | Local stack: Kafka (KRaft), PostgreSQL, Valkey, backend, frontend (Nginx), with health-gated startup and a 512 MB backend memory cap |
| `render.yaml` | Render Blueprint for the backend (free plan, Docker, `/api/health` health check, secrets prompted) |
| `.env.example` | Every environment variable for Docker Compose and cloud, with defaults |
| `.gitignore` | Ignores `.env`, IDE folders, OS files |

### `backend/`
| Path | Purpose |
|---|---|
| `pom.xml` | Spring Boot 4.1.1 parent: WebFlux, Spring Kafka, R2DBC + r2dbc-postgresql, Reactive Redis, Actuator, Micrometer Prometheus registry; `-Xlint:unchecked,deprecation` |
| `Dockerfile` | Two stages: JDK build with `dependency:go-offline`, then a non-root JRE runtime with `MaxRAMPercentage=70` and SerialGC for 512 MB hosts |
| `.dockerignore` | Keeps `target/` and IDE files out of the build context |
| `.env.example` | Backend-only variables for running the jar or configuring Render/Koyeb |
| `mvnw`, `mvnw.cmd`, `.mvn/wrapper/maven-wrapper.properties` | Maven wrapper (Maven 3.9.16) so builds need no local Maven |
| `.gitattributes`, `.gitignore` | Line endings for the wrapper; ignore build output |
| `src/main/resources/application.properties` | All configuration, each value overridable by an env var (datasources, Kafka TLS/SASL, OpenSky, simulator, retention, CORS, Actuator) |
| `src/main/resources/schema.sql` | Idempotent schema with `CREATE TABLE IF NOT EXISTS` and `ADD COLUMN IF NOT EXISTS`, plus 4 indexes; runs on every boot |

`src/main/java/com/aerosync/`
| File | Purpose |
|---|---|
| `BackendApplication.java` | Spring Boot entry point; `@EnableScheduling` |
| **config/** | |
| `CorsConfig.java` | `WebFluxConfigurer` that turns `CORS_ALLOWED_ORIGINS` (exact origins or patterns) into one CORS policy for `/api/**`, and exposes the same policy for the WebSocket |
| `KafkaConfig.java` | Producer, consumer, admin and listener factories built from env vars. `NewTopic` for `telemetry.flights.live`, a fail-fast producer (`max.block.ms=10s`), and a no-retry error handler |
| `KafkaSecuritySupport.java` | Maps `SSL`/`SASL_SSL` settings and base64 PEM env vars to Kafka's native PEM properties (no keystore files) |
| `TelemetryWebSocketHandler.java` | Streams `TelemetryStream` frames to a socket and ends when the client closes or the write fails |
| `TimeConfig.java` | A single `Clock` bean, so tests can move time deterministically |
| `WebClientConfig.java` | OpenSky `WebClient` on Reactor Netty with 8 s connect and response timeouts and a 4 MB buffer |
| `WebSocketConfig.java` | Maps `/ws/telemetry` and applies the CORS policy to the handshake `Origin` |
| **controller/** | |
| `FlightController.java` | `GET /api/flights/active` and `GET /api/flights/{identifier}` (trims, then validates `^[A-Za-z0-9_-]{1,32}$`) |
| `SystemController.java` | `GET /api/health` (touches no dependency), `/api/system/status`, `/api/system/metrics` |
| **dto/** | |
| `ApiErrorResponse.java` | Uniform error body `{status, error, message, path, timestamp}` |
| `FlightDetailResponse.java` | Lookup result: telemetry plus knots, `status` (ACTIVE/HISTORICAL), `source` (store), `origin` (data source) |
| `HealthResponse.java` | `{status, service, timestamp}` |
| `SystemMetrics.java` | Dashboard metrics snapshot (rate, counts, hit ratio, subscribers, uptime) |
| `SystemStatus.java` | Mode, reason, message, quota, reset time, active count, `feed` label |
| **exception/** | |
| `GlobalExceptionHandler.java` | `@RestControllerAdvice` mapping every error, including unknown routes and wrong methods, to `ApiErrorResponse`; never returns stack traces |
| `FlightNotFoundException.java`, `InvalidIdentifierException.java`, `ServiceUnavailableException.java` | 404 / 400 / 503 cases |
| `OpenSkyRateLimitException.java`, `OpenSkyUpstreamException.java` | Typed upstream failures that drive the failover state machine |
| **messaging/** | |
| `FlightSimulator.java` | Synthetic fallback: 8 corridors, bearing and haversine math, ping-pong motion, reactive tick |
| `TelemetryConsumer.java` | `@KafkaListener` that fans out to Valkey, a throttled PostgreSQL write and the WebSocket stream, each failure-isolated and instrumented |
| `TelemetryProducer.java` | Serialises and sends to Kafka on `boundedElastic`; failures are counted and swallowed |
| `TelemetryStream.java` | Hot multicast sink with a bounded, drop-oldest buffer per subscriber |
| **model/** | |
| `FlightTelemetry.java` | The event record (callsign, icao24, lat, lon, altitude ft, velocity m/s, heading, timestamp, origin) |
| `FlightRecord.java` | R2DBC entity for `flight_telemetry_history` |
| **observability/** | |
| `TelemetryMetrics.java` | Micrometer counters for every pipeline stage |
| `ObservabilityConfig.java` | `MeterBinder` gauges: WebSocket subscribers, quota used/limit, simulation flag |
| `SystemMetricsService.java` | Builds the `/api/system/metrics` snapshot; samples throughput every 5 s and averages over 60 s |
| **repository/** | |
| `FlightRecordRepository.java` | Reactive repository: latest row by callsign/icao24, latest-per-callsign since a time, delete older than |
| **service/** | |
| `ActiveFlightService.java` | Valkey reads with staleness and feed filtering, Postgres fallback for the active list, stale/leftover eviction |
| `AviationApiClient.java` | OpenSky client: typed 429/upstream errors, defensive `JsonNode` parsing, unit conversion |
| `DataRetentionService.java` | Reactive jobs: prune Postgres rows older than 24 h (hourly), evict Valkey entries (every minute) |
| `FlightLookupService.java` | Cache-then-archive lookup with hit/miss/error metrics |
| `TelemetryIngestionService.java` | Reactive poll loop gated by quota; classifies failures; publishes to Kafka |
| `TelemetryModeManager.java` | LIVE/SIMULATION state machine, daily quota, failover reasons, cool-down recovery |

`src/test/java/com/aerosync/` (156 tests)
| File | Covers |
|---|---|
| `config/KafkaSecuritySupportTest.java` | PEM and SASL mapping, bad base64, topic definition |
| `controller/FlightControllerApiContractTest.java` | Every endpoint, validation and injection strings, 400/404/405/500/503, CORS allow/deny, feed filtering, metrics snapshot |
| `messaging/FlightSimulatorTest.java` | 8 distinct realistic aircraft, corridor bounds over 20,000 ticks, reactive tick, origin |
| `messaging/KafkaPipelineIntegrationTest.java` | Full context on a random port with embedded Kafka: producer to consumer to Valkey/Postgres (mocked) to a **real WebSocket client**; Actuator health, probes and Prometheus; WebSocket origin rejection |
| `messaging/TelemetryConsumerTest.java` | Blank/malformed input, trimming, throttle, fault isolation, metrics |
| `messaging/TelemetryProducerTest.java` | Lazy send, runs on `boundedElastic`, key and payload, failures counted |
| `messaging/TelemetryStreamAndWebSocketTest.java` | Multicast, cancellation, surviving zero subscribers, 10k bursts, slow subscriber, handler lifecycle |
| `observability/SystemMetricsServiceTest.java` | 60 s rate (including once-per-minute bursts), hit ratio, feed label, gauge names |
| `service/AviationApiClientTest.java` | Parsing edge cases, 429 with retry-after, 4xx/5xx, malformed JSON, timeout, network error, auth header (stubbed `ExchangeFunction`) |
| `service/DataRetentionServiceTest.java` | Pruning cutoff, eviction of stale, corrupted and other-feed entries |
| `service/TelemetryIngestionServiceTest.java` | No API call in simulation, 429/error handling, reactive scheduling, metrics |
| `service/TelemetryModeManagerTest.java` | Quota exhaustion, 32-thread race, UTC-day reset with a mutable clock, failover and recovery |
| `support/MutableClock.java`, `support/TestFlights.java` | Test clock and fixtures |

### `frontend/`
| Path | Purpose |
|---|---|
| `package.json` | Scripts `dev`, `build`, `lint`, `test` (`vitest run`); React, Leaflet, react-leaflet; Vitest and Testing Library |
| `vite.config.js` | React plugin and Vitest config (jsdom, setup file) |
| `eslint.config.js` | ESLint with the React hooks and refresh rules |
| `index.html` | SPA shell |
| `Dockerfile` | Node 22 build (Vite env vars as build args), then Nginx serving `dist/` |
| `nginx.conf` | SPA fallback, `/api/` and `/ws/` proxy to `backend:8080` with WebSocket upgrade, 1 h WS timeout |
| `vercel.json`, `public/_redirects` | SPA rewrites for Vercel / Netlify |
| `.env.example` | `VITE_API_BASE_URL`, `VITE_WS_URL`, map tile overrides |
| `.dockerignore`, `.gitignore` | Build hygiene |
| `public/favicon.svg` | Tab icon |
| `src/main.jsx` | React root |
| `src/App.jsx` | State owner: flight map, initial load, WebSocket batching, feed filtering, search, selection, theme |
| `src/index.css` | Glass-morphism design tokens (dark/light), layout, markers, health bar |
| `src/config/api.js` | API and WebSocket URL resolution for dev, Docker proxy and cloud |
| `src/hooks/useTelemetrySocket.js` | WebSocket with exponential back-off (1 s up to 30 s) |
| `src/hooks/useSystemStatus.js` | Polls `/api/system/status` every 30 s, with an immediate throttled `refresh()` |
| `src/hooks/useSystemMetrics.js` | Polls `/api/system/metrics` every 10 s |
| `src/components/Sidebar.jsx` | Search form, feed badge, tracked count, sorted telemetry feed |
| `src/components/StatusBadge.jsx` | `LIVE FEED` / `SIMULATED FALLBACK` / `SIMULATED FEED` with the reason |
| `src/components/SystemHealthBar.jsx` | API up/down, events/s (60 s avg), processed, cache hit, WS clients, upstream errors, uptime |
| `src/components/FlightDetailCard.jsx` | Altitude, speed (m/s and kts), heading and cardinal, position, last contact, store and origin |
| `src/components/MapController.jsx` | Imperative Leaflet bridge (`flyTo`, map handle) |
| `src/components/MapControls.jsx` | Theme toggle and zoom buttons |
| `src/components/ConnectionBanner.jsx` | "Waking up…" / "Reconnecting…" banner |
| `src/lib/flight.js` | Formatting, sorting, search matching, identifier validation |
| `src/lib/planeIcon.js` | Cached Leaflet `divIcon` with a rotated inline SVG |
| `src/lib/mapTheme.js` | Keyless Esri basemaps with env-var overrides |
| `src/lib/status.js` | `feedOf`, `matchesFeed`, `formatUptime` |
| `src/test/*` | 37 tests: rendering, badges, feed filtering, search paths, WebSocket updates, health bar, banners, URL logic |

---

## 3. Architecture and data modelling

### 3.1 Diagram

```
                ┌──────────────────────── Spring Boot 4 WebFlux (Netty) ─────────────────────────┐
 OpenSky REST ──┤ TelemetryIngestionService ──(quota: TelemetryModeManager)──┐                    │
  (60 s poll)   │   AviationApiClient (WebClient, 8 s timeouts, 429-aware)   │                    │
                │                                                            ▼                    │
                │ FlightSimulator (3 s, only in SIMULATION) ─────────► TelemetryProducer ─────────┼──► Kafka
                │                                                     (boundedElastic send)      │   telemetry.flights.live
                │                                                                                 │        │
                │ TelemetryConsumer (@KafkaListener) ◄────────────────────────────────────────────┼────────┘
                │   ├─► Valkey  HSET active_flights            (every event)                      │
                │   ├─► PostgreSQL INSERT history              (≤ 1 per aircraft per 15 s)        │
                │   └─► TelemetryStream (multicast) ──► /ws/telemetry ────────────────────────────┼──► React (WebSocket)
                │                                                                                 │
                │ REST: /api/flights/active · /api/flights/{id} · /api/system/{status,metrics}    │◄── React (fetch)
                │ Actuator: /actuator/health{,/liveness,/readiness} · /metrics · /prometheus      │◄── Render health / Prometheus
                └─────────────────────────────────────────────────────────────────────────────────┘
```

### 3.2 Kafka
* **Topic** `telemetry.flights.live`, declared by a `NewTopic` bean (`KafkaConfig`). It has **1 partition** and replication factor 1
  by default (`KAFKA_TOPIC_PARTITIONS` / `KAFKA_TOPIC_REPLICATION_FACTOR`), sized for free-tier brokers.
* **Key = callsign.** All updates for one aircraft hash to the same partition, so per-aircraft order is preserved
  even if partitions are increased.
* **Consumer group** `aerosync-group`, `auto.offset.reset=earliest`, Spring Kafka's default batch acknowledgement.
  `DefaultErrorHandler(FixedBackOff(0,0))` logs and skips a bad record instead of retrying it forever.
* **Delivery semantics:** at-least-once from Kafka's side. The Valkey and Postgres writes are fire-and-forget subscriptions
  from the listener thread, so a crash between offset commit and a store write can lose that one write. For a position
  feed that is overwritten seconds later this is acceptable; it would not be for billing data.
* **Producer:** `acks=1`, `linger.ms=20`, `max.block.ms=10s`, `delivery.timeout.ms=30s`.

### 3.3 Valkey
* **Key schema:** one hash, `active_flights`. Field = trimmed callsign, value = `FlightTelemetry` JSON.
* **Why a hash:** `HVALS` returns the whole map in one round trip, and `HSET` per event is O(1) and overwrites in place.
* **TTL strategy:** Valkey 8 has no per-field expiry on hashes. Freshness is therefore handled by the application:
  * readers ignore entries older than `ACTIVE_FLIGHT_STALE_SECONDS` (300 s), entries that don't parse, and entries from the other feed;
  * `DataRetentionService.evictStaleFlights()` runs `HDEL` on them every 60 s.

  The alternative of one key per aircraft with `EXPIRE` would need `SCAN` or `KEYS` to list active flights.
* **Write volume:** about 2.7 writes/s in simulation (8 aircraft every 3 s); live data arrives as 150 writes once a minute.

### 3.4 PostgreSQL (`schema.sql`)
```sql
flight_telemetry_history(
  id BIGSERIAL PK, callsign VARCHAR(32) NOT NULL, icao24 VARCHAR(24) NOT NULL,
  latitude/longitude DOUBLE PRECISION NOT NULL, altitude, velocity, heading DOUBLE PRECISION,
  recorded_at BIGINT NOT NULL /* epoch ms */, origin VARCHAR(16) /* OPENSKY | SIMULATOR */)
```
| Index | Serves |
|---|---|
| `(callsign, recorded_at DESC)` | Trail by exact callsign |
| `(UPPER(callsign), recorded_at DESC)` | Case-insensitive lookup without a sequential scan |
| `(LOWER(icao24), recorded_at DESC)` | Lookup by transponder address |
| `(recorded_at)` | Hourly `DELETE … WHERE recorded_at < cutoff` and the "recent flights" fallback |

Growth is bounded twice: at most one row per aircraft per 15 s, and 24 h retention.

### 3.5 React state for streaming data
* `flights` is an object keyed by upper-case callsign. Updates replace a key, so the update cost doesn't grow with the number of aircraft on screen.
* WebSocket frames go into `pendingRef` (a `Map`) and are flushed by one `setFlights` every 100 ms
  (`App.jsx:88–93`). A burst of 150 live updates causes one render instead of 150.
* What is displayed is derived with `useMemo`: filter by feed (`matchesFeed`), sort by callsign, then filter by the
  search query. The sort is deterministic so cards don't jump around.
* Every 30 s a sweep drops aircraft not heard from in 5 minutes, measured by the browser's own receive time, so clock skew between server and browser doesn't matter.
* On reconnect the socket reloads `/api/flights/active`, so frames missed while disconnected are recovered.

---

## 4. Line-level "why this instead of that"

1. **`TelemetryProducer.java:40`, `.subscribeOn(Schedulers.boundedElastic())`.**
   *Instead of calling `send()` on the current thread.* `KafkaTemplate.send()` can block for up to `max.block.ms` waiting
   for metadata. In the live path the current thread is the Netty event loop that also serves HTTP and WebSocket traffic.
   `boundedElastic` is Reactor's pool for blocking work. A full migration to Reactor Kafka would also fix this, but at the
   cost of Spring Kafka's listener container, error handler and embedded-Kafka testing.

2. **`TelemetryIngestionService.java:49`, a reactive `@Scheduled` method returning `Mono<Void>`.**
   *Instead of a `void` method that calls `.block()`, or a `Flux.interval`.* With fixed delay, Spring waits for the Mono to
   complete before scheduling the next poll, so slow polls never overlap and no thread is parked. `Mono.defer` makes each tick re-check the quota.
   `Flux.interval` would keep firing on schedule whether or not the previous poll had finished, so a slow OpenSky response would
   stack up polls and spend quota faster.

3. **`TelemetryModeManager.java:103`, `synchronized tryAcquireLiveRequest()`.**
   *Instead of an `AtomicInteger` alone, or Redis `INCR`.* The check-then-increment, the window roll-over and the mode transition
   have to be one atomic step. A lone `AtomicInteger` would make the counter safe but not the transition. The call happens once a minute,
   so lock contention is irrelevant. A 32-thread test proves exactly N grants. Redis would survive restarts and replicas; that is the
   documented next step (§0, #8).

4. **`AviationApiClient.java:94` and `:103`, `exchangeToMono` with an explicit `status == 429` branch.**
   *Instead of `retrieve()` with generic `WebClientResponseException`s.* A 429 has to be told apart from a 503 because they
   drive different failovers (immediate versus after 3 errors), and the `X-Rate-Limit-Retry-After-Seconds` header has to be read
   before the body is released. `.timeout()` at `:95` sits on top of the Netty timeouts, so a server that sends headers and then
   stalls on the body still ends the request.

5. **`AviationApiClient` parsing through `JsonNode`.**
   *Instead of `bodyToMono(Map.class)` with casts.* OpenSky rows are heterogeneous arrays: strings, numbers, booleans and nulls by
   position. A `JsonNode` walk checks each type (`isString`, `isNumber`) and skips bad rows without `ClassCastException`
   and without the unchecked warnings the old raw `Map` code produced.

6. **`TelemetryStream.java:21`, `Sinks.many().multicast().directBestEffort()`.**
   *Instead of `multicast().onBackpressureBuffer()`, which was the original code.* That sink cancels itself when its last
   subscriber leaves, so the first time every browser tab closed, broadcasting stopped until a restart. `directBestEffort`
   survives having zero subscribers and doesn't buffer frames for nobody.

7. **`TelemetryStream.java:29`, a per-subscriber `onBackpressureBuffer(512, DROP_OLDEST)`.**
   *Instead of unbounded buffering, or blocking the producer.* One slow phone must not stall the broadcast or grow memory without limit.
   For position data the newest frame is the only one that matters, so dropping the oldest is the right trade.

8. **`TelemetryWebSocketHandler.java:26`, `Mono.firstWithSignal(outbound, inbound)`.**
   *Instead of returning only `session.send(...)`.* `send` alone over an infinite stream finishes only when a write fails.
   Watching the inbound side ends the session, and its sink subscription, the moment the browser closes.

9. **`WebSocketConfig.java` and WebSocket instead of SSE or polling.**
   SSE would also work, since traffic only flows server to client. WebSocket was kept because the original client already used
   it, it streams through the same Nginx and Render paths, and the handshake `Origin` is checked with the same CORS policy
   (`WebSocketConfig.java:22`). Polling every second from N browsers would mean N × 86,400 requests a day against the API
   and Valkey; with push, every browser shares one Kafka-to-sink fan-out.

10. **`TelemetryConsumer.java:59` and Kafka between ingestion and storage.**
    *Instead of calling Valkey, Postgres and the socket directly from the poller.* The live poller and the simulator are two
    independent producers that share one consumer path. If a store is slow, events queue in Kafka instead of in the
    poller, and consumers can restart and resume from committed offsets. An in-memory `Sinks` queue would be simpler and
    would lose everything on restart.

11. **`TelemetryConsumer.java:138`, `lastPersistedAt.compute(...)` throttle.**
    *Instead of writing every event to Postgres, or batching with a timer.* `compute` makes the check and the claim atomic per
    callsign. Valkey and the WebSocket still get every event while Postgres gets at most one row per aircraft per 15 s, which
    keeps free-tier storage and connection use low. A failed save releases the slot, so the next event retries.

12. **`FlightLookupService.java:59–65`, cache, then archive, then 404, with deferred fallbacks.**
    *Instead of querying Postgres every time.* Active aircraft come from memory. Postgres is only touched on a miss
    (`Mono.defer`), and a Valkey outage degrades to the archive instead of returning a 500. Errors map to 503 only
    when the archive also fails.

13. **`ActiveFlightService.java:71`, `/api/flights/active` falls back to Postgres.**
    *Instead of returning 500 when Valkey is down.* `findLatestSince(now - 300 s)` with `DISTINCT ON (callsign)`
    rebuilds the same snapshot from the archive. It is slower but correct, and the dashboard keeps working.

14. **`ActiveFlightService.java:152` and `App.jsx:142`, feed-consistent filtering on the server and the client.**
    *Instead of trusting the stale-entry TTL to clean up.* Without it, synthetic aircraft would appear under a "LIVE FEED" badge for
    up to 5 minutes after a switch. Filtering on both sides makes the badge and the data agree at every moment.

15. **`schema.sql:27`, functional index `UPPER(callsign)`.**
    *Instead of `ILIKE`, or lower-casing at write time.* The lookup query is `UPPER(callsign) = UPPER(:identifier)`, which
    can only use an index built on the same expression. Stored values keep their original case.

16. **`KafkaConfig.java:121`, `DefaultErrorHandler(new FixedBackOff(0L, 0L))`.**
    *Instead of the default 10 retries.* The listener catches everything itself, but if something ever escaped, retrying a poison
    record would freeze the partition. Logging and skipping is the right choice for a live position stream.

---

## 5. Honest positioning: why build this?

### 5.1 What naive polling gets wrong
A "simple" tracker has each browser call the aviation API directly, or has the backend proxy each request to it.

| Problem | Naive polling | AeroSync |
|---|---|---|
| **Rate-limit amplification** | N viewers make N × poll-rate upstream calls. OpenSky's 400/day is gone in minutes. | One server-side poller per 60 s, whatever the viewer count. A quota state machine guarantees at most 400 calls per UTC day. |
| **Upstream outage** | Blank map or errors. | Automatic failover to a clearly labelled simulated feed, with cool-down recovery. |
| **Fan-out** | Every client repeats the same work. | One Kafka consumer feeds one multicast sink that fans out to every socket. |
| **Backpressure** | A slow client slows the request it is waiting on. | A bounded drop-oldest buffer per subscriber. Kafka absorbs store slowness. |
| **Hot and cold data** | Either everything comes from the API or everything goes to one DB. | Valkey holds "now" (O(1) overwrite), Postgres holds a throttled 24 h history with indexed lookup. |
| **Coupling** | The poller has to know every consumer. | Producers (live, simulator) and consumers (cache, archive, socket) only share a topic. |

### 5.2 Where it sits next to Flightradar24 and similar trackers
Commercial trackers run their **own sensor networks**: tens of thousands of ADS-B and MLAT receivers feeding raw messages
in near real time, decoding, tracking, merging with schedule data, and serving it globally.

AeroSync is an **aggregation and streaming pipeline on top of one public API**. It has no receivers. Its live data is only as fresh as
a 60 s poll of OpenSky, limited to one bounding box, capped at 150 aircraft per poll, and runs as a single instance.
Its value is the **engineering shape**: decoupled ingestion, quota-aware failover, hot and cold storage, push fan-out and
observability. Coverage and accuracy are not where it competes.

### 5.3 Interview Q&A
**Q: Is it really non-blocking?**
The HTTP, WebSocket, Redis (Lettuce) and Postgres (R2DBC) paths are. Kafka is the exception: Spring Kafka is thread-based. The
producer's potentially blocking `send` is moved to `boundedElastic`, and the consumer runs on its own listener thread and only
fires reactive writes. The scheduled jobs are reactive `@Scheduled` methods with no `.block()`.

**Q: Why Kafka for one producer and one consumer?**
There are two producers (live and simulator). Kafka also gives the pipeline a durable buffer, ordering per aircraft key and
replayable offsets, and adding a consumer (alerts, analytics) needs no change to ingestion. At this scale an in-memory
queue would work. Kafka is here for decoupling and failure isolation, not for throughput.

**Q: How do you stop the free tier being exhausted?**
`TelemetryModeManager` counts every OpenSky call against `OPENSKY_DAILY_LIMIT` atomically and switches to the simulator at
the limit, on a 429, or after 3 consecutive failures. The UI badge then says `SIMULATED FALLBACK` with the reason. The known
gap is that the counter is in memory, so a restart resets it.

**Q: What happens when a recruiter opens the link and OpenSky is down?**
After at most 3 failed polls the badge turns amber, `SIMULATED FALLBACK · OpenSky API unreachable`, and 8
synthetic aircraft fly. Live data is never shown mixed with synthetic data. The app probes OpenSky again after 15 minutes.

**Q: Why not just use Redis TTLs?**
Valkey 8 hashes have no per-field TTL, and one key per aircraft would need `SCAN` to list them. Filtering on read
(always correct) plus a 60 s `HDEL` sweep (keeps memory tidy) gives the same effect with one `HVALS`.

**Q: What would you change for production?**
* Persist the quota in Valkey.
* Use leader election for the poller so replicas don't double-poll.
* Make the cache and DB writes part of the consumer's acknowledgement (or move to Reactor Kafka) for true backpressure.
* Use OpenSky's OAuth2 client-credentials flow.
* Put auth in front of `/actuator`.
* Add a Prometheus server and Grafana dashboards with alerts on `aerosync_opensky_requests_total{outcome="error"}`.

---

## 6. Hosting and deployment blueprint ($0 tier)

```
Vercel/Netlify (frontend/)  ──HTTPS + WSS──►  Render free web service (backend/Dockerfile, 512 MB)
                                                 ├─ Neon or Aiven PostgreSQL  (R2DBC, sslMode=require)
                                                 ├─ Aiven Valkey              (rediss://)
                                                 └─ Aiven Kafka               (SSL client certs or SASL_SSL)
```

**Backend environment** (Render prompts for these from `render.yaml`):

| Variable | Example |
|---|---|
| `SPRING_R2DBC_URL` | `r2dbc:postgresql://<host>/<db>?sslMode=require` |
| `SPRING_R2DBC_USERNAME`, `SPRING_R2DBC_PASSWORD` | from the provider |
| `SPRING_DATA_REDIS_URL` | `rediss://default:<pw>@<host>:<port>` |
| `SPRING_KAFKA_BOOTSTRAP_SERVERS` | `<host>:<port>` |
| `SPRING_KAFKA_PROPERTIES_SECURITY_PROTOCOL` | `SSL` (or `SASL_SSL` plus `..._SASL_MECHANISM` and `..._SASL_JAAS_CONFIG`) |
| `KAFKA_CA_CERT_B64`, `KAFKA_CLIENT_CERT_B64`, `KAFKA_CLIENT_KEY_B64` | `base64 -i file \| tr -d '\n'` (key must be PKCS#8) |
| `CORS_ALLOWED_ORIGINS` | `https://<app>.vercel.app` |
| `OPENSKY_ENABLED`, `OPENSKY_DAILY_LIMIT` | `true`, `400` |
| `LOGGING_STRUCTURED_FORMAT_CONSOLE` (optional) | `ecs` for JSON logs |

**Frontend environment:** `VITE_API_BASE_URL=https://<backend>.onrender.com`. The WebSocket URL is derived from it.

**Health checks**

| Endpoint | Use |
|---|---|
| `/api/health` | Render health check. Liveness only, touches no dependency, so a Kafka blip doesn't restart the service. |
| `/actuator/health/liveness`, `/actuator/health/readiness` | Kubernetes-style probes |
| `/actuator/health` | Component status (`r2dbc`, `redis`, `ping`, `diskSpace`), without connection details |

**Startup verification**
1. `curl https://<backend>/api/health` returns `{"status":"UP",...}`.
2. `curl https://<backend>/actuator/health` returns `"status":"UP"`, with `r2dbc` and `redis` UP (database and cache reachable).
3. `curl https://<backend>/api/system/status`. `feed` is `LIVE_FEED` after the first poll (about 5 s after start); `requestsUsedToday` is 1 or more.
4. `curl https://<backend>/api/system/metrics`. `eventsConsumed` grows, so Kafka is working end to end.
5. `curl https://<backend>/actuator/prometheus | grep aerosync_` lists the custom meters.
6. Open the Vercel URL. The badge shows LIVE FEED (or an honest SIMULATED reason), and the health bar shows API UP and WS clients 1 or more.

**Free-tier caveats**
* Render's free tier sleeps after about 15 minutes idle. The first visit after that takes about a minute ("Waking up…" banner), and no OpenSky polling happens while it sleeps.
* Requests from cloud IPs to OpenSky may be rejected. The app then shows `SIMULATED FALLBACK · OpenSky API unreachable`.
