# AeroSync

Real-time, event-driven flight tracking for Indian airspace. Live state vectors from the
[OpenSky Network](https://opensky-network.org) (or a built-in flight simulator) stream through
Kafka into a Valkey cache, a PostgreSQL archive and a WebSocket feed rendered on a Leaflet map.

```
 OpenSky REST ──► TelemetryIngestionService ─┐               ┌─► Valkey  (active_flights hash)
                  (quota-gated, 60 s poll)   ├─► Kafka ──► TelemetryConsumer ─┼─► PostgreSQL (throttled history)
 FlightSimulator ────────────────────────────┘  telemetry.flights.live       └─► TelemetryStream ─► /ws/telemetry ─► React
 (when in SIMULATION mode, 3 s tick)
                                                  REST: /api/flights/active · /api/flights/{id} · /api/system/status · /api/health
```

| Layer    | Stack |
|----------|-------|
| Backend  | Java 21, Spring Boot 4.1.1 (WebFlux / Netty), Spring Kafka, R2DBC PostgreSQL, Reactive Redis (Valkey) |
| Frontend | React 19, Vite 8, Leaflet + react-leaflet, native WebSockets |
| Infra    | Kafka (KRaft), PostgreSQL 16, Valkey 8, Nginx |

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
positions go through the same Kafka pipeline as live data. The UI status pill shows the current mode,
reason and `used/limit` polls.

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
| GET | `/api/system/status` | `mode`, `reason`, `message`, `requestsUsedToday`, `dailyLimit`, `windowResetAt`, `activeFlightCount` |
| GET | `/api/flights/active` | Active, non-stale flights sorted by callsign |
| GET | `/api/flights/{identifier}` | Callsign / ICAO24 lookup (`200`, `400 INVALID_IDENTIFIER`, `404 FLIGHT_NOT_FOUND`, `503 SERVICE_UNAVAILABLE`) |
| WS  | `/ws/telemetry` | One JSON `FlightTelemetry` frame per position update |

`FlightTelemetry`: `callsign`, `icao24`, `latitude`, `longitude`, `altitude` (ft), `velocity` (m/s),
`heading` (degrees true), `timestamp` (epoch ms).

## Local quickstart (Topology A: Docker)

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

None of the tests call `opensky-network.org` or need Docker. The OpenSky client is tested against a
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
| `TelemetryIngestionServiceTest` | No API call in simulation mode, 429 handling, success path, Kafka failures not counted as upstream errors |
| `TelemetryConsumerTest` | Blank/null callsigns, trimming, per-callsign PostgreSQL throttle, Valkey/PostgreSQL/WebSocket fault isolation, malformed JSON |
| `FlightControllerApiContractTest` | Every endpoint and error path: lookup fallbacks, validation and injection strings, 404/405/503/500, CORS allow/deny |
| `TelemetryStreamAndWebSocketTest` | Multicast, cancellation, surviving zero subscribers, 10k-message bursts, slow consumers, WebSocket handler lifecycle |
| `KafkaPipelineIntegrationTest` | Full context: producer → Kafka → consumer → Valkey/PostgreSQL → real WebSocket client; WebSocket Origin rejection |
| `FlightSimulatorTest`, `DataRetentionServiceTest`, `KafkaSecuritySupportTest` | Corridors and realism, pruning and eviction, PEM/SASL mapping |
| `frontend/src/test/*` | Rendering, status badges, live filtering, search (live hit, archive hit, 404, 400, client validation), WebSocket updates in the open card, marker click, sidebar collapse, theme toggle, zoom, cold-start and reconnect banners, URL resolution |

## $0 free-tier deployment (Topology B)

```
 Vercel / Netlify (static React) ──HTTPS/WSS──► Render / Koyeb (backend Docker, 512 MB)
                                                     ├─► Aiven for Apache Kafka (free)
                                                     ├─► Aiven for Valkey (free)  or Upstash Redis
                                                     └─► Aiven for PostgreSQL (free)  or Neon
```

1. **Managed services.** Create the free Kafka, Valkey and PostgreSQL services (Aiven covers all three;
   Neon and Upstash also work). Create the topic `telemetry.flights.live` if your Kafka plan doesn't
   allow auto-creation. AeroSync also declares it on startup.
   * The simulator sends about 230k Kafka messages and Valkey writes per day (8 aircraft every 3 s). Check
     your Redis provider's command quota: Aiven Valkey has no per-command cap, and Upstash's free tier may
     run out. Raise `SIMULATOR_INTERVAL_MS` to cut the volume.
2. **Backend on Render.** Use *New → Blueprint* with this repo; `render.yaml` defines a free Docker
   web service with `healthCheckPath: /api/health`. Fill in the prompted secrets:
   * `SPRING_R2DBC_URL=r2dbc:postgresql://<host>:<port>/<db>?sslMode=require`, plus `SPRING_R2DBC_USERNAME` / `SPRING_R2DBC_PASSWORD`
   * `SPRING_DATA_REDIS_URL=rediss://default:<password>@<host>:<port>`
   * `SPRING_KAFKA_BOOTSTRAP_SERVERS=<host>:<port>`, `SPRING_KAFKA_PROPERTIES_SECURITY_PROTOCOL=SSL`
   * `KAFKA_CA_CERT_B64`, `KAFKA_CLIENT_CERT_B64`, `KAFKA_CLIENT_KEY_B64`: Aiven's `ca.pem`, `service.cert`,
     `service.key`, each encoded with `base64 -i <file> | tr -d '\n'`. The key must be PKCS#8
     (`BEGIN PRIVATE KEY`); convert with `openssl pkcs8 -topk8 -nocrypt -in service.key` if needed.
     For SASL_SSL, set `SPRING_KAFKA_PROPERTIES_SASL_MECHANISM` and `SPRING_KAFKA_PROPERTIES_SASL_JAAS_CONFIG` instead of the client cert/key.
   * `CORS_ALLOWED_ORIGINS=https://<your-app>.vercel.app` (comma separated; patterns allowed)

   **Koyeb** works the same way: deploy `backend/` as a Docker service, set the same variables and use
   `/api/health` as the health check. `PORT` is read automatically.
3. **Frontend on Vercel or Netlify.** Set the project root to `frontend/` and add
   `VITE_API_BASE_URL=https://<backend>.onrender.com`. The WebSocket URL is derived from it
   (`https` → `wss`); set `VITE_WS_URL` only to override it. `vercel.json` and `public/_redirects`
   handle SPA routing.
4. **Cold starts.** Free Render services sleep when idle. The UI shows "Waking up AeroSync server..."
   and reconnects on its own once the backend is up.

### OpenSky credentials
Anonymous access gives 400 requests a day, which is the default. `OPENSKY_USERNAME` / `OPENSKY_PASSWORD`
are sent as HTTP Basic Auth when set, and you can raise `OPENSKY_DAILY_LIMIT` up to 4000. OpenSky has
been moving accounts to OAuth2 client credentials. AeroSync does not implement that flow yet, so leave
the credentials blank if your account only supports OAuth2.

## Configuration reference

All settings are environment variables; see [`.env.example`](.env.example) and
[`backend/src/main/resources/application.properties`](backend/src/main/resources/application.properties)
for the full list and defaults. Frontend build variables are documented in
[`frontend/.env.example`](frontend/.env.example).
