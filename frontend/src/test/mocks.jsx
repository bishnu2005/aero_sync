import { vi } from 'vitest'

export class MockWebSocket {
  static instances = []

  constructor(url) {
    this.url = url
    this.readyState = 0
    MockWebSocket.instances.push(this)
  }

  static latest() {
    return MockWebSocket.instances[MockWebSocket.instances.length - 1]
  }

  open() {
    this.readyState = 1
    this.onopen?.()
  }

  message(payload) {
    this.onmessage?.({ data: typeof payload === 'string' ? payload : JSON.stringify(payload) })
  }

  close() {
    this.readyState = 3
    this.onclose?.()
  }

  send() {}
}

const json = (status, body) => ({
  ok: status >= 200 && status < 300,
  status,
  json: async () => body,
})

/** fetch stub routing the AeroSync API; `lookups` maps identifier -> [status, body]. */
export function installFetch({ flights = [], status = LIVE_STATUS, metrics = METRICS, lookups = {} } = {}) {
  const fetchMock = vi.fn(async (input) => {
    const url = String(input)
    if (url.endsWith('/api/flights/active')) return json(200, flights)
    if (url.endsWith('/api/system/status')) return json(200, status)
    if (url.endsWith('/api/system/metrics')) {
      if (metrics === null) throw new TypeError('Failed to fetch')
      return json(200, metrics)
    }
    const match = url.match(/\/api\/flights\/([^/]+)$/)
    if (match) {
      const id = decodeURIComponent(match[1])
      const [code, body] = lookups[id] ?? [
        404,
        { status: 404, error: 'FLIGHT_NOT_FOUND', message: `No flight found for '${id}'.` },
      ]
      return json(code, body)
    }
    return json(404, {})
  })
  globalThis.fetch = fetchMock
  return fetchMock
}

export const METRICS = {
  feed: 'LIVE_FEED',
  eventsPerSecond: 2.7,
  eventsConsumed: 12345,
  eventsPublishedLive: 12000,
  eventsPublishedSimulated: 345,
  eventsDropped: 3,
  kafkaPublishFailures: 0,
  cacheHitRatio: 0.917,
  cacheLookups: 12,
  cacheWriteFailures: 0,
  dbWrites: 800,
  dbWritesThrottled: 11000,
  dbWriteFailures: 0,
  upstreamRequests: 40,
  upstreamErrors: 1,
  upstreamRateLimited: 1,
  websocketSubscribers: 3,
  uptimeSeconds: 3725,
  timestamp: '2026-11-27T10:00:00Z',
}

export const LIVE_STATUS = {
  mode: 'LIVE',
  reason: 'NONE',
  message: 'Live OpenSky radar active (12/400 polls used today).',
  requestsUsedToday: 12,
  dailyLimit: 400,
  windowResetAt: '2026-11-28T00:00:00Z',
  activeFlightCount: 2,
  feed: 'LIVE_FEED',
}

export const flight = (callsign, icao24, overrides = {}) => ({
  callsign,
  icao24,
  latitude: 28.5562,
  longitude: 77.1,
  altitude: 35000,
  velocity: 245,
  heading: 210,
  timestamp: Date.now(),
  ...overrides,
})
