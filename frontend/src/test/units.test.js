import { describe, expect, it } from 'vitest'
import { getApiBaseUrl, getWsUrl } from '../config/api'
import { getTileThemes } from '../lib/mapTheme'
import { backoffDelay } from '../hooks/useTelemetrySocket'
import { feedOf, formatUptime, matchesFeed } from '../lib/status'
import { cardinal, findExactMatch, matchesQuery, sortFlights, toKnots, validateIdentifier } from '../lib/flight'

describe('api url resolution', () => {
  it('prefers VITE_API_BASE_URL and strips trailing slashes', () => {
    expect(getApiBaseUrl({ VITE_API_BASE_URL: 'https://aerosync.onrender.com/' })).toBe('https://aerosync.onrender.com')
  })

  it('uses localhost:8080 in dev and same-origin in production', () => {
    expect(getApiBaseUrl({ DEV: true })).toBe('http://localhost:8080')
    expect(getApiBaseUrl({ DEV: false })).toBe('')
  })

  it('uses VITE_WS_URL verbatim when set', () => {
    expect(getWsUrl({ VITE_WS_URL: 'wss://ws.example/ws/telemetry', VITE_API_BASE_URL: 'https://x' })).toBe(
      'wss://ws.example/ws/telemetry',
    )
  })

  it('derives the socket URL from VITE_API_BASE_URL', () => {
    expect(getWsUrl({ VITE_API_BASE_URL: 'https://aerosync.onrender.com/' })).toBe('wss://aerosync.onrender.com/ws/telemetry')
    expect(getWsUrl({ VITE_API_BASE_URL: 'http://10.0.0.2:8080' })).toBe('ws://10.0.0.2:8080/ws/telemetry')
  })

  it('falls back to localhost in dev and window.location in production', () => {
    expect(getWsUrl({ DEV: true })).toBe('ws://localhost:8080/ws/telemetry')
    expect(getWsUrl({}, { protocol: 'https:', host: 'aerosync.vercel.app' })).toBe('wss://aerosync.vercel.app/ws/telemetry')
    expect(getWsUrl({}, { protocol: 'http:', host: 'localhost:5173' })).toBe('ws://localhost:5173/ws/telemetry')
  })
})

describe('reconnect back-off', () => {
  it('doubles from 1s and caps at 30s', () => {
    expect([0, 1, 2, 3, 4, 5, 6, 10].map(backoffDelay)).toEqual([1000, 2000, 4000, 8000, 16000, 30000, 30000, 30000])
  })
})

describe('flight helpers', () => {
  const flights = [
    { callsign: 'IGO612', icao24: '800b02' },
    { callsign: 'AIC860', icao24: '800a01' },
  ]

  it('sorts deterministically by callsign', () => {
    expect(sortFlights(flights).map((f) => f.callsign)).toEqual(['AIC860', 'IGO612'])
  })

  it('matches callsign or icao24 case-insensitively', () => {
    expect(matchesQuery(flights[1], 'aic')).toBe(true)
    expect(matchesQuery(flights[1], '800A')).toBe(true)
    expect(matchesQuery(flights[1], 'igo')).toBe(false)
    expect(matchesQuery(flights[1], '   ')).toBe(true)
    expect(findExactMatch(flights, ' 800B02 ')?.callsign).toBe('IGO612')
    expect(findExactMatch(flights, '800b')).toBeUndefined()
  })

  it('converts speed and heading', () => {
    expect(toKnots(245)).toBe(476)
    expect([0, 44, 46, 90, 180, 225, 270, 315, 359, -90].map(cardinal)).toEqual([
      'N', 'NE', 'NE', 'E', 'S', 'SW', 'W', 'NW', 'N', 'W',
    ])
  })

  it('validates identifiers like the backend', () => {
    expect(validateIdentifier('AIC860')).toBeNull()
    expect(validateIdentifier('800a01')).toBeNull()
    expect(validateIdentifier('  ')).toMatch(/Enter/)
    expect(validateIdentifier('A'.repeat(33))).toMatch(/32/)
    expect(validateIdentifier('<script>')).toMatch(/letters/)
  })
})

describe('map tile themes', () => {
  it('defaults to keyless Esri canvas tiles with a labels overlay', () => {
    const themes = getTileThemes({})
    expect(themes.dark.url).toContain('World_Dark_Gray_Base')
    expect(themes.dark.labelsUrl).toContain('World_Dark_Gray_Reference')
    expect(themes.light.url).toContain('World_Light_Gray_Base')
  })

  it('lets a build override each theme with a custom provider', () => {
    const themes = getTileThemes({ VITE_MAP_TILE_DARK: 'https://tiles.example/dark/{z}/{x}/{y}.png?key=k' })
    expect(themes.dark).toMatchObject({ url: 'https://tiles.example/dark/{z}/{x}/{y}.png?key=k', labelsUrl: null })
    expect(themes.light.url).toContain('World_Light_Gray_Base')
  })
})

describe('observability helpers', () => {
  it('formats uptime compactly', () => {
    expect([0, 42, 125, 3725, 90061].map(formatUptime)).toEqual(['0s', '42s', '2m 5s', '1h 2m', '1d 1h'])
  })

  it('derives the feed label when an older backend omits it', () => {
    expect(feedOf({ mode: 'LIVE' })).toBe('LIVE_FEED')
    expect(feedOf({ mode: 'SIMULATION', reason: 'UPSTREAM_RATE_LIMITED_429' })).toBe('SIMULATED_FALLBACK')
    expect(feedOf({ mode: 'SIMULATION', reason: 'MANUAL_SIMULATION_CONFIG' })).toBe('SIMULATED_MANUAL')
  })
})

describe('feed filtering', () => {
  it('keeps synthetic and real aircraft apart', () => {
    const live = { origin: 'OPENSKY' }
    const legacy = {}
    const sim = { origin: 'SIMULATOR' }
    expect([live, legacy, sim].map((f) => matchesFeed(f, 'LIVE_FEED'))).toEqual([true, true, false])
    expect([live, legacy, sim].map((f) => matchesFeed(f, 'SIMULATED_FALLBACK'))).toEqual([false, false, true])
    expect([live, legacy, sim].map((f) => matchesFeed(f, 'SIMULATED_MANUAL'))).toEqual([false, false, true])
    expect([live, sim].map((f) => matchesFeed(f, null))).toEqual([true, true])
  })
})
