export const MPS_TO_KNOTS = 1.943844
export const IDENTIFIER_PATTERN = /^[A-Za-z0-9_-]{1,32}$/

const CARDINALS = ['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW']

export const toKnots = (mps) => Math.round((Number(mps) || 0) * MPS_TO_KNOTS)

export function cardinal(heading) {
  const h = ((Number(heading) || 0) % 360 + 360) % 360
  return CARDINALS[Math.round(h / 45) % 8]
}

export const formatNumber = (value, digits = 0) =>
  Number.isFinite(Number(value))
    ? Number(value).toLocaleString('en-US', { minimumFractionDigits: digits, maximumFractionDigits: digits })
    : '—'

export const formatCoord = (value) => (Number.isFinite(Number(value)) ? Number(value).toFixed(4) : '—')

export function formatLastContact(timestamp, now = Date.now()) {
  if (!timestamp) return '—'
  const seconds = Math.max(0, Math.round((now - timestamp) / 1000))
  if (seconds < 5) return 'just now'
  if (seconds < 60) return `${seconds}s ago`
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`
  return new Date(timestamp).toLocaleString()
}

export const flightKey = (flight) => (flight?.callsign ?? '').trim().toUpperCase()

/** Stable order so cards never jump around as telemetry streams in. */
export const sortFlights = (flights) =>
  [...flights].sort(
    (a, b) => flightKey(a).localeCompare(flightKey(b)) || (a.icao24 ?? '').localeCompare(b.icao24 ?? ''),
  )

export function matchesQuery(flight, query) {
  const q = query.trim().toLowerCase()
  if (!q) return true
  return (flight.callsign ?? '').toLowerCase().includes(q) || (flight.icao24 ?? '').toLowerCase().includes(q)
}

export function findExactMatch(flights, query) {
  const q = query.trim().toLowerCase()
  return flights.find(
    (f) => (f.callsign ?? '').trim().toLowerCase() === q || (f.icao24 ?? '').trim().toLowerCase() === q,
  )
}

/** Mirrors the backend's validation so obviously bad input never leaves the browser. */
export function validateIdentifier(raw) {
  const value = raw.trim()
  if (!value) return 'Enter a callsign or ICAO24 address.'
  if (value.length > 32) return 'Flight IDs are at most 32 characters.'
  if (!IDENTIFIER_PATTERN.test(value)) return "Use only letters, digits, '_' or '-'."
  return null
}
