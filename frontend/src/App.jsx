import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { MapContainer, Marker, TileLayer } from 'react-leaflet'
import 'leaflet/dist/leaflet.css'

import { apiUrl, ENDPOINTS, getWsUrl } from './config/api'
import { findExactMatch, flightKey, matchesQuery, sortFlights, validateIdentifier } from './lib/flight'
import { planeIcon } from './lib/planeIcon'
import { TILE_THEMES } from './lib/mapTheme'
import { feedOf, matchesFeed } from './lib/status'
import { useSystemStatus } from './hooks/useSystemStatus'
import { useTelemetrySocket } from './hooks/useTelemetrySocket'
import ConnectionBanner from './components/ConnectionBanner'
import FlightDetailCard from './components/FlightDetailCard'
import MapController from './components/MapController'
import MapControls from './components/MapControls'
import Sidebar from './components/Sidebar'
import SystemHealthBar from './components/SystemHealthBar'

const INDIA_CENTER = [21.0, 78.0]
const SLOW_START_MS = 2500
const FLUSH_MS = 100
const CLIENT_STALE_MS = 5 * 60 * 1000

/** Fetches the active-flight snapshot keyed by callsign; resolves null if the server is unreachable. */
async function fetchActiveFlights() {
  try {
    const res = await fetch(apiUrl(ENDPOINTS.activeFlights))
    if (!res.ok) return null
    const data = await res.json()
    const now = Date.now()
    const next = {}
    for (const flight of Array.isArray(data) ? data : []) {
      if (flight?.callsign) next[flightKey(flight)] = { ...flight, receivedAt: now }
    }
    return next
  } catch {
    return null // the banner covers this; the WebSocket reconnect triggers another load
  }
}

export default function App() {
  const [flights, setFlights] = useState({})
  const [loaded, setLoaded] = useState(false)
  const [slowStart, setSlowStart] = useState(false)
  const [selectedKey, setSelectedKey] = useState(null)
  const [lookupDetail, setLookupDetail] = useState(null)
  const [focus, setFocus] = useState(null)
  const [theme, setTheme] = useState('dark')
  const [collapsed, setCollapsed] = useState(false)
  const [query, setQuery] = useState('')
  const [searching, setSearching] = useState(false)
  const [feedback, setFeedback] = useState(null)

  const mapRef = useRef(null)
  const pendingRef = useRef(new Map())
  const flushTimerRef = useRef(null)

  const { status, refresh: refreshStatus } = useSystemStatus()
  const feed = status && !status.unavailable ? feedOf(status) : null
  const feedRef = useRef(feed)

  const loadActiveFlights = useCallback(() => {
    return fetchActiveFlights().then((next) => {
      if (!next) return
      setFlights(next)
      setLoaded(true)
    })
  }, [])

  useEffect(() => {
    const timer = setTimeout(() => setSlowStart(true), SLOW_START_MS)
    let cancelled = false
    fetchActiveFlights().then((next) => {
      if (cancelled || !next) return
      setFlights(next)
      setLoaded(true)
    })
    return () => {
      cancelled = true
      clearTimeout(timer)
    }
  }, [])

  // WebSocket frames are batched so a burst of live OpenSky updates costs one render.
  const handleTelemetry = useCallback((telemetry) => {
    // A frame from the other feed means the backend switched modes: re-check the badge right away.
    if (feedRef.current && !matchesFeed(telemetry, feedRef.current)) refreshStatus()
    pendingRef.current.set(flightKey(telemetry), telemetry)
    if (flushTimerRef.current) return
    flushTimerRef.current = setTimeout(() => {
      flushTimerRef.current = null
      const batch = pendingRef.current
      pendingRef.current = new Map()
      const now = Date.now()
      setFlights((prev) => {
        const next = { ...prev }
        batch.forEach((t, key) => {
          next[key] = { ...t, receivedAt: now }
        })
        return next
      })
    }, FLUSH_MS)
  }, [refreshStatus])

  // When the feed changes (live <-> simulated), reload the snapshot so the other feed's aircraft vanish.
  useEffect(() => {
    const previous = feedRef.current
    feedRef.current = feed
    if (!previous || !feed || previous === feed) return
    let cancelled = false
    fetchActiveFlights().then((next) => {
      if (!cancelled && next) setFlights(next)
    })
    return () => {
      cancelled = true
    }
  }, [feed])

  const handleSocketOpen = useCallback(() => {
    setLoaded(true)
    loadActiveFlights()
  }, [loadActiveFlights])

  const socketStatus = useTelemetrySocket(getWsUrl(), { onMessage: handleTelemetry, onOpen: handleSocketOpen })

  useEffect(() => () => clearTimeout(flushTimerRef.current), [])

  // Drop aircraft the server has stopped reporting.
  useEffect(() => {
    const id = setInterval(() => {
      const cutoff = Date.now() - CLIENT_STALE_MS
      setFlights((prev) => {
        const entries = Object.entries(prev).filter(([, f]) => f.receivedAt >= cutoff)
        return entries.length === Object.keys(prev).length ? prev : Object.fromEntries(entries)
      })
    }, 30000)
    return () => clearInterval(id)
  }, [])

  // Only aircraft from the feed named on the badge are shown, so synthetic data is never labelled live.
  const allFlights = useMemo(
    () => sortFlights(Object.values(flights).filter((f) => matchesFeed(f, feed))),
    [flights, feed],
  )
  const visibleFlights = useMemo(() => allFlights.filter((f) => matchesQuery(f, query)), [allFlights, query])

  const liveSelected = selectedKey && flights[selectedKey] && matchesFeed(flights[selectedKey], feed) ? flights[selectedKey] : null
  const detail = liveSelected ? { ...liveSelected, status: 'ACTIVE', source: 'LIVE_STREAM' } : lookupDetail
  const historicalMarker = !liveSelected && lookupDetail ? lookupDetail : null

  const focusOn = useCallback((flight) => {
    if (!flight) return
    setFocus({ lat: Number(flight.latitude), lon: Number(flight.longitude), at: Date.now() })
  }, [])

  const selectFlight = useCallback(
    (flight) => {
      setSelectedKey(flightKey(flight))
      setLookupDetail(null)
      setFeedback(null)
      focusOn(flight)
    },
    [focusOn],
  )

  const closeDetail = useCallback(() => {
    setSelectedKey(null)
    setLookupDetail(null)
  }, [])

  const handleSearch = async () => {
    const identifier = query.trim()
    const invalid = validateIdentifier(identifier)
    if (invalid) {
      setFeedback({ kind: 'error', text: invalid })
      return
    }
    const live = findExactMatch(allFlights, identifier)
    if (live) {
      selectFlight(live)
      return
    }

    setSearching(true)
    setFeedback(null)
    try {
      const res = await fetch(apiUrl(ENDPOINTS.flight(identifier)))
      const body = await res.json().catch(() => null)
      if (res.ok && body) {
        setSelectedKey(flightKey(body))
        setLookupDetail(body)
        focusOn(body)
        if (body.status === 'HISTORICAL') {
          setFeedback({ kind: 'info', text: `Showing last archived position for ${body.callsign}.` })
        }
      } else if (res.status === 404) {
        setFeedback({ kind: 'error', text: `No flight found for '${identifier}'` })
      } else if (res.status === 400) {
        setFeedback({ kind: 'error', text: body?.message ?? 'Invalid flight identifier.' })
      } else if (res.status === 503) {
        setFeedback({ kind: 'error', text: 'Flight data is temporarily unavailable. Try again shortly.' })
      } else {
        setFeedback({ kind: 'error', text: 'Search failed. Please try again.' })
      }
    } catch {
      setFeedback({ kind: 'error', text: 'Cannot reach AeroSync server. Please try again.' })
    } finally {
      setSearching(false)
    }
  }

  const clearSearch = () => {
    setQuery('')
    setFeedback(null)
  }

  const onMapReady = useCallback((map) => {
    mapRef.current = map
  }, [])

  let bannerMessage = null
  if (!loaded && slowStart) bannerMessage = 'Waking up AeroSync server...'
  else if (loaded && socketStatus === 'reconnecting') bannerMessage = 'Reconnecting to telemetry stream...'

  return (
    <div className="app" data-theme={theme}>
      <Sidebar
        collapsed={collapsed}
        onToggle={() => setCollapsed((c) => !c)}
        status={status}
        flights={visibleFlights}
        totalCount={allFlights.length}
        query={query}
        onQueryChange={(value) => {
          setQuery(value)
          setFeedback(null)
        }}
        onSearch={handleSearch}
        onClear={clearSearch}
        searching={searching}
        feedback={feedback}
        selectedKey={selectedKey}
        onSelect={selectFlight}
      />

      <ConnectionBanner message={bannerMessage} />

      <SystemHealthBar />

      <FlightDetailCard flight={detail} onClose={closeDetail} onCenter={focusOn} />

      <MapControls
        theme={theme}
        onToggleTheme={() => setTheme((t) => (t === 'dark' ? 'light' : 'dark'))}
        onZoomIn={() => mapRef.current?.zoomIn()}
        onZoomOut={() => mapRef.current?.zoomOut()}
      />

      <MapContainer center={INDIA_CENTER} zoom={5} zoomControl={false} className="map" worldCopyJump>
        <TileLayer key={theme} url={TILE_THEMES[theme].url} attribution={TILE_THEMES[theme].attribution} maxZoom={16} />
        {TILE_THEMES[theme].labelsUrl && (
          <TileLayer key={`${theme}-labels`} url={TILE_THEMES[theme].labelsUrl} maxZoom={16} />
        )}
        <MapController focus={focus} onReady={onMapReady} />

        {visibleFlights.map((flight) => {
          const key = flightKey(flight)
          return (
            <Marker
              key={key}
              position={[flight.latitude, flight.longitude]}
              icon={planeIcon(flight.heading, theme, key === selectedKey)}
              title={flight.callsign}
              eventHandlers={{ click: () => selectFlight(flight) }}
            />
          )
        })}

        {historicalMarker && (
          <Marker
            key={`historical-${flightKey(historicalMarker)}`}
            position={[historicalMarker.latitude, historicalMarker.longitude]}
            icon={planeIcon(historicalMarker.heading, theme, true, true)}
            title={`${historicalMarker.callsign} (last known)`}
          />
        )}
      </MapContainer>
    </div>
  )
}
