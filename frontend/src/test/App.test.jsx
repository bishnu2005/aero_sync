import { act, render, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import App from '../App'
import { flight, installFetch, LIVE_STATUS, MockWebSocket } from './mocks'

const leaflet = vi.hoisted(() => ({
  map: { flyTo: vi.fn(), getZoom: vi.fn(() => 5), zoomIn: vi.fn(), zoomOut: vi.fn() },
}))

vi.mock('react-leaflet', () => ({
  MapContainer: ({ children }) => <div data-testid="map">{children}</div>,
  TileLayer: ({ url }) => <div data-testid="tile-layer" data-url={url} />,
  Marker: ({ title, eventHandlers }) => (
    <button type="button" data-testid={`marker-${title}`} onClick={() => eventHandlers?.click?.()}>
      {title}
    </button>
  ),
  useMap: () => leaflet.map,
}))

const AIC860 = flight('AIC860', '800a01', { latitude: 28.5562, longitude: 77.1, altitude: 35000, velocity: 245, heading: 210 })
const IGO612 = flight('IGO612', '800b02', { latitude: 13.1986, longitude: 77.7066, altitude: 37000, velocity: 250, heading: 0 })

beforeEach(() => {
  MockWebSocket.instances = []
  globalThis.WebSocket = MockWebSocket
  Object.values(leaflet.map).forEach((fn) => fn.mockClear())
})

afterEach(() => {
  vi.useRealTimers()
})

const sidebar = () => screen.getByRole('region', { name: 'Flight sidebar' })
const detailCard = () => screen.getByRole('complementary', { name: /Flight details for/ })

describe('AeroSync dashboard', () => {
  it('renders the header, tracked count and initial flights from /api/flights/active', async () => {
    const fetchMock = installFetch({ flights: [IGO612, AIC860] })
    render(<App />)

    expect(screen.getByRole('heading', { name: 'AeroSync' })).toBeInTheDocument()
    await waitFor(() => expect(screen.getByTestId('tracked-count')).toHaveTextContent('2'))

    const feed = within(screen.getByRole('list', { name: 'Telemetry feed' }))
    const cards = feed.getAllByRole('button')
    // Deterministic alphabetical order, regardless of API order.
    expect(cards.map((c) => c.dataset.testid)).toEqual(['feed-card-AIC860', 'feed-card-IGO612'])
    expect(screen.getByTestId('marker-AIC860')).toBeInTheDocument()
    expect(fetchMock).toHaveBeenCalledWith('http://localhost:8080/api/flights/active')
    expect(MockWebSocket.latest().url).toBe('ws://localhost:8080/ws/telemetry')
  })

  it('shows the LIVE FEED badge with the quota counter', async () => {
    installFetch({ flights: [] })
    render(<App />)

    const pill = await screen.findByText('LIVE FEED')
    expect(pill.closest('[role="status"]')).toHaveAttribute('data-feed', 'LIVE_FEED')
    expect(screen.getByText('OpenSky Network · 12/400 polls today')).toBeInTheDocument()
  })

  it.each([
    ['DAILY_QUOTA_EXHAUSTED', 'Daily OpenSky quota reached (400/400) · synthetic flights'],
    ['UPSTREAM_RATE_LIMITED_429', 'OpenSky rate-limited the server (HTTP 429) · synthetic flights'],
    ['UPSTREAM_UNAVAILABLE', 'OpenSky API unreachable · synthetic flights'],
  ])('labels automatic failover (%s) as SIMULATED FALLBACK', async (reason, sub) => {
    installFetch({
      status: {
        ...LIVE_STATUS,
        mode: 'SIMULATION',
        reason,
        feed: 'SIMULATED_FALLBACK',
        requestsUsedToday: 400,
        message: 'Live flight simulator active.',
      },
    })
    render(<App />)

    const badge = await screen.findByText('SIMULATED FALLBACK')
    expect(badge.closest('[role="status"]')).toHaveAttribute('data-feed', 'SIMULATED_FALLBACK')
    expect(screen.getByText(sub)).toBeInTheDocument()
    expect(screen.queryByText('LIVE FEED')).not.toBeInTheDocument()
  })

  it('labels a configured simulator as SIMULATED FEED, never as live', async () => {
    installFetch({
      status: { ...LIVE_STATUS, mode: 'SIMULATION', reason: 'MANUAL_SIMULATION_CONFIG', feed: 'SIMULATED_MANUAL' },
    })
    render(<App />)
    expect(await screen.findByText('SIMULATED FEED')).toBeInTheDocument()
    expect(screen.getByText('Live feed disabled by configuration · synthetic flights')).toBeInTheDocument()
  })

  it('shows only real aircraft under LIVE FEED and ignores simulator frames', async () => {
    installFetch({
      flights: [
        { ...AIC860, origin: 'SIMULATOR' },
        { ...IGO612, callsign: 'ABY800', icao24: '896558', origin: 'OPENSKY' },
      ],
    })
    render(<App />)
    await screen.findByText('LIVE FEED')

    await waitFor(() => expect(screen.getByTestId('tracked-count')).toHaveTextContent('1'))
    expect(screen.getByTestId('feed-card-ABY800')).toBeInTheDocument()
    expect(screen.queryByTestId('feed-card-AIC860')).not.toBeInTheDocument()
    expect(screen.queryByTestId('marker-AIC860')).not.toBeInTheDocument()

    act(() => MockWebSocket.latest().message(flight('VTI941', '800c03', { origin: 'SIMULATOR' })))
    await new Promise((r) => setTimeout(r, 150))
    expect(screen.queryByTestId('feed-card-VTI941')).not.toBeInTheDocument()
    expect(screen.getByTestId('tracked-count')).toHaveTextContent('1')
  })

  it('shows only simulator aircraft under a simulated badge', async () => {
    installFetch({
      status: { ...LIVE_STATUS, mode: 'SIMULATION', reason: 'UPSTREAM_UNAVAILABLE', feed: 'SIMULATED_FALLBACK' },
      flights: [
        { ...AIC860, origin: 'SIMULATOR' },
        { ...IGO612, callsign: 'ABY800', icao24: '896558', origin: 'OPENSKY' },
      ],
    })
    const user = userEvent.setup()
    render(<App />)
    await screen.findByText('SIMULATED FALLBACK')

    await waitFor(() => expect(screen.getByTestId('tracked-count')).toHaveTextContent('1'))
    await user.click(screen.getByTestId('feed-card-AIC860'))
    expect(within(detailCard()).getByText('Simulator (synthetic)')).toBeInTheDocument()
    expect(screen.queryByTestId('feed-card-ABY800')).not.toBeInTheDocument()
  })

  it('renders live pipeline health from /api/system/metrics', async () => {
    const fetchMock = installFetch()
    render(<App />)

    const bar = within(await screen.findByRole('region', { name: 'System health' }))
    await waitFor(() => expect(bar.getByText('2.7')).toBeInTheDocument())
    expect(bar.getByText('UP')).toBeInTheDocument()
    expect(bar.getByText('12,345')).toBeInTheDocument()
    expect(bar.getByText('92%')).toBeInTheDocument()
    expect(bar.getByText('3')).toBeInTheDocument()
    expect(bar.getByText('2')).toBeInTheDocument() // 1 error + 1 rate-limit
    expect(bar.getByText('1h 2m')).toBeInTheDocument()
    expect(fetchMock).toHaveBeenCalledWith('http://localhost:8080/api/system/metrics')
  })

  it('shows API DOWN in the health bar when the backend is unreachable', async () => {
    installFetch({ metrics: null })
    render(<App />)

    const bar = within(await screen.findByRole('region', { name: 'System health' }))
    expect(await bar.findByText('DOWN')).toBeInTheDocument()
  })

  it('filters the telemetry feed by callsign and ICAO24 while typing', async () => {
    installFetch({ flights: [AIC860, IGO612] })
    const user = userEvent.setup()
    render(<App />)
    await screen.findByTestId('feed-card-IGO612')
    const input = screen.getByPlaceholderText('Search Callsign or ICAO24 (e.g. AIC860)')

    await user.type(input, 'aic')
    expect(screen.getByTestId('feed-card-AIC860')).toBeInTheDocument()
    expect(screen.queryByTestId('feed-card-IGO612')).not.toBeInTheDocument()
    expect(screen.queryByTestId('marker-IGO612')).not.toBeInTheDocument()

    await user.clear(input)
    await user.type(input, '800B')
    expect(screen.getByTestId('feed-card-IGO612')).toBeInTheDocument()
    expect(screen.queryByTestId('feed-card-AIC860')).not.toBeInTheDocument()

    await user.click(screen.getByRole('button', { name: 'Clear search' }))
    expect(screen.getByTestId('feed-card-AIC860')).toBeInTheDocument()
    expect(screen.getByTestId('feed-card-IGO612')).toBeInTheDocument()
  })

  it('selects an exact live match without calling the lookup API and shows full details', async () => {
    const fetchMock = installFetch({ flights: [AIC860, IGO612] })
    const user = userEvent.setup()
    render(<App />)
    await screen.findByTestId('feed-card-AIC860')

    await user.type(screen.getByRole('textbox', { name: 'Search callsign or ICAO24' }), 'aic860{Enter}')

    const card = within(detailCard())
    expect(card.getByRole('heading', { name: 'AIC860' })).toBeInTheDocument()
    expect(card.getByText('800a01')).toBeInTheDocument()
    expect(card.getByText('ACTIVE')).toBeInTheDocument()
    expect(card.getByText('35,000 ft')).toBeInTheDocument()
    expect(card.getByText('245 m/s')).toBeInTheDocument()
    expect(card.getByText('476 kts')).toBeInTheDocument()
    expect(card.getByText('210°')).toBeInTheDocument()
    expect(card.getByText('SW')).toBeInTheDocument()
    expect(card.getByText('28.5562')).toBeInTheDocument()
    expect(card.getByText('77.1000')).toBeInTheDocument()
    expect(fetchMock.mock.calls.some(([url]) => /\/api\/flights\/aic860/i.test(url))).toBe(false)
    expect(leaflet.map.flyTo).toHaveBeenCalledWith([28.5562, 77.1], 7, { duration: 1.0 })
  })

  it('fetches /api/flights/{id} for a flight that is not live and shows the archived result', async () => {
    const fetchMock = installFetch({
      flights: [AIC860],
      lookups: {
        SIA423: [
          200,
          {
            callsign: 'SIA423',
            icao24: '801108',
            latitude: 19.0896,
            longitude: 72.8656,
            altitude: 12000,
            velocity: 180,
            velocityKnots: 349.9,
            heading: 90,
            timestamp: Date.now() - 3_600_000,
            status: 'HISTORICAL',
            source: 'POSTGRES_ARCHIVE',
          },
        ],
      },
    })
    const user = userEvent.setup()
    render(<App />)
    await screen.findByTestId('feed-card-AIC860')

    await user.type(screen.getByRole('textbox', { name: 'Search callsign or ICAO24' }), 'SIA423')
    await user.click(screen.getByRole('button', { name: 'Search' }))

    const card = within(await screen.findByRole('complementary', { name: 'Flight details for SIA423' }))
    expect(card.getByText('HISTORICAL')).toBeInTheDocument()
    expect(card.getByText('PostgreSQL archive')).toBeInTheDocument()
    expect(card.getByText('12,000 ft')).toBeInTheDocument()
    expect(card.getByText('180 m/s')).toBeInTheDocument()
    expect(card.getByText('350 kts')).toBeInTheDocument()
    expect(card.getByText('90°')).toBeInTheDocument()
    expect(card.getByText('E')).toBeInTheDocument()
    expect(card.getByText('19.0896')).toBeInTheDocument()
    expect(card.getByText('72.8656')).toBeInTheDocument()
    expect(fetchMock).toHaveBeenCalledWith('http://localhost:8080/api/flights/SIA423')
    expect(screen.getByText('Showing last archived position for SIA423.')).toBeInTheDocument()
  })

  it('shows "No flight found" when the lookup API answers 404', async () => {
    installFetch({ flights: [AIC860] })
    const user = userEvent.setup()
    render(<App />)
    await screen.findByTestId('feed-card-AIC860')

    await user.type(screen.getByRole('textbox', { name: 'Search callsign or ICAO24' }), 'ZZZ999{Enter}')

    expect(await screen.findByRole('alert')).toHaveTextContent("No flight found for 'ZZZ999'")
    expect(screen.queryByRole('complementary')).not.toBeInTheDocument()
  })

  it('shows the server validation message when the lookup API answers 400', async () => {
    installFetch({
      lookups: {
        'abc-_-': [400, { status: 400, error: 'INVALID_IDENTIFIER', message: 'Flight identifier is not valid.' }],
      },
    })
    const user = userEvent.setup()
    render(<App />)

    await user.type(screen.getByRole('textbox', { name: 'Search callsign or ICAO24' }), 'abc-_-{Enter}')

    expect(await screen.findByRole('alert')).toHaveTextContent('Flight identifier is not valid.')
  })

  it.each([
    ["' OR 1=1--", "Use only letters, digits, '_' or '-'."],
    ['<script>', "Use only letters, digits, '_' or '-'."],
    ['A'.repeat(33), 'Flight IDs are at most 32 characters.'],
  ])('rejects invalid input %s before calling the API', async (input, message) => {
    const fetchMock = installFetch()
    const user = userEvent.setup()
    render(<App />)
    await waitFor(() => expect(fetchMock).toHaveBeenCalled())
    const before = fetchMock.mock.calls.length

    await user.type(screen.getByRole('textbox', { name: 'Search callsign or ICAO24' }), `${input}{Enter}`)

    expect(await screen.findByRole('alert')).toHaveTextContent(message)
    const lookupCalls = fetchMock.mock.calls.slice(before).filter(([url]) => !/active|status/.test(url))
    expect(lookupCalls).toHaveLength(0)
  })

  it('applies WebSocket telemetry in real time, including inside an open detail card', async () => {
    installFetch({ flights: [AIC860] })
    const user = userEvent.setup()
    render(<App />)
    await user.click(await screen.findByTestId('feed-card-AIC860'))
    expect(within(detailCard()).getByText('28.5562')).toBeInTheDocument()

    act(() => MockWebSocket.latest().open())
    act(() =>
      MockWebSocket.latest().message({ ...AIC860, latitude: 20.1234, longitude: 75.5678, altitude: 36000, velocity: 250, heading: 90 }),
    )

    const card = within(detailCard())
    await waitFor(() => expect(card.getByText('20.1234')).toBeInTheDocument())
    expect(card.getByText('75.5678')).toBeInTheDocument()
    expect(card.getByText('36,000 ft')).toBeInTheDocument()
    expect(card.getByText('Live stream')).toBeInTheDocument()

    act(() => MockWebSocket.latest().message(flight('VTI941', '800c03')))
    await waitFor(() => expect(screen.getByTestId('tracked-count')).toHaveTextContent('2'))
    expect(screen.getByTestId('feed-card-VTI941')).toBeInTheDocument()

    // Malformed frames are ignored.
    act(() => MockWebSocket.latest().message('{not json'))
    expect(screen.getByTestId('tracked-count')).toHaveTextContent('2')
  })

  it('opens the detail card from a map marker click and closes it again', async () => {
    installFetch({ flights: [AIC860] })
    const user = userEvent.setup()
    render(<App />)

    await user.click(await screen.findByTestId('marker-AIC860'))
    expect(detailCard()).toBeInTheDocument()

    await user.click(screen.getByRole('button', { name: 'Center on Aircraft' }))
    expect(leaflet.map.flyTo).toHaveBeenCalledTimes(2)

    await user.click(screen.getByRole('button', { name: 'Close flight details' }))
    expect(screen.queryByRole('complementary')).not.toBeInTheDocument()
  })

  it('collapses and expands the sidebar and toggles the map theme', async () => {
    installFetch({ flights: [AIC860] })
    const user = userEvent.setup()
    const { container } = render(<App />)
    await screen.findByTestId('feed-card-AIC860')

    await user.click(within(sidebar()).getByRole('button', { name: 'Collapse sidebar' }))
    expect(screen.queryByRole('region', { name: 'Flight sidebar' })).not.toBeInTheDocument()
    await user.click(screen.getByRole('button', { name: 'Expand sidebar' }))
    expect(sidebar()).toBeInTheDocument()

    expect(screen.getAllByTestId('tile-layer')[0].dataset.url).toContain('World_Dark_Gray_Base')
    expect(container.firstChild).toHaveAttribute('data-theme', 'dark')
    await user.click(screen.getByRole('button', { name: 'Switch to light map' }))
    expect(screen.getAllByTestId('tile-layer')[0].dataset.url).toContain('World_Light_Gray_Base')
    expect(container.firstChild).toHaveAttribute('data-theme', 'light')
    await user.click(screen.getByRole('button', { name: 'Switch to dark map' }))
    expect(screen.getAllByTestId('tile-layer')[0].dataset.url).toContain('World_Dark_Gray_Base')

    await user.click(screen.getByRole('button', { name: 'Zoom in' }))
    await user.click(screen.getByRole('button', { name: 'Zoom out' }))
    expect(leaflet.map.zoomIn).toHaveBeenCalledTimes(1)
    expect(leaflet.map.zoomOut).toHaveBeenCalledTimes(1)
  })

  it('shows the cold-start banner when the first load is slow, then the reconnect banner', async () => {
    vi.useFakeTimers()
    globalThis.fetch = vi.fn(() => new Promise(() => {})) // server asleep
    render(<App />)

    expect(screen.queryByText('Waking up AeroSync server...')).not.toBeInTheDocument()
    act(() => vi.advanceTimersByTime(2600))
    expect(screen.getByText('Waking up AeroSync server...')).toBeInTheDocument()

    act(() => MockWebSocket.latest().open())
    expect(screen.queryByText('Waking up AeroSync server...')).not.toBeInTheDocument()

    act(() => MockWebSocket.latest().close())
    expect(screen.getByText('Reconnecting to telemetry stream...')).toBeInTheDocument()

    const before = MockWebSocket.instances.length
    act(() => vi.advanceTimersByTime(1000))
    expect(MockWebSocket.instances.length).toBe(before + 1)
    act(() => MockWebSocket.latest().open())
    expect(screen.queryByText('Reconnecting to telemetry stream...')).not.toBeInTheDocument()
  })
})
