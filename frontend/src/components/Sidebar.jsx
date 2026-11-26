import StatusBadge from './StatusBadge'
import { formatNumber } from '../lib/flight'

export default function Sidebar({
  collapsed,
  onToggle,
  status,
  flights,
  totalCount,
  query,
  onQueryChange,
  onSearch,
  onClear,
  searching,
  feedback,
  selectedKey,
  onSelect,
}) {
  if (collapsed) {
    return (
      <button type="button" className="glass sidebar-expand" onClick={onToggle} aria-label="Expand sidebar">
        <span aria-hidden="true">☰</span> AeroSync
      </button>
    )
  }

  const submit = (event) => {
    event.preventDefault()
    onSearch()
  }

  return (
    <section className="glass sidebar" aria-label="Flight sidebar">
      <header className="sidebar-header">
        <div>
          <h1 className="brand">AeroSync</h1>
          <p className="brand-sub">Indian airspace · real-time</p>
        </div>
        <button type="button" className="icon-btn" onClick={onToggle} aria-label="Collapse sidebar">
          ‹
        </button>
      </header>

      <form className="search" role="search" onSubmit={submit}>
        <div className="search-field">
          <input
            type="text"
            value={query}
            onChange={(e) => onQueryChange(e.target.value)}
            placeholder="Search Callsign or ICAO24 (e.g. AIC860)"
            aria-label="Search callsign or ICAO24"
            maxLength={64}
            spellCheck={false}
            autoComplete="off"
          />
          {query && (
            <button type="button" className="search-clear" onClick={onClear} aria-label="Clear search">
              ×
            </button>
          )}
        </div>
        <button type="submit" className="primary-btn search-btn" disabled={searching}>
          {searching ? 'Searching…' : 'Search'}
        </button>
      </form>
      {feedback && (
        <p className={`feedback feedback--${feedback.kind}`} role={feedback.kind === 'error' ? 'alert' : 'status'}>
          {feedback.text}
        </p>
      )}

      <StatusBadge status={status} />

      <div className="stat-card">
        <span className="stat-label">Total Tracked Flights</span>
        <span className="stat-value" data-testid="tracked-count">{totalCount}</span>
      </div>

      <h2 className="section-title">
        Telemetry Feed
        {query.trim() && <span className="section-count">{flights.length} match{flights.length === 1 ? '' : 'es'}</span>}
      </h2>

      <ul className="feed" aria-label="Telemetry feed">
        {flights.length === 0 ? (
          <li className="feed-empty">{totalCount === 0 ? 'Awaiting radar pings…' : 'No flights match your search.'}</li>
        ) : (
          flights.map((flight) => {
            const key = flight.callsign.toUpperCase()
            return (
              <li key={key}>
                <button
                  type="button"
                  className={`feed-card ${selectedKey === key ? 'feed-card--selected' : ''}`}
                  onClick={() => onSelect(flight)}
                  aria-pressed={selectedKey === key}
                  data-testid={`feed-card-${key}`}
                >
                  <span className="feed-row">
                    <strong className="feed-callsign">{flight.callsign}</strong>
                    <span className="feed-speed">{formatNumber(flight.velocity)} m/s</span>
                  </span>
                  <span className="feed-row feed-meta">
                    <span>{flight.icao24}</span>
                    <span>ALT {formatNumber(flight.altitude)} ft</span>
                    <span>HDG {Math.round(Number(flight.heading) || 0)}°</span>
                  </span>
                </button>
              </li>
            )
          })
        )}
      </ul>
    </section>
  )
}
