import { cardinal, formatCoord, formatLastContact, formatNumber, toKnots } from '../lib/flight'

const SOURCE_LABELS = {
  LIVE_STREAM: 'Live stream',
  VALKEY_CACHE: 'Valkey cache',
  POSTGRES_ARCHIVE: 'PostgreSQL archive',
}

function Metric({ label, value, sub }) {
  return (
    <div className="metric">
      <span className="metric-label">{label}</span>
      <span className="metric-value">{value}</span>
      {sub && <span className="metric-sub">{sub}</span>}
    </div>
  )
}

export default function FlightDetailCard({ flight, onClose, onCenter }) {
  if (!flight) return null
  const status = flight.status ?? 'ACTIVE'
  const heading = Number(flight.heading) || 0

  return (
    <aside className="glass detail-card" aria-label={`Flight details for ${flight.callsign}`}>
      <header className="detail-header">
        <div>
          <h2 className="detail-callsign">{flight.callsign}</h2>
          <div className="detail-tags">
            <span className="badge badge--mono" title="ICAO24 transponder address">{flight.icao24}</span>
            <span className={`badge ${status === 'ACTIVE' ? 'badge--active' : 'badge--historical'}`}>{status}</span>
          </div>
        </div>
        <button type="button" className="icon-btn" onClick={onClose} aria-label="Close flight details">
          ×
        </button>
      </header>

      <div className="detail-source">
        Source: <strong>{SOURCE_LABELS[flight.source] ?? 'Live stream'}</strong>
        {' · '}Origin: <strong>{flight.origin === 'SIMULATOR' ? 'Simulator (synthetic)' : 'OpenSky Network'}</strong>
      </div>

      <div className="metric-grid">
        <Metric label="Altitude" value={`${formatNumber(flight.altitude)} ft`} />
        <Metric
          label="Speed"
          value={`${formatNumber(flight.velocity)} m/s`}
          sub={`${formatNumber(flight.velocityKnots ?? toKnots(flight.velocity))} kts`}
        />
        <Metric label="Heading" value={`${Math.round(heading)}°`} sub={cardinal(heading)} />
        <Metric label="Latitude" value={formatCoord(flight.latitude)} />
        <Metric label="Longitude" value={formatCoord(flight.longitude)} />
        <Metric label="Last Contact" value={formatLastContact(flight.timestamp)} />
      </div>

      <button type="button" className="primary-btn" onClick={() => onCenter(flight)}>
        Center on Aircraft
      </button>
    </aside>
  )
}
