import { useSystemMetrics } from '../hooks/useSystemMetrics'
import { formatUptime } from '../lib/status'

function Stat({ label, value, tone }) {
  return (
    <span className={`health-stat ${tone ? `health-stat--${tone}` : ''}`}>
      <span className="health-label">{label}</span>
      <span className="health-value">{value}</span>
    </span>
  )
}

/** Live pipeline health from GET /api/system/metrics (backed by Micrometer counters). */
export default function SystemHealthBar() {
  const { metrics, reachable } = useSystemMetrics()
  if (reachable === null) return null

  const m = metrics ?? {}
  const hitRatio = m.cacheHitRatio == null ? '—' : `${Math.round(m.cacheHitRatio * 100)}%`
  const upstreamIssues = (m.upstreamErrors ?? 0) + (m.upstreamRateLimited ?? 0)

  return (
    <div className="health-bar-wrap">
      <div className="glass health-bar" role="region" aria-label="System health">
        <Stat label="API" value={reachable ? 'UP' : 'DOWN'} tone={reachable ? 'ok' : 'bad'} />
        {metrics && (
          <>
            <Stat label="Events/s (60s avg)" value={(m.eventsPerSecond ?? 0).toFixed(1)} />
            <Stat label="Processed" value={(m.eventsConsumed ?? 0).toLocaleString('en-US')} />
            <Stat label="Cache hit" value={hitRatio} />
            <Stat label="WS clients" value={m.websocketSubscribers ?? 0} />
            <Stat label="Upstream errors" value={upstreamIssues} tone={upstreamIssues > 0 ? 'warn' : undefined} />
            <Stat label="Uptime" value={formatUptime(m.uptimeSeconds)} />
          </>
        )}
      </div>
    </div>
  )
}
