import { feedOf } from '../lib/status'

// Honest data-source badge: real OpenSky data is "LIVE FEED"; anything synthetic says so.
const FALLBACK_REASONS = {
  DAILY_QUOTA_EXHAUSTED: (s) => `Daily OpenSky quota reached (${s.requestsUsedToday}/${s.dailyLimit})`,
  UPSTREAM_RATE_LIMITED_429: () => 'OpenSky rate-limited the server (HTTP 429)',
  UPSTREAM_UNAVAILABLE: () => 'OpenSky API unreachable',
}

export default function StatusBadge({ status }) {
  if (!status || status.unavailable) {
    return (
      <div className="status-pill status-pill--muted" role="status">
        <span className="status-dot" />
        <span className="status-title">{status ? 'Status unavailable' : 'Checking radar status…'}</span>
      </div>
    )
  }

  const feed = feedOf(status)
  const live = feed === 'LIVE_FEED'
  let title = 'LIVE FEED'
  let sub = `OpenSky Network · ${status.requestsUsedToday}/${status.dailyLimit} polls today`
  if (feed === 'SIMULATED_FALLBACK') {
    title = 'SIMULATED FALLBACK'
    sub = `${(FALLBACK_REASONS[status.reason] ?? (() => 'Live feed unavailable'))(status)} · synthetic flights`
  } else if (feed === 'SIMULATED_MANUAL') {
    title = 'SIMULATED FEED'
    sub = 'Live feed disabled by configuration · synthetic flights'
  }

  return (
    <div
      className={`status-pill ${live ? 'status-pill--live' : 'status-pill--sim'}`}
      role="status"
      data-feed={feed}
      title={status.message}
    >
      <span className="status-dot" />
      <div className="status-text">
        <span className="status-title">{title}</span>
        <span className="status-sub">{sub}</span>
      </div>
    </div>
  )
}
