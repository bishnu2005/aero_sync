const SIMULATION_TITLES = {
  DAILY_QUOTA_EXHAUSTED: 'Simulation Mode — Daily Free-Tier Quota Reached',
  UPSTREAM_RATE_LIMITED_429: 'Simulation Mode — OpenSky Rate Limited (429)',
  UPSTREAM_UNAVAILABLE: 'Simulation Mode — OpenSky Unavailable',
  MANUAL_SIMULATION_CONFIG: 'Simulation Mode — Live Feed Disabled',
}

export default function StatusBadge({ status }) {
  if (!status) {
    return (
      <div className="status-pill status-pill--muted" role="status">
        <span className="status-dot" />
        <span className="status-title">Checking radar status…</span>
      </div>
    )
  }
  if (status.unavailable) {
    return (
      <div className="status-pill status-pill--muted" role="status">
        <span className="status-dot" />
        <span className="status-title">Status unavailable</span>
      </div>
    )
  }

  const live = status.mode === 'LIVE'
  const quota = `${status.requestsUsedToday ?? 0}/${status.dailyLimit ?? 0} polls`
  return (
    <div
      className={`status-pill ${live ? 'status-pill--live' : 'status-pill--sim'}`}
      role="status"
      data-mode={status.mode}
      title={status.message}
    >
      <span className="status-dot" />
      <div className="status-text">
        <span className="status-title">
          {live ? 'Live Radar (OpenSky)' : SIMULATION_TITLES[status.reason] ?? 'Simulation Mode'}
        </span>
        <span className="status-sub">
          {live ? quota : `${quota} · ${status.reason}`}
        </span>
      </div>
    </div>
  )
}
