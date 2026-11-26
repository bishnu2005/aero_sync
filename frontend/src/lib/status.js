/** Maps a /api/system/status payload to LIVE_FEED, SIMULATED_FALLBACK or SIMULATED_MANUAL. */
export function feedOf(status) {
  if (status.feed) return status.feed
  if (status.mode === 'LIVE') return 'LIVE_FEED'
  return status.reason === 'MANUAL_SIMULATION_CONFIG' ? 'SIMULATED_MANUAL' : 'SIMULATED_FALLBACK'
}

/** 3725 -> '1h 2m'. */
export function formatUptime(seconds) {
  const s = Math.max(0, Math.floor(Number(seconds) || 0))
  const d = Math.floor(s / 86400)
  const h = Math.floor((s % 86400) / 3600)
  const m = Math.floor((s % 3600) / 60)
  if (d) return `${d}d ${h}h`
  if (h) return `${h}h ${m}m`
  if (m) return `${m}m ${s % 60}s`
  return `${s}s`
}

/**
 * True when a flight belongs to the feed currently shown. Only the simulator tags events SIMULATOR;
 * real OpenSky events (and untagged legacy ones) belong to the live feed. Unknown feed shows everything.
 */
export function matchesFeed(flight, feed) {
  if (!feed) return true
  const simulated = flight?.origin === 'SIMULATOR'
  return feed === 'LIVE_FEED' ? !simulated : simulated
}
