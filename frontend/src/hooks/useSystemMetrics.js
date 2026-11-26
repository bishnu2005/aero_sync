import { useEffect, useState } from 'react'
import { apiUrl, ENDPOINTS } from '../config/api'

export const METRICS_POLL_MS = 10000

/** Polls GET /api/system/metrics; `reachable` turns false when the backend stops answering. */
export function useSystemMetrics(intervalMs = METRICS_POLL_MS) {
  const [state, setState] = useState({ metrics: null, reachable: null })

  useEffect(() => {
    let cancelled = false
    const load = async () => {
      try {
        const res = await fetch(apiUrl(ENDPOINTS.systemMetrics))
        if (!res.ok) throw new Error(`HTTP ${res.status}`)
        const metrics = await res.json()
        if (!cancelled) setState({ metrics, reachable: true })
      } catch {
        if (!cancelled) setState((prev) => ({ metrics: prev.metrics, reachable: false }))
      }
    }
    load()
    const id = setInterval(load, intervalMs)
    return () => {
      cancelled = true
      clearInterval(id)
    }
  }, [intervalMs])

  return state
}
