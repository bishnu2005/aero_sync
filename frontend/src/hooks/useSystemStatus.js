import { useCallback, useEffect, useRef, useState } from 'react'
import { apiUrl, ENDPOINTS } from '../config/api'

export const STATUS_POLL_MS = 30000

/**
 * Polls GET /api/system/status on mount and every 30 s. `refresh()` re-checks immediately
 * (throttled to once per 5 s), e.g. when a WebSocket frame from the other feed shows up.
 */
export function useSystemStatus(intervalMs = STATUS_POLL_MS) {
  const [status, setStatus] = useState(null)
  const loadRef = useRef(null)
  const lastRefreshRef = useRef(0)

  const refresh = useCallback(() => {
    const now = Date.now()
    if (now - lastRefreshRef.current < 5000) return
    lastRefreshRef.current = now
    loadRef.current?.()
  }, [])

  useEffect(() => {
    let cancelled = false
    const load = async () => {
      try {
        const res = await fetch(apiUrl(ENDPOINTS.systemStatus))
        if (!res.ok) throw new Error(`HTTP ${res.status}`)
        const body = await res.json()
        if (!cancelled) setStatus(body)
      } catch {
        if (!cancelled) setStatus((prev) => prev ?? { unavailable: true })
      }
    }
    loadRef.current = load
    load()
    const id = setInterval(load, intervalMs)
    return () => {
      cancelled = true
      loadRef.current = null
      clearInterval(id)
    }
  }, [intervalMs])

  return { status, refresh }
}
