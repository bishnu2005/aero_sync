import { useEffect, useState } from 'react'
import { apiUrl, ENDPOINTS } from '../config/api'

export const STATUS_POLL_MS = 30000

/** Polls GET /api/system/status on mount and every 30 s. */
export function useSystemStatus(intervalMs = STATUS_POLL_MS) {
  const [status, setStatus] = useState(null)

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
    load()
    const id = setInterval(load, intervalMs)
    return () => {
      cancelled = true
      clearInterval(id)
    }
  }, [intervalMs])

  return status
}
