import { useEffect, useRef, useState } from 'react'

export const MIN_RECONNECT_MS = 1000
export const MAX_RECONNECT_MS = 30000

/** 1s, 2s, 4s ... capped at 30s. */
export const backoffDelay = (attempt) => Math.min(MAX_RECONNECT_MS, MIN_RECONNECT_MS * 2 ** attempt)

/**
 * Keeps a WebSocket open with exponential back-off.
 * Returns 'connecting' (first attempt), 'open', or 'reconnecting'.
 */
export function useTelemetrySocket(url, { onMessage, onOpen } = {}) {
  const [status, setStatus] = useState('connecting')
  const onMessageRef = useRef(onMessage)
  const onOpenRef = useRef(onOpen)

  useEffect(() => {
    onMessageRef.current = onMessage
    onOpenRef.current = onOpen
  })

  useEffect(() => {
    let socket
    let timer
    let attempt = 0
    let disposed = false

    const scheduleReconnect = () => {
      if (disposed) return
      setStatus('reconnecting')
      timer = setTimeout(connect, backoffDelay(attempt))
      attempt += 1
    }

    function connect() {
      if (disposed) return
      try {
        socket = new WebSocket(url)
      } catch {
        scheduleReconnect()
        return
      }
      socket.onopen = () => {
        attempt = 0
        setStatus('open')
        onOpenRef.current?.()
      }
      socket.onmessage = (event) => {
        try {
          const payload = JSON.parse(event.data)
          if (payload && payload.callsign) onMessageRef.current?.(payload)
        } catch {
          // ignore malformed frames
        }
      }
      socket.onerror = () => socket.close()
      socket.onclose = scheduleReconnect
    }

    connect()
    return () => {
      disposed = true
      clearTimeout(timer)
      if (socket) {
        socket.onclose = null
        socket.close()
      }
    }
  }, [url])

  return status
}
