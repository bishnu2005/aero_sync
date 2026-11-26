// Resolves backend URLs for every topology:
//  - VITE_API_BASE_URL / VITE_WS_URL set at build time (Vercel / Netlify -> Render / Koyeb)
//  - `npm run dev` -> http://localhost:8080
//  - production build behind the bundled Nginx proxy -> same origin

const trimSlash = (url) => url.replace(/\/+$/, '')

export function getApiBaseUrl(env = import.meta.env) {
  if (env.VITE_API_BASE_URL) return trimSlash(env.VITE_API_BASE_URL)
  if (env.DEV) return 'http://localhost:8080'
  return ''
}

export function getWsUrl(env = import.meta.env, location = globalThis.location) {
  if (env.VITE_WS_URL) return env.VITE_WS_URL
  if (env.VITE_API_BASE_URL) {
    const base = trimSlash(env.VITE_API_BASE_URL)
      .replace(/^https:\/\//i, 'wss://')
      .replace(/^http:\/\//i, 'ws://')
    return `${base}/ws/telemetry`
  }
  if (env.DEV) return 'ws://localhost:8080/ws/telemetry'
  const protocol = location?.protocol === 'https:' ? 'wss:' : 'ws:'
  return `${protocol}//${location?.host}/ws/telemetry`
}

export const apiUrl = (path) => `${getApiBaseUrl()}${path}`

export const ENDPOINTS = {
  activeFlights: '/api/flights/active',
  systemStatus: '/api/system/status',
  systemMetrics: '/api/system/metrics',
  flight: (identifier) => `/api/flights/${encodeURIComponent(identifier)}`,
}
