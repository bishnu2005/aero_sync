import { useEffect } from 'react'
import { useMap } from 'react-leaflet'

/** Bridges Leaflet's imperative API: hands the map to the parent and flies to the focus target. */
export default function MapController({ focus, onReady }) {
  const map = useMap()

  useEffect(() => {
    onReady?.(map)
  }, [map, onReady])

  useEffect(() => {
    if (!focus || !Number.isFinite(focus.lat) || !Number.isFinite(focus.lon)) return
    map.flyTo([focus.lat, focus.lon], Math.max(map.getZoom(), 7), { duration: 1.0 })
  }, [map, focus])

  return null
}
