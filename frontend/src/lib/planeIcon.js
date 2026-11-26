import L from 'leaflet'

// Nose-up airplane silhouette in a 24x24 box; rotated by heading (degrees clockwise from north).
const PLANE_PATH =
  'M12 1.8c.66 0 1.1.55 1.1 1.3V9l7.4 4.4v2l-7.4-2.2v4.7l2.1 1.6v1.7L12 20.3l-3.2.9v-1.7l2.1-1.6v-4.7l-7.4 2.2v-2L10.9 9V3.1c0-.75.44-1.3 1.1-1.3z'

const cache = new Map()

/** Cached Leaflet divIcon with an inline SVG plane; adapts to the map theme and selection state. */
export function planeIcon(heading = 0, theme = 'dark', selected = false, historical = false) {
  const angle = Math.round((((Number(heading) || 0) % 360) + 360) % 360 / 5) * 5
  const key = `${angle}|${theme}|${selected}|${historical}`
  let icon = cache.get(key)
  if (!icon) {
    const classes = ['plane-marker', `plane-marker--${theme}`]
    if (selected) classes.push('plane-marker--selected')
    if (historical) classes.push('plane-marker--historical')
    icon = L.divIcon({
      className: 'plane-icon-root',
      html: `<div class="${classes.join(' ')}"><span class="plane-ring"></span><svg viewBox="0 0 24 24" width="26" height="26" style="transform: rotate(${angle}deg)" aria-hidden="true"><path d="${PLANE_PATH}"/></svg></div>`,
      iconSize: [34, 34],
      iconAnchor: [17, 17],
    })
    cache.set(key, icon)
  }
  return icon
}
