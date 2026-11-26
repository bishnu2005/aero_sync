// Default basemaps are Esri's keyless Canvas tiles (dark / light gray base + a labels overlay).
// CARTO Dark Matter / Positron now serve "API KEY REQUIRED" watermark tiles without a key, so they are
// opt-in: set VITE_MAP_TILE_DARK / VITE_MAP_TILE_LIGHT (e.g. a keyed CARTO or Stadia URL) at build time.

const ESRI = 'https://server.arcgisonline.com/ArcGIS/rest/services/Canvas'
const ESRI_ATTRIBUTION =
  'Tiles &copy; <a href="https://www.esri.com">Esri</a> &mdash; Esri, HERE, Garmin, &copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
const CUSTOM_ATTRIBUTION = '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'

export function getTileThemes(env = import.meta.env) {
  return {
    dark: env.VITE_MAP_TILE_DARK
      ? { url: env.VITE_MAP_TILE_DARK, labelsUrl: null, attribution: CUSTOM_ATTRIBUTION }
      : {
          url: `${ESRI}/World_Dark_Gray_Base/MapServer/tile/{z}/{y}/{x}`,
          labelsUrl: `${ESRI}/World_Dark_Gray_Reference/MapServer/tile/{z}/{y}/{x}`,
          attribution: ESRI_ATTRIBUTION,
        },
    light: env.VITE_MAP_TILE_LIGHT
      ? { url: env.VITE_MAP_TILE_LIGHT, labelsUrl: null, attribution: CUSTOM_ATTRIBUTION }
      : {
          url: `${ESRI}/World_Light_Gray_Base/MapServer/tile/{z}/{y}/{x}`,
          labelsUrl: `${ESRI}/World_Light_Gray_Reference/MapServer/tile/{z}/{y}/{x}`,
          attribution: ESRI_ATTRIBUTION,
        },
  }
}

export const TILE_THEMES = getTileThemes()
