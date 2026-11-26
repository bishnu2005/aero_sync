export default function MapControls({ theme, onToggleTheme, onZoomIn, onZoomOut }) {
  const next = theme === 'dark' ? 'light' : 'dark'
  return (
    <div className="map-controls">
      <button
        type="button"
        className="glass control-btn"
        onClick={onToggleTheme}
        aria-label={`Switch to ${next} map`}
        title={`Switch to ${next} map`}
      >
        {theme === 'dark' ? '☀' : '☾'}
      </button>
      <div className="glass zoom-stack">
        <button type="button" className="control-btn" onClick={onZoomIn} aria-label="Zoom in">+</button>
        <button type="button" className="control-btn" onClick={onZoomOut} aria-label="Zoom out">−</button>
      </div>
    </div>
  )
}
