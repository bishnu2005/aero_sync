export default function ConnectionBanner({ message }) {
  if (!message) return null
  return (
    <div className="glass connection-banner" role="alert">
      <span className="spinner" aria-hidden="true" />
      {message}
    </div>
  )
}
