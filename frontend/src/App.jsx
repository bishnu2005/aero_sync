import React, { useEffect, useState } from 'react';
import { MapContainer, TileLayer, Marker, Popup } from 'react-leaflet';
import L from 'leaflet';
import 'leaflet/dist/leaflet.css';

const createPlaneIcon = (heading) => L.divIcon({
  className: '',
  html: `<div style="transform: rotate(${heading || 0}deg); transition: transform 1s ease-in-out;">
           <img src="https://upload.wikimedia.org/wikipedia/commons/1/17/Plane_icon_nose_up.svg" 
                style="width: 24px; height: 24px;"/>
         </div>`,
  iconSize: [24, 24],
  iconAnchor: [12, 12],
  popupAnchor: [0, -12]
});

export default function App() {
  const [flights, setFlights] = useState({});

  useEffect(() => {
    fetch('http://localhost:8080/api/flights/active')
        .then(res => res.json())
        .then(data => {
          const initialFlights = {};
          data.forEach(flight => {
            initialFlights[flight.callsign] = { ...flight, heading: 45 };
          });
          setFlights(initialFlights);
        })
        .catch(err => console.error("Failed to load initial flights:", err));

    // 2. Open WebSocket for real-time movement
    const ws = new WebSocket('ws://localhost:8080/ws/telemetry');

    ws.onopen = () => console.log('📡 Connected to AeroSync Flight Stream!');

    ws.onmessage = (event) => {
      const telemetry = JSON.parse(event.data);

      setFlights(prev => {
        const prevFlight = prev[telemetry.callsign];
        let heading = prevFlight ? prevFlight.heading : 45;

        if (prevFlight && (prevFlight.latitude !== telemetry.latitude || prevFlight.longitude !== telemetry.longitude)) {
          const dy = telemetry.latitude - prevFlight.latitude;
          const dx = Math.cos((Math.PI / 180) * prevFlight.latitude) * (telemetry.longitude - prevFlight.longitude);
          heading = Math.atan2(dx, dy) * (180 / Math.PI);
        }

        return {
          ...prev,
          [telemetry.callsign]: { ...telemetry, heading }
        };
      });
    };

    return () => {
      if (ws.readyState === WebSocket.OPEN) {
        ws.close();
      }
    };
  }, []);

  const activePlanes = Object.values(flights);

  return (
      <div style={{ position: 'relative', height: '100vh', width: '100vw', backgroundColor: '#f4f4f4', overflow: 'hidden' }}>


        <div style={{
          position: 'absolute',
          top: '24px',
          left: '24px',
          bottom: '24px',
          width: '340px',
          zIndex: 1000, // Keeps it above the Leaflet map layer
          background: 'rgba(15, 23, 42, 0.65)',
          backdropFilter: 'blur(16px)',
          WebkitBackdropFilter: 'blur(16px)',
          border: '1px solid rgba(255, 255, 255, 0.1)',
          borderRadius: '24px',
          color: '#ffffff',
          padding: '24px',
          display: 'flex',
          flexDirection: 'column',
          fontFamily: 'system-ui, -apple-system, sans-serif',
          boxShadow: '0 10px 40px rgba(0, 0, 0, 0.3)'
        }}>


          <div style={{ marginBottom: '24px' }}>
            <h1 style={{ margin: '0 0 4px 0', fontSize: '28px', fontWeight: '700', tracking: '-0.5px' }}>AeroSync</h1>
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px', fontSize: '13px', color: '#94a3b8' }}>
              <span style={{ display: 'inline-block', width: '8px', height: '8px', borderRadius: '50%', backgroundColor: '#10b981', boxShadow: '0 0 8px #10b981' }}></span>
              Live Radar Active
            </div>
          </div>


          <div style={{ background: 'rgba(255, 255, 255, 0.05)', borderRadius: '16px', padding: '16px', marginBottom: '24px', border: '1px solid rgba(255, 255, 255, 0.05)' }}>
            <div style={{ fontSize: '13px', color: '#94a3b8', marginBottom: '4px' }}>Total Tracked Flights</div>
            <div style={{ fontSize: '32px', fontWeight: '800', color: '#00ffcc' }}>{activePlanes.length}</div>
          </div>

          {/* Scrolling Flight List */}
          <h2 style={{ fontSize: '14px', textTransform: 'uppercase', letterSpacing: '1px', color: '#94a3b8', marginBottom: '12px', margin: '0 0 12px 0' }}>Telemetry Feed</h2>

          <div style={{ overflowY: 'auto', flex: 1, paddingRight: '8px', display: 'flex', flexDirection: 'column', gap: '10px' }}>
            {activePlanes.length === 0 ? (
                <div style={{ color: '#94a3b8', fontSize: '14px', fontStyle: 'italic' }}>Awaiting radar pings...</div>
            ) : (
                activePlanes.map(flight => (
                    <div key={flight.callsign} style={{
                      background: 'rgba(255, 255, 255, 0.03)',
                      borderRadius: '12px',
                      padding: '12px',
                      border: '1px solid rgba(255, 255, 255, 0.05)',
                      transition: 'background 0.2s'
                    }}>
                      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '8px' }}>
                        <strong style={{ fontSize: '16px', fontWeight: '600' }}>{flight.callsign}</strong>
                        <span style={{ fontSize: '13px', fontWeight: '600', color: '#38bdf8' }}>{Math.round(flight.velocity)} m/s</span>
                      </div>
                      <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '12px', color: '#94a3b8' }}>
                        <span>ALT: {Math.round(flight.altitude)} ft</span>
                        <span>HDG: {Math.round(flight.heading)}°</span>
                      </div>
                    </div>
                ))
            )}
          </div>
        </div>


        <MapContainer
            center={[21.00, 78.00]}
            zoom={5}
            style={{ height: '100%', width: '100%', zIndex: 1 }}
            zoomControl={false} // Hiding the default Leaflet zoom controls for a cleaner look
        >
          <TileLayer
              url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
              attribution='&copy; OpenStreetMap contributors'
          />

          {activePlanes.map(flight => (
              <Marker
                  key={flight.callsign}
                  position={[flight.latitude, flight.longitude]}
                  icon={createPlaneIcon(flight.heading)}
              >
                <Popup>
                  <strong>{flight.callsign}</strong><br/>
                  Altitude: {flight.altitude} ft<br/>
                  Velocity: {flight.velocity} m/s
                </Popup>
              </Marker>
          ))}
        </MapContainer>
      </div>
  );
}