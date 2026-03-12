import React, { useState, useEffect, useCallback } from 'react';
import './App.css';

interface MeshtasticData {
  INGESTED_AT: string;
  PACKET_TYPE: string;
  FROM_ID: string | null;
  BATTERY_LEVEL: number | null;
  VOLTAGE: number | null;
  TEMPERATURE: number | null;
  TEMPERATURE_F: number | null;
  LATITUDE: number | null;
  LONGITUDE: number | null;
  ALTITUDE: number | null;
  RX_SNR: number | null;
  RX_RSSI: number | null;
  CHANNEL_UTILIZATION: number | null;
  UPTIME_SECONDS: number | null;
  TEXT_MESSAGE: string | null;
  RELATIVE_HUMIDITY: number | null;
  BAROMETRIC_PRESSURE: number | null;
}

interface Stats {
  totalMessages: number;
  uniqueDevices: number;
  latestBattery: number | null;
  latestTemp: number | null;
  avgSnr: number | null;
  positions: number;
}

const REFRESH_INTERVAL = 120000;

const SAMPLE_DATA: MeshtasticData[] = [
  { INGESTED_AT: "2026-03-06 16:52:38.929 +0000", PACKET_TYPE: "telemetry", FROM_ID: "!b9d44b14", BATTERY_LEVEL: 91, VOLTAGE: 4.094, TEMPERATURE: null, TEMPERATURE_F: null, LATITUDE: null, LONGITUDE: null, ALTITUDE: null, RX_SNR: null, RX_RSSI: null, CHANNEL_UTILIZATION: 4.35, UPTIME_SECONDS: 28990, TEXT_MESSAGE: null, RELATIVE_HUMIDITY: null, BAROMETRIC_PRESSURE: null },
  { INGESTED_AT: "2026-03-06 16:52:12.945 +0000", PACKET_TYPE: "telemetry", FROM_ID: "!b9d44b14", BATTERY_LEVEL: null, VOLTAGE: null, TEMPERATURE: 29.47, TEMPERATURE_F: 85.04, LATITUDE: null, LONGITUDE: null, ALTITUDE: null, RX_SNR: null, RX_RSSI: null, CHANNEL_UTILIZATION: null, UPTIME_SECONDS: null, TEXT_MESSAGE: null, RELATIVE_HUMIDITY: null, BAROMETRIC_PRESSURE: null },
  { INGESTED_AT: "2026-03-06 16:52:07.306 +0000", PACKET_TYPE: "nodeinfo", FROM_ID: "!d7dff297", BATTERY_LEVEL: null, VOLTAGE: null, TEMPERATURE: null, TEMPERATURE_F: null, LATITUDE: null, LONGITUDE: null, ALTITUDE: null, RX_SNR: -19.25, RX_RSSI: -109, CHANNEL_UTILIZATION: null, UPTIME_SECONDS: null, TEXT_MESSAGE: null, RELATIVE_HUMIDITY: null, BAROMETRIC_PRESSURE: null },
  { INGESTED_AT: "2026-03-06 16:51:35.627 +0000", PACKET_TYPE: "position", FROM_ID: "!9c0dc996", BATTERY_LEVEL: null, VOLTAGE: null, TEMPERATURE: null, TEMPERATURE_F: null, LATITUDE: 40.6729, LONGITUDE: -73.9643, ALTITUDE: 99, RX_SNR: -14.75, RX_RSSI: -110, CHANNEL_UTILIZATION: null, UPTIME_SECONDS: null, TEXT_MESSAGE: null, RELATIVE_HUMIDITY: null, BAROMETRIC_PRESSURE: null },
  { INGESTED_AT: "2026-03-06 16:49:56.949 +0000", PACKET_TYPE: "position", FROM_ID: "!84b5e0d4", BATTERY_LEVEL: null, VOLTAGE: null, TEMPERATURE: null, TEMPERATURE_F: null, LATITUDE: 41.2533, LONGITUDE: -74.8896, ALTITUDE: 270, RX_SNR: -18.25, RX_RSSI: -111, CHANNEL_UTILIZATION: null, UPTIME_SECONDS: null, TEXT_MESSAGE: null, RELATIVE_HUMIDITY: null, BAROMETRIC_PRESSURE: null },
  { INGESTED_AT: "2026-03-06 16:49:23.199 +0000", PACKET_TYPE: "telemetry", FROM_ID: "!sensor01", BATTERY_LEVEL: null, VOLTAGE: null, TEMPERATURE: 19.77, TEMPERATURE_F: 67.58, LATITUDE: null, LONGITUDE: null, ALTITUDE: null, RX_SNR: -16.25, RX_RSSI: -109, CHANNEL_UTILIZATION: null, UPTIME_SECONDS: null, TEXT_MESSAGE: null, RELATIVE_HUMIDITY: null, BAROMETRIC_PRESSURE: null },
  { INGESTED_AT: "2026-03-06 16:46:25.930 +0000", PACKET_TYPE: "telemetry", FROM_ID: "!9ea3dbcc", BATTERY_LEVEL: 101, VOLTAGE: 4.27, TEMPERATURE: null, TEMPERATURE_F: null, LATITUDE: null, LONGITUDE: null, ALTITUDE: null, RX_SNR: -19.25, RX_RSSI: -110, CHANNEL_UTILIZATION: 4.94, UPTIME_SECONDS: 90119, TEXT_MESSAGE: null, RELATIVE_HUMIDITY: null, BAROMETRIC_PRESSURE: null },
  { INGESTED_AT: "2026-03-06 16:46:20.263 +0000", PACKET_TYPE: "position", FROM_ID: "!5fd97501", BATTERY_LEVEL: null, VOLTAGE: null, TEMPERATURE: null, TEMPERATURE_F: null, LATITUDE: 40.7568, LONGITUDE: -74.0491, ALTITUDE: 50, RX_SNR: -18.25, RX_RSSI: -110, CHANNEL_UTILIZATION: null, UPTIME_SECONDS: null, TEXT_MESSAGE: null, RELATIVE_HUMIDITY: null, BAROMETRIC_PRESSURE: null },
  { INGESTED_AT: "2026-03-06 16:46:20.262 +0000", PACKET_TYPE: "position", FROM_ID: "!433a9874", BATTERY_LEVEL: null, VOLTAGE: null, TEMPERATURE: null, TEMPERATURE_F: null, LATITUDE: 40.8158, LONGITUDE: -73.8984, ALTITUDE: 100, RX_SNR: -17.5, RX_RSSI: -110, CHANNEL_UTILIZATION: null, UPTIME_SECONDS: null, TEXT_MESSAGE: null, RELATIVE_HUMIDITY: null, BAROMETRIC_PRESSURE: null },
  { INGESTED_AT: "2026-03-06 16:46:20.262 +0000", PACKET_TYPE: "telemetry", FROM_ID: "!3b97675b", BATTERY_LEVEL: 101, VOLTAGE: 4.215, TEMPERATURE: null, TEMPERATURE_F: null, LATITUDE: null, LONGITUDE: null, ALTITUDE: null, RX_SNR: -17.5, RX_RSSI: -110, CHANNEL_UTILIZATION: 17.57, UPTIME_SECONDS: 7706631, TEXT_MESSAGE: null, RELATIVE_HUMIDITY: null, BAROMETRIC_PRESSURE: null },
  { INGESTED_AT: "2026-03-06 16:46:17.494 +0000", PACKET_TYPE: "telemetry", FROM_ID: "!b9d44b14", BATTERY_LEVEL: null, VOLTAGE: null, TEMPERATURE: 30.03, TEMPERATURE_F: 86.05, LATITUDE: null, LONGITUDE: null, ALTITUDE: null, RX_SNR: null, RX_RSSI: null, CHANNEL_UTILIZATION: null, UPTIME_SECONDS: null, TEXT_MESSAGE: null, RELATIVE_HUMIDITY: null, BAROMETRIC_PRESSURE: null },
  { INGESTED_AT: "2026-03-06 16:46:15.267 +0000", PACKET_TYPE: "telemetry", FROM_ID: "!4d7e2195", BATTERY_LEVEL: 101, VOLTAGE: 4.191, TEMPERATURE: null, TEMPERATURE_F: null, LATITUDE: null, LONGITUDE: null, ALTITUDE: null, RX_SNR: -15.25, RX_RSSI: -112, CHANNEL_UTILIZATION: 16.18, UPTIME_SECONDS: 13928, TEXT_MESSAGE: null, RELATIVE_HUMIDITY: null, BAROMETRIC_PRESSURE: null },
];

function App() {
  const [data, setData] = useState<MeshtasticData[]>(SAMPLE_DATA);
  const [stats, setStats] = useState<Stats>({ totalMessages: 0, uniqueDevices: 0, latestBattery: null, latestTemp: null, avgSnr: null, positions: 0 });
  const [loading, setLoading] = useState(true);
  const [lastUpdate, setLastUpdate] = useState<Date>(new Date());
  const [countdown, setCountdown] = useState(120);
  const [pacmanPos, setPacmanPos] = useState(0);
  const [useApi, setUseApi] = useState(true);

  const calculateStats = useCallback((records: MeshtasticData[]) => {
    const devices = new Set(records.map(d => d.FROM_ID).filter(Boolean));
    const batteryData = records.find(d => d.BATTERY_LEVEL !== null && d.BATTERY_LEVEL <= 100);
    const tempData = records.find(d => d.TEMPERATURE !== null);
    const snrValues = records.filter(d => d.RX_SNR !== null).map(d => d.RX_SNR as number);
    const positions = records.filter(d => d.PACKET_TYPE === 'position').length;

    return {
      totalMessages: records.length,
      uniqueDevices: devices.size,
      latestBattery: batteryData?.BATTERY_LEVEL || null,
      latestTemp: tempData?.TEMPERATURE || null,
      avgSnr: snrValues.length ? snrValues.reduce((a, b) => a + b, 0) / snrValues.length : null,
      positions
    };
  }, []);

  const fetchData = useCallback(async () => {
    if (!useApi) {
      setStats(calculateStats(SAMPLE_DATA));
      setData(SAMPLE_DATA);
      setLastUpdate(new Date());
      setCountdown(120);
      setLoading(false);
      return;
    }

    try {
      const response = await fetch('/api/meshtastic');
      if (!response.ok) throw new Error('API unavailable');
      const result = await response.json();
      const records = result.data || [];
      
      if (records.length > 0) {
        setData(records);
        setStats(calculateStats(records));
      } else {
        setData(SAMPLE_DATA);
        setStats(calculateStats(SAMPLE_DATA));
      }
    } catch {
      setData(SAMPLE_DATA);
      setStats(calculateStats(SAMPLE_DATA));
    } finally {
      setLastUpdate(new Date());
      setCountdown(120);
      setLoading(false);
    }
  }, [useApi, calculateStats]);

  useEffect(() => {
    fetchData();
    const interval = setInterval(fetchData, REFRESH_INTERVAL);
    return () => clearInterval(interval);
  }, [fetchData]);

  useEffect(() => {
    const timer = setInterval(() => {
      setCountdown(prev => Math.max(0, prev - 1));
    }, 1000);
    return () => clearInterval(timer);
  }, []);

  useEffect(() => {
    const animation = setInterval(() => {
      setPacmanPos(prev => (prev + 1) % 100);
    }, 100);
    return () => clearInterval(animation);
  }, []);

  const formatUptime = (seconds: number | null) => {
    if (!seconds) return '-';
    const days = Math.floor(seconds / 86400);
    const hours = Math.floor((seconds % 86400) / 3600);
    const mins = Math.floor((seconds % 3600) / 60);
    if (days > 0) return `${days}d ${hours}h`;
    return `${hours}h ${mins}m`;
  };

  const formatTime = (timestamp: string) => {
    const clean = timestamp.replace(/"/g, '');
    return new Date(clean).toLocaleString();
  };

  const getPacketIcon = (type: string) => {
    switch (type) {
      case 'telemetry': return '📊';
      case 'position': return '📍';
      case 'text': return '💬';
      case 'nodeinfo': return '📡';
      case 'raw': return '📦';
      default: return '•';
    }
  };

  const getBatteryIcon = (level: number | null) => {
    if (!level) return '⚫';
    if (level > 80) return '🟢';
    if (level > 50) return '🟡';
    if (level > 20) return '🟠';
    return '🔴';
  };

  return (
    <div className="app">
      <div className="maze-border">
        <header className="header">
          <div className="pac-dots">
            {Array(20).fill(0).map((_, i) => (
              <span key={i} className={`dot ${i < pacmanPos % 20 ? 'eaten' : ''}`}>•</span>
            ))}
          </div>
          <div className="title-section">
            <span className="pacman">ᗧ</span>
            <h1>MESHTASTIC DASHBOARD</h1>
            <span className="ghost">👻</span>
          </div>
          <div className="pac-dots reverse">
            {Array(20).fill(0).map((_, i) => (
              <span key={i} className={`dot ${i < (20 - pacmanPos % 20) ? 'eaten' : ''}`}>•</span>
            ))}
          </div>
        </header>

        <div className="status-bar">
          <div className="status-item">
            <span className="cherry">🍒</span>
            <span>Last Update: {lastUpdate.toLocaleTimeString()}</span>
          </div>
          <div className="status-item countdown">
            <span className="power-pellet">●</span>
            <span>Next refresh: {countdown}s</span>
          </div>
          <div className="status-item">
            <span className="fruit">🍊</span>
            <span>Auto-refresh: 2 min</span>
          </div>
          <div className="status-item">
            <button 
              onClick={() => { setUseApi(!useApi); fetchData(); }}
              style={{ background: 'none', border: '1px solid #00ffff', color: '#00ffff', cursor: 'pointer', padding: '4px 8px', borderRadius: '4px', fontFamily: 'inherit', fontSize: '0.5rem' }}
            >
              {useApi ? '🔌 API' : '📁 Demo'}
            </button>
          </div>
        </div>

        {loading ? (
          <div className="loading">
            <div className="loading-pacman">ᗧ••••••</div>
            <p>LOADING DATA...</p>
          </div>
        ) : (
          <>
            <div className="stats-grid">
              <div className="stat-box blue-ghost">
                <div className="stat-icon">👻</div>
                <div className="stat-value">{stats.totalMessages}</div>
                <div className="stat-label">MESSAGES</div>
              </div>
              <div className="stat-box pink-ghost">
                <div className="stat-icon">📡</div>
                <div className="stat-value">{stats.uniqueDevices}</div>
                <div className="stat-label">DEVICES</div>
              </div>
              <div className="stat-box orange-ghost">
                <div className="stat-icon">{getBatteryIcon(stats.latestBattery)}</div>
                <div className="stat-value">{stats.latestBattery ?? '-'}%</div>
                <div className="stat-label">BATTERY</div>
              </div>
              <div className="stat-box red-ghost">
                <div className="stat-icon">🌡️</div>
                <div className="stat-value">{stats.latestTemp?.toFixed(1) ?? '-'}°C</div>
                <div className="stat-label">TEMP</div>
              </div>
            </div>

            <div className="stats-grid" style={{ gridTemplateColumns: 'repeat(2, 1fr)', marginBottom: '20px' }}>
              <div className="stat-box blue-ghost">
                <div className="stat-icon">📍</div>
                <div className="stat-value">{stats.positions}</div>
                <div className="stat-label">POSITIONS</div>
              </div>
              <div className="stat-box pink-ghost">
                <div className="stat-icon">📶</div>
                <div className="stat-value">{stats.avgSnr?.toFixed(1) ?? '-'} dB</div>
                <div className="stat-label">AVG SNR</div>
              </div>
            </div>

            <div className="data-section">
              <h2 className="section-title">
                <span className="dot-line">• • • • •</span>
                RECENT PACKETS
                <span className="dot-line">• • • • •</span>
              </h2>
              
              <div className="data-table">
                <div className="table-header">
                  <span>TYPE</span>
                  <span>DEVICE</span>
                  <span>DATA</span>
                  <span>TIME</span>
                </div>
                {data.slice(0, 15).map((item, index) => (
                  <div key={index} className={`table-row ${index % 2 === 0 ? 'even' : 'odd'}`}>
                    <span className="packet-type">
                      {getPacketIcon(item.PACKET_TYPE)} {item.PACKET_TYPE}
                    </span>
                    <span className="device-id">{item.FROM_ID || 'Unknown'}</span>
                    <span className="packet-data">
                      {item.PACKET_TYPE === 'telemetry' && (
                        <>
                          {item.BATTERY_LEVEL && item.BATTERY_LEVEL <= 100 && <span className="data-pill">{getBatteryIcon(item.BATTERY_LEVEL)} {item.BATTERY_LEVEL}%</span>}
                          {item.TEMPERATURE && <span className="data-pill">🌡️ {item.TEMPERATURE.toFixed(1)}°C</span>}
                          {item.VOLTAGE && <span className="data-pill">⚡ {item.VOLTAGE.toFixed(2)}V</span>}
                          {item.UPTIME_SECONDS && <span className="data-pill">⏱️ {formatUptime(item.UPTIME_SECONDS)}</span>}
                          {item.CHANNEL_UTILIZATION && <span className="data-pill">📊 {item.CHANNEL_UTILIZATION.toFixed(1)}%</span>}
                        </>
                      )}
                      {item.PACKET_TYPE === 'position' && item.LATITUDE && (
                        <>
                          <span className="data-pill">📍 {item.LATITUDE.toFixed(4)}, {item.LONGITUDE?.toFixed(4)}</span>
                          {item.ALTITUDE && <span className="data-pill">⛰️ {item.ALTITUDE}m</span>}
                        </>
                      )}
                      {item.RX_SNR && <span className="data-pill signal">📶 {item.RX_SNR}dB</span>}
                      {item.RX_RSSI && <span className="data-pill signal">📻 {item.RX_RSSI}dBm</span>}
                    </span>
                    <span className="timestamp">{formatTime(item.INGESTED_AT)}</span>
                  </div>
                ))}
              </div>
            </div>

            <div className="maze-footer">
              <div className="ghost-row">
                <span className="ghost blinky">👻</span>
                <span className="ghost pinky">👻</span>
                <span className="ghost inky">👻</span>
                <span className="ghost clyde">👻</span>
              </div>
              <div className="score">HIGH SCORE: {stats.totalMessages * 100}</div>
            </div>
          </>
        )}
      </div>
    </div>
  );
}

export default App;
