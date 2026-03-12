const express = require('express');
const snowflake = require('snowflake-sdk');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3001;

const connectionName = process.env.SNOWFLAKE_CONNECTION_NAME || 'tspann1';

let connection = null;

function getConnection() {
  return new Promise((resolve, reject) => {
    if (connection && connection.isUp()) {
      resolve(connection);
      return;
    }
    
    snowflake.configure({ insecureConnect: true });
    
    connection = snowflake.createConnection({
      account: process.env.SNOWFLAKE_ACCOUNT,
      username: process.env.SNOWFLAKE_USER,
      password: process.env.SNOWFLAKE_PASSWORD,
      authenticator: 'EXTERNALBROWSER',
      database: 'DEMO',
      schema: 'DEMO',
      warehouse: 'INGEST'
    });

    connection.connect((err, conn) => {
      if (err) {
        console.error('Failed to connect to Snowflake:', err.message);
        reject(err);
      } else {
        console.log('Connected to Snowflake');
        resolve(conn);
      }
    });
  });
}

app.use(express.static(path.join(__dirname, '../build')));

app.get('/api/meshtastic', async (req, res) => {
  try {
    const conn = await getConnection();
    
    conn.execute({
      sqlText: `
        SELECT * FROM DEMO.DEMO.MESHTASTIC_DATA 
        ORDER BY INGESTED_AT DESC 
        LIMIT 50
      `,
      complete: (err, stmt, rows) => {
        if (err) {
          console.error('Query error:', err.message);
          res.status(500).json({ error: err.message });
        } else {
          res.json({ data: rows, count: rows.length, timestamp: new Date().toISOString() });
        }
      }
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.get('/api/stats', async (req, res) => {
  try {
    const conn = await getConnection();
    
    conn.execute({
      sqlText: `
        SELECT 
          COUNT(*) as total_messages,
          COUNT(DISTINCT FROM_ID) as unique_devices,
          MAX(BATTERY_LEVEL) as max_battery,
          AVG(TEMPERATURE) as avg_temp,
          AVG(RX_SNR) as avg_snr,
          MAX(INGESTED_AT) as last_message
        FROM DEMO.DEMO.MESHTASTIC_DATA
        WHERE INGESTED_AT > DATEADD(hour, -24, CURRENT_TIMESTAMP())
      `,
      complete: (err, stmt, rows) => {
        if (err) {
          res.status(500).json({ error: err.message });
        } else {
          res.json({ stats: rows[0], timestamp: new Date().toISOString() });
        }
      }
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, '../build', 'index.html'));
});

app.listen(PORT, () => {
  console.log(`🎮 Pac-Man Meshtastic Dashboard server running on port ${PORT}`);
  console.log(`🕹️  Open http://localhost:${PORT} in your browser`);
});
