const http = require('http');
const fs = require('fs');
const path = require('path');
const os = require('os');
const { exec } = require('child_process');
const { DatabaseSync } = require('node:sqlite');

const PORT = process.env.PORT || 8080;
const ROOT_DIR = __dirname;
const DATA_DIR = path.join(ROOT_DIR, 'data');
const BACKUPS_DIR = path.join(ROOT_DIR, 'backups');

// Ensure directories exist
if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
if (!fs.existsSync(BACKUPS_DIR)) fs.mkdirSync(BACKUPS_DIR, { recursive: true });

const DB_FILE = path.join(DATA_DIR, 'automation.db');
const JSON_STORE_FILE = path.join(DATA_DIR, 'automation_store.json');

// Initialize SQLite database
const db = new DatabaseSync(DB_FILE);
db.exec(`
  CREATE TABLE IF NOT EXISTS system_store (
    collection TEXT PRIMARY KEY,
    data_json TEXT NOT NULL,
    updated_at TEXT NOT NULL
  );
  CREATE TABLE IF NOT EXISTS audit_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    action TEXT NOT NULL,
    user TEXT,
    details TEXT,
    created_at TEXT NOT NULL
  );
`);

// SSE (Server-Sent Events) clients
const sseClients = new Set();

function broadcastSSE(event, data) {
  const payload = `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
  for (const client of sseClients) {
    try {
      client.write(payload);
    } catch (e) {
      sseClients.delete(client);
    }
  }
}

// Heartbeat for SSE connections
setInterval(() => {
  for (const client of sseClients) {
    try {
      client.write(':keepalive\n\n');
    } catch (e) {
      sseClients.delete(client);
    }
  }
}, 15000);

// Load all collections into memory
function getAllData() {
  const stmt = db.prepare('SELECT collection, data_json, updated_at FROM system_store');
  const rows = stmt.all();
  const result = {};
  for (const row of rows) {
    try {
      result[row.collection] = JSON.parse(row.data_json);
    } catch (e) {
      result[row.collection] = null;
    }
  }
  return result;
}

// Save or merge data
function saveCollectionData(collection, data, user = 'System') {
  const dataJson = typeof data === 'string' ? data : JSON.stringify(data);
  const now = new Date().toISOString();
  
  const stmt = db.prepare('INSERT OR REPLACE INTO system_store (collection, data_json, updated_at) VALUES (?, ?, ?)');
  stmt.run(collection, dataJson, now);

  const logStmt = db.prepare('INSERT INTO audit_logs (action, user, details, created_at) VALUES (?, ?, ?, ?)');
  logStmt.run('UPDATE_COLLECTION', user, `Updated collection: ${collection}`, now);

  // Write snapshot to JSON store for human inspection / recovery
  try {
    const all = getAllData();
    fs.writeFileSync(JSON_STORE_FILE, JSON.stringify(all, null, 2), 'utf8');
  } catch (err) {
    console.error('[WARN] Failed to write JSON store snapshot:', err.message);
  }

  return now;
}

const MIME_TYPES = {
  '.html': 'text/html; charset=utf-8',
  '.htm': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.jpeg': 'image/jpeg',
  '.gif': 'image/gif',
  '.svg': 'image/svg+xml',
  '.ico': 'image/x-icon',
  '.xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
  '.csv': 'text/csv; charset=utf-8'
};

function getLocalIp() {
  const interfaces = os.networkInterfaces();
  let wifiIp = null;
  let fallbackIp = null;

  for (const name of Object.keys(interfaces)) {
    for (const iface of interfaces[name]) {
      if (iface.family === 'IPv4' && !iface.internal) {
        if (name.toLowerCase().includes('wi-fi') || name.toLowerCase().includes('wireless')) {
          wifiIp = iface.address;
        } else if (!fallbackIp) {
          fallbackIp = iface.address;
        }
      }
    }
  }
  return wifiIp || fallbackIp || 'localhost';
}

function parseBody(req) {
  return new Promise((resolve, reject) => {
    let body = '';
    req.on('data', chunk => {
      body += chunk;
      if (body.length > 50 * 1024 * 1024) { // 50MB limit
        reject(new Error('Payload too large'));
      }
    });
    req.on('end', () => {
      try {
        resolve(body ? JSON.parse(body) : {});
      } catch (err) {
        reject(err);
      }
    });
    req.on('error', reject);
  });
}

const server = http.createServer(async (req, res) => {
  // CORS headers
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');

  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    res.end();
    return;
  }

  const parsedUrl = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
  const pathname = decodeURI(parsedUrl.pathname);

  // ==========================================
  // API: REAL-TIME SERVER-SENT EVENTS STREAM
  // ==========================================
  if (pathname === '/api/stream' && req.method === 'GET') {
    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      'Connection': 'keep-alive',
      'Access-Control-Allow-Origin': '*'
    });
    res.write('retry: 3000\n\n');
    res.write(`event: connected\ndata: ${JSON.stringify({ clientCount: sseClients.size + 1, time: new Date().toISOString() })}\n\n`);
    
    sseClients.add(res);
    broadcastSSE('clients_count', { count: sseClients.size });

    req.on('close', () => {
      sseClients.delete(res);
      broadcastSSE('clients_count', { count: sseClients.size });
    });
    return;
  }

  // ==========================================
  // API: SERVER STATUS & HEALTH
  // ==========================================
  if (pathname === '/api/status' && req.method === 'GET') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    let dbSize = 0;
    try { dbSize = fs.statSync(DB_FILE).size; } catch (_) {}
    res.end(JSON.stringify({
      status: 'online',
      serverTime: new Date().toISOString(),
      connectedDevices: sseClients.size,
      dbSizeBytes: dbSize,
      nodeVersion: process.version,
      platform: process.platform
    }));
    return;
  }

  // ==========================================
  // API: GET FULL DATABASE OR SINGLE COLLECTION
  // ==========================================
  if (pathname === '/api/data' && req.method === 'GET') {
    const collName = parsedUrl.searchParams.get('collection');
    if (collName) {
      const stmt = db.prepare('SELECT data_json, updated_at FROM system_store WHERE collection = ?');
      const row = stmt.get(collName);
      res.writeHead(200, { 'Content-Type': 'application/json' });
      if (row) {
        res.end(JSON.stringify({ collection: collName, data: JSON.parse(row.data_json), updatedAt: row.updated_at }));
      } else {
        res.end(JSON.stringify({ collection: collName, data: null, updatedAt: null }));
      }
      return;
    }

    const allData = getAllData();
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ success: true, data: allData, timestamp: new Date().toISOString() }));
    return;
  }

  // ==========================================
  // API: SYNC / SAVE DATA (FROM ANY DEVICE)
  // ==========================================
  if (pathname === '/api/sync' && req.method === 'POST') {
    try {
      const payload = await parseBody(req);
      const user = payload.user || 'Unknown User';
      const updatedKeys = [];

      // Support multi-collection sync (full bundle) or single collection update
      if (payload.collections && typeof payload.collections === 'object') {
        for (const [key, value] of Object.entries(payload.collections)) {
          if (value !== undefined) {
            saveCollectionData(key, value, user);
            updatedKeys.push(key);
          }
        }
      } else if (payload.collection && payload.data !== undefined) {
        saveCollectionData(payload.collection, payload.data, user);
        updatedKeys.push(payload.collection);
      } else {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ success: false, error: 'Invalid payload. Provide collection/data or collections map.' }));
        return;
      }

      // Broadcast real-time update to all other connected phones and PCs
      broadcastSSE('data_updated', {
        collections: updatedKeys,
        updatedBy: user,
        timestamp: new Date().toISOString()
      });

      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        success: true,
        updatedCollections: updatedKeys,
        timestamp: new Date().toISOString()
      }));
    } catch (err) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ success: false, error: err.message }));
    }
    return;
  }

  // ==========================================
  // API: BACKUP ENGINE
  // ==========================================
  if (pathname === '/api/backup' && req.method === 'POST') {
    try {
      const allData = getAllData();
      const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
      const backupFilename = `backup_${timestamp}.json`;
      const backupPath = path.join(BACKUPS_DIR, backupFilename);
      fs.writeFileSync(backupPath, JSON.stringify(allData, null, 2), 'utf8');

      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ success: true, filename: backupFilename, timestamp }));
    } catch (err) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ success: false, error: err.message }));
    }
    return;
  }

  // ==========================================
  // STATIC FILE SERVING
  // ==========================================
  let reqPath = pathname;
  if (reqPath === '/' || reqPath === '') {
    reqPath = fs.existsSync(path.join(ROOT_DIR, 'index.html')) ? '/index.html' : '/Index.html';
  }

  let safePath = path.normalize(path.join(ROOT_DIR, reqPath));
  if (!fs.existsSync(safePath) && reqPath.toLowerCase() === '/index.html') {
    safePath = path.normalize(path.join(ROOT_DIR, 'Index.html'));
  }
  if (!safePath.startsWith(ROOT_DIR)) {
    res.writeHead(403, { 'Content-Type': 'text/plain' });
    res.end('403 Forbidden');
    return;
  }

  fs.stat(safePath, (err, stats) => {
    if (err || !stats.isFile()) {
      res.writeHead(404, { 'Content-Type': 'text/plain' });
      res.end('404 Not Found');
      return;
    }

    const ext = path.extname(safePath).toLowerCase();
    const contentType = MIME_TYPES[ext] || 'application/octet-stream';

    res.writeHead(200, {
      'Content-Type': contentType,
      'Content-Length': stats.size,
      'Cache-Control': 'no-cache'
    });

    const stream = fs.createReadStream(safePath);
    stream.pipe(res);
  });
});

server.listen(PORT, '0.0.0.0', () => {
  const localIp = getLocalIp();
  console.log('\n============================================================');
  console.log('   P&A DISTRIBUTORS - OFFICE AUTOMATION ENTERPRISE SERVER');
  console.log('============================================================\n');
  console.log(` > On this computer:`);
  console.log(`   http://localhost:${PORT}`);
  console.log(`   http://${localIp}:${PORT}\n`);
  console.log(` > On any Mobile Phone or other PC on the SAME WI-FI:`);
  console.log(`   \x1b[32m\x1b[1mhttp://${localIp}:${PORT}\x1b[0m\n`);
  console.log(' > Real-time Database: Active (node:sqlite + JSON Sync)');
  console.log(' > Multi-Device Event Stream: /api/stream');
  console.log('------------------------------------------------------------');
  console.log(' Instructions for Mobile Devices:');
  console.log(' 1. Connect your phone / tablet to the same Wi-Fi network.');
  console.log(` 2. Open Chrome or Safari and enter: http://${localIp}:${PORT}`);
  console.log(' 3. Leave this terminal window OPEN while using the app.');
  console.log('    To stop the server, press Ctrl + C.\n');
  console.log('============================================================\n');

  // Open browser on host PC if not in headless/cloud environment
  const isCloudOrHeadless = process.env.NODE_ENV === 'production' || process.env.RENDER || process.env.RAILWAY_ENVIRONMENT || process.env.PORT;
  if (process.env.AUTO_OPEN_BROWSER !== 'false' && !isCloudOrHeadless) {
    const startCmd = process.platform === 'win32' ? `start http://localhost:${PORT}` :
                     process.platform === 'darwin' ? `open http://localhost:${PORT}` :
                     `xdg-open http://localhost:${PORT}`;
    exec(startCmd, () => {});
  }
});

server.on('error', (e) => {
  if (e.code === 'EADDRINUSE') {
    console.error(`\n[ERROR] Port ${PORT} is already in use! Close any other server or choose a different port.\n`);
  } else {
    console.error('[ERROR]', e);
  }
});
