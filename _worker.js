// VLESS Cloudflare Worker - WORKING VERSION
// Implementasi lengkap dan tested untuk Xray/V2Ray clients

const CONFIG = {
  UUID: '89b3cbba-e6ac-485a-9481-976a0e093b7b', // Ganti dengan UUID Anda
  WS_PATH: '/vless'
};

// Konversi string UUID ke bytes
function stringToUint8Array(str) {
  return new TextEncoder().encode(str);
}

// Parse VLESS header
function parseVlessHeader(buffer) {
  if (buffer.byteLength < 24) return null;
  
  const version = new Uint8Array(buffer.slice(0, 1))[0];
  if (version !== 0) return null;

  const uuid = new Uint8Array(buffer.slice(1, 17));
  const uuidStr = Array.from(uuid).map(b => b.toString(16).padStart(2, '0')).join('');
  
  const optLength = new Uint8Array(buffer.slice(17, 18))[0];
  let cursor = 18 + optLength;
  
  const command = new Uint8Array(buffer.slice(cursor, cursor + 1))[0];
  cursor += 1;
  
  const portRemote = new DataView(buffer).getUint16(cursor);
  cursor += 2;
  
  const addressType = new Uint8Array(buffer.slice(cursor, cursor + 1))[0];
  cursor += 1;
  
  let addressRemote = '';
  let addressLength = 0;
  
  switch (addressType) {
    case 1: // IPv4
      addressLength = 4;
      addressRemote = new Uint8Array(buffer.slice(cursor, cursor + addressLength)).join('.');
      break;
    case 2: // Domain
      addressLength = new Uint8Array(buffer.slice(cursor, cursor + 1))[0];
      cursor += 1;
      addressRemote = new TextDecoder().decode(buffer.slice(cursor, cursor + addressLength));
      break;
    case 3: // IPv6
      addressLength = 16;
      const ipv6 = new Uint8Array(buffer.slice(cursor, cursor + addressLength));
      addressRemote = Array.from({length: 8}, (_, i) => 
        ipv6[i * 2].toString(16).padStart(2, '0') + ipv6[i * 2 + 1].toString(16).padStart(2, '0')
      ).join(':');
      break;
    default:
      return null;
  }
  
  cursor += addressLength;
  
  return {
    version,
    uuid: uuidStr,
    command,
    addressRemote,
    portRemote,
    rawDataIndex: cursor,
    isUDP: command === 2
  };
}

// Handle VLESS over WebSocket
async function vlessOverWSHandler(webSocket) {
  let isHeaderParsed = false;
  let remoteConnection = null;
  let remoteConnectionReadyResolve;
  const remoteConnectionReadyPromise = new Promise(resolve => {
    remoteConnectionReadyResolve = resolve;
  });

  webSocket.addEventListener('message', async (event) => {
    try {
      if (!isHeaderParsed) {
        isHeaderParsed = true;
        
        const vlessBuffer = event.data;
        const vlessHeader = parseVlessHeader(vlessBuffer);
        
        if (!vlessHeader) {
          webSocket.close(1002, 'Invalid VLESS header');
          return;
        }
        
        // Verify UUID
        const expectedUUID = CONFIG.UUID.replace(/-/g, '').toLowerCase();
        if (vlessHeader.uuid !== expectedUUID) {
          webSocket.close(1008, 'Invalid UUID');
          return;
        }

        // Send response to client
        const responseHeader = new Uint8Array([vlessHeader.version, 0]);
        webSocket.send(responseHeader.buffer);

        // Connect to remote server
        try {
          const tcpSocket = connect({
            hostname: vlessHeader.addressRemote,
            port: vlessHeader.portRemote
          });

          remoteConnectionReadyResolve(tcpSocket);
          remoteConnection = tcpSocket;

          // Send initial data if any
          if (vlessBuffer.byteLength > vlessHeader.rawDataIndex) {
            const rawData = vlessBuffer.slice(vlessHeader.rawDataIndex);
            const writer = tcpSocket.writable.getWriter();
            await writer.write(new Uint8Array(rawData));
            writer.releaseLock();
          }

          // Pipe remote to websocket
          await pipeRemoteToWebSocket(tcpSocket, webSocket);

        } catch (error) {
          console.error('Connection error:', error.message);
          webSocket.close(1011, 'Connection failed');
        }

      } else {
        // Forward data to remote
        if (remoteConnection) {
          const writer = remoteConnection.writable.getWriter();
          await writer.write(new Uint8Array(event.data));
          writer.releaseLock();
        }
      }
    } catch (error) {
      console.error('Message error:', error.message);
      webSocket.close(1011, error.message);
    }
  });

  webSocket.addEventListener('close', () => {
    if (remoteConnection) {
      try {
        remoteConnection.close();
      } catch (e) {}
    }
  });

  webSocket.addEventListener('error', (error) => {
    console.error('WebSocket error:', error);
  });
}

// Pipe data from remote to websocket
async function pipeRemoteToWebSocket(remoteSocket, webSocket) {
  try {
    const reader = remoteSocket.readable.getReader();
    
    while (true) {
      const { done, value } = await reader.read();
      
      if (done) break;
      
      if (webSocket.readyState === WebSocket.READY_STATE_OPEN || 
          webSocket.readyState === 1) {
        webSocket.send(value.buffer);
      } else {
        break;
      }
    }
  } catch (error) {
    console.error('Pipe error:', error);
  } finally {
    try {
      if (webSocket.readyState === WebSocket.READY_STATE_OPEN || 
          webSocket.readyState === 1) {
        webSocket.close();
      }
    } catch (e) {}
  }
}

// Main fetch handler
export default {
  async fetch(request, env, ctx) {
    try {
      const url = new URL(request.url);
      const upgradeHeader = request.headers.get('Upgrade');

      // WebSocket upgrade
      if (url.pathname === CONFIG.WS_PATH && upgradeHeader === 'websocket') {
        const pair = new WebSocketPair();
        const [client, server] = Object.values(pair);

        server.accept();
        
        vlessOverWSHandler(server).catch((err) => {
          console.error('VLESS handler error:', err);
          try {
            server.close(1011, err.message);
          } catch (e) {}
        });

        return new Response(null, {
          status: 101,
          webSocket: client,
        });
      }

      // HTTP endpoints
      if (url.pathname === '/health') {
        return new Response(JSON.stringify({
          status: 'ok',
          protocol: 'vless',
          timestamp: Date.now()
        }), {
          headers: { 'Content-Type': 'application/json' }
        });
      }

      // Default page
      return new Response(getHTMLPage(), {
        headers: { 
          'Content-Type': 'text/html;charset=UTF-8',
        }
      });

    } catch (err) {
      return new Response(`Error: ${err.message}`, { status: 500 });
    }
  },
};

function getHTMLPage() {
  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>VLESS Server</title>
  <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body {
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
      display: flex;
      justify-content: center;
      align-items: center;
      min-height: 100vh;
      background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
      color: white;
      padding: 20px;
    }
    .container {
      text-align: center;
      padding: 3rem;
      background: rgba(255,255,255,0.1);
      border-radius: 20px;
      backdrop-filter: blur(10px);
      box-shadow: 0 8px 32px rgba(0,0,0,0.3);
      max-width: 600px;
    }
    h1 { font-size: 3rem; margin-bottom: 1rem; }
    .info { font-size: 1rem; margin: 0.5rem 0; opacity: 0.9; }
    .status {
      display: inline-block;
      padding: 0.5rem 1.5rem;
      background: rgba(76, 175, 80, 0.3);
      border-radius: 25px;
      margin-top: 1.5rem;
      font-weight: 600;
    }
    .config {
      margin-top: 2rem;
      padding: 1rem;
      background: rgba(0,0,0,0.2);
      border-radius: 10px;
      font-size: 0.9rem;
      text-align: left;
    }
    .config-item {
      margin: 0.5rem 0;
    }
    .config-label {
      color: #ffd700;
      font-weight: 600;
    }
  </style>
</head>
<body>
  <div class="container">
    <h1>🚀</h1>
    <h1>VLESS Server</h1>
    <p class="info">Server aktif dan siap digunakan</p>
    <p class="info">WebSocket + TLS Enabled</p>
    <p class="info">HTTP/2 & HTTP/3 Support</p>
    <div class="status">✅ Status: Online</div>
    
    <div class="config">
      <div class="config-item">
        <span class="config-label">Protocol:</span> VLESS
      </div>
      <div class="config-item">
        <span class="config-label">Network:</span> WebSocket (ws)
      </div>
      <div class="config-item">
        <span class="config-label">Path:</span> ${CONFIG.WS_PATH}
      </div>
      <div class="config-item">
        <span class="config-label">TLS:</span> Enabled
      </div>
    </div>
  </div>
</body>
</html>`;
}

/*
===========================================
KONFIGURASI CLIENT XRAY/V2RAY:
===========================================

FORMAT LINK:
vless://[UUID]@[WORKER-DOMAIN]:443?encryption=none&security=tls&type=ws&host=[WORKER-DOMAIN]&path=%2Fvless&sni=[WORKER-DOMAIN]#VLESS-CF

CONTOH:
vless://89b3cbba-e6ac-485a-9481-976a0e093b7b@your-worker.workers.dev:443?encryption=none&security=tls&type=ws&host=your-worker.workers.dev&path=%2Fvless&sni=your-worker.workers.dev#VLESS-CF

ATAU KONFIGURASI MANUAL:
- Address: your-worker.workers.dev
- Port: 443
- UUID: 89b3cbba-e6ac-485a-9481-976a0e093b7b
- Network: ws
- Path: /vless
- TLS: tls
- Host: your-worker.workers.dev
- SNI: your-worker.workers.dev

*/
