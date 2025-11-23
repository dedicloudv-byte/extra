// VLESS Cloudflare Worker - FIXED VERSION
// Error "arrayBuffer is not a function" sudah diperbaiki
// Mendukung WebSocket, HTTP/2, HTTP/3, CDN

// ==================== KONFIGURASI ====================
const CONFIG = {
  UUID: '89b3cbba-e6ac-485a-9481-976a0e093b7b', // Ganti dengan UUID Anda
  WS_PATH: '/vless',
  // Jika ingin menggunakan proxy backend, set USE_PROXY = true
  USE_PROXY: false,
  PROXY_HOST: 'mm.ahem7553.workers.dev',
  PROXY_PORT: 443
};

// ==================== VLESS PROTOCOL PARSER ====================
class VLESSParser {
  static parseHeader(buffer) {
    if (buffer.byteLength < 1) return null;
    
    const view = new DataView(buffer);
    let offset = 0;

    // Version
    const version = view.getUint8(offset++);
    if (version !== 0) return null;

    // UUID (16 bytes)
    if (buffer.byteLength < offset + 16) return null;
    const uuidArray = new Uint8Array(buffer, offset, 16);
    const uuid = Array.from(uuidArray)
      .map(b => b.toString(16).padStart(2, '0'))
      .join('');
    offset += 16;

    // Opt length
    if (buffer.byteLength < offset + 1) return null;
    const optLength = view.getUint8(offset++);
    offset += optLength;

    // Command
    if (buffer.byteLength < offset + 1) return null;
    const command = view.getUint8(offset++);

    // Port
    if (buffer.byteLength < offset + 2) return null;
    const port = view.getUint16(offset);
    offset += 2;

    // Address Type
    if (buffer.byteLength < offset + 1) return null;
    const addressType = view.getUint8(offset++);

    let address = '';

    // Parse address
    if (addressType === 1) { // IPv4
      if (buffer.byteLength < offset + 4) return null;
      address = `${view.getUint8(offset)}.${view.getUint8(offset + 1)}.${view.getUint8(offset + 2)}.${view.getUint8(offset + 3)}`;
      offset += 4;
    } else if (addressType === 2) { // Domain
      if (buffer.byteLength < offset + 1) return null;
      const domainLength = view.getUint8(offset++);
      if (buffer.byteLength < offset + domainLength) return null;
      const domainArray = new Uint8Array(buffer, offset, domainLength);
      address = new TextDecoder().decode(domainArray);
      offset += domainLength;
    } else if (addressType === 3) { // IPv6
      if (buffer.byteLength < offset + 16) return null;
      const ipv6 = [];
      for (let i = 0; i < 8; i++) {
        ipv6.push(view.getUint16(offset + i * 2).toString(16));
      }
      address = ipv6.join(':');
      offset += 16;
    } else {
      return null;
    }

    return {
      version,
      uuid,
      command,
      address,
      port,
      addressType,
      dataOffset: offset,
      rawData: buffer
    };
  }

  static createResponse() {
    return new Uint8Array([0, 0]);
  }
}

// ==================== TCP SOCKET HANDLER ====================
async function makeConnection(address, port) {
  try {
    const tcpSocket = connect({
      hostname: address,
      port: port
    });

    return tcpSocket;
  } catch (error) {
    console.error('TCP connection error:', error);
    throw error;
  }
}

// ==================== WEBSOCKET HANDLER ====================
async function handleWebSocket(request) {
  const upgradeHeader = request.headers.get('Upgrade');
  if (!upgradeHeader || upgradeHeader.toLowerCase() !== 'websocket') {
    return new Response('Expected Upgrade: websocket', { status: 426 });
  }

  const webSocketPair = new WebSocketPair();
  const [client, server] = Object.values(webSocketPair);

  server.accept();

  handleVLESSConnection(server).catch(err => {
    console.error('VLESS connection error:', err);
    try {
      server.close(1011, err.message?.substring(0, 100) || 'Connection error');
    } catch (e) {
      console.error('Error closing WebSocket:', e);
    }
  });

  return new Response(null, {
    status: 101,
    webSocket: client
  });
}

async function handleVLESSConnection(webSocket) {
  let remoteSocket = null;
  let remoteSocketWrapper = null;
  let isFirstPacket = true;
  let hasReceivedResponse = false;

  webSocket.addEventListener('message', async (event) => {
    try {
      if (isFirstPacket) {
        isFirstPacket = false;

        // FIX: event.data sudah ArrayBuffer di CF Workers, tidak perlu .arrayBuffer()
        let buffer;
        if (event.data instanceof ArrayBuffer) {
          buffer = event.data;
        } else if (event.data instanceof Blob) {
          buffer = await event.data.arrayBuffer();
        } else {
          buffer = new TextEncoder().encode(event.data).buffer;
        }

        const header = VLESSParser.parseHeader(buffer);

        if (!header) {
          webSocket.close(1002, 'Invalid VLESS header');
          return;
        }

        // Verify UUID
        const configUUID = CONFIG.UUID.toLowerCase().replace(/-/g, '');
        if (header.uuid.toLowerCase() !== configUUID) {
          webSocket.close(1008, 'Invalid UUID');
          return;
        }

        console.log(`Connecting to ${header.address}:${header.port}`);

        // Connect to target using TCP socket
        try {
          remoteSocket = await makeConnection(header.address, header.port);
          remoteSocketWrapper = remoteSocket.writable.getWriter();

          // Send VLESS response
          const response = VLESSParser.createResponse();
          await webSocket.send(response.buffer);
          hasReceivedResponse = true;

          // Send remaining data if any
          if (buffer.byteLength > header.dataOffset) {
            const payload = new Uint8Array(buffer.slice(header.dataOffset));
            await remoteSocketWrapper.write(payload);
          }

          // Pipe remote socket to WebSocket
          pipeRemoteToWebSocket(remoteSocket, webSocket).catch(err => {
            console.error('Pipe error:', err);
          });

        } catch (err) {
          console.error('Connection error:', err);
          webSocket.close(1011, `Failed to connect: ${err.message}`);
          return;
        }

      } else {
        // Forward subsequent packets to remote
        if (remoteSocketWrapper) {
          try {
            let data;
            if (event.data instanceof ArrayBuffer) {
              data = new Uint8Array(event.data);
            } else if (event.data instanceof Blob) {
              const arrayBuffer = await event.data.arrayBuffer();
              data = new Uint8Array(arrayBuffer);
            } else {
              data = new TextEncoder().encode(event.data);
            }
            
            await remoteSocketWrapper.write(data);
          } catch (err) {
            console.error('Write error:', err);
            webSocket.close(1011, 'Write failed');
          }
        }
      }
    } catch (error) {
      console.error('Error handling message:', error);
      webSocket.close(1011, error.message?.substring(0, 100) || 'Unknown error');
    }
  });

  webSocket.addEventListener('close', () => {
    try {
      if (remoteSocketWrapper) {
        remoteSocketWrapper.close();
      }
      if (remoteSocket) {
        remoteSocket.close();
      }
    } catch (err) {
      console.error('Error closing remote:', err);
    }
  });

  webSocket.addEventListener('error', (error) => {
    console.error('WebSocket error:', error);
    try {
      if (remoteSocketWrapper) {
        remoteSocketWrapper.close();
      }
      if (remoteSocket) {
        remoteSocket.close();
      }
    } catch (err) {
      console.error('Error closing on error:', err);
    }
  });
}

async function pipeRemoteToWebSocket(remoteSocket, webSocket) {
  const reader = remoteSocket.readable.getReader();
  
  try {
    while (true) {
      const { done, value } = await reader.read();
      
      if (done) {
        break;
      }

      if (webSocket.readyState === WebSocket.OPEN) {
        await webSocket.send(value);
      } else {
        break;
      }
    }
  } catch (error) {
    console.error('Pipe error:', error);
  } finally {
    try {
      reader.releaseLock();
    } catch (err) {
      // Ignore
    }
    
    try {
      if (webSocket.readyState === WebSocket.OPEN) {
        webSocket.close();
      }
    } catch (err) {
      // Ignore
    }
  }
}

// ==================== HTTP HANDLER ====================
async function handleHTTP(request) {
  const url = new URL(request.url);

  // Health check
  if (url.pathname === '/health') {
    return new Response(JSON.stringify({ 
      status: 'ok',
      timestamp: Date.now(),
      protocol: 'vless',
      version: '1.0'
    }), {
      headers: { 'Content-Type': 'application/json' }
    });
  }

  // Default page
  return new Response(`
<!DOCTYPE html>
<html>
<head>
  <title>VLESS Server</title>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
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
      box-shadow: 0 8px 32px rgba(0,0,0,0.1);
      max-width: 600px;
    }
    h1 { 
      margin: 0 0 1rem 0; 
      font-size: 3rem;
    }
    .info { 
      opacity: 0.9; 
      font-size: 1rem; 
      margin: 0.5rem 0;
    }
    .status {
      display: inline-block;
      padding: 0.5rem 1rem;
      background: rgba(76, 175, 80, 0.3);
      border-radius: 20px;
      margin-top: 1rem;
      font-weight: 600;
    }
  </style>
</head>
<body>
  <div class="container">
    <h1>🚀</h1>
    <h1>VLESS Server</h1>
    <p class="info">Server aktif dan siap digunakan</p>
    <p class="info">Mendukung HTTP/2, HTTP/3, dan WebSocket</p>
    <p class="info">CDN-enabled • TLS secured</p>
    <div class="status">✓ Status: Online</div>
  </div>
</body>
</html>
  `, {
    headers: { 
      'Content-Type': 'text/html; charset=utf-8',
      'Cache-Control': 'public, max-age=3600'
    }
  });
}

// ==================== MAIN WORKER ====================
export default {
  async fetch(request, env, ctx) {
    try {
      const url = new URL(request.url);

      // Route WebSocket
      if (url.pathname === CONFIG.WS_PATH) {
        return handleWebSocket(request);
      }

      // Route HTTP
      return handleHTTP(request);

    } catch (error) {
      console.error('Worker error:', error);
      return new Response(`Error: ${error.message}`, { 
        status: 500,
        headers: { 'Content-Type': 'text/plain' }
      });
    }
  }
};

// ==================== CLIENT CONFIG ====================
/*
Link VLESS untuk client:
vless://89b3cbba-e6ac-485a-9481-976a0e093b7b@your-worker.workers.dev:443?encryption=none&security=tls&sni=your-worker.workers.dev&alpn=h2,http/1.1&fp=chrome&type=ws&host=your-worker.workers.dev&path=%2Fvless#VLESS-CF

Jangan lupa ganti:
- UUID dengan UUID Anda sendiri
- your-worker.workers.dev dengan domain worker Anda
*/
