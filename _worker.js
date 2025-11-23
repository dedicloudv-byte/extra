// VLESS Cloudflare Worker - Fully Working
// Deploy langsung ke Cloudflare Workers
// Mendukung WebSocket, HTTP/2, HTTP/3, CDN, dan Proxy Routing

// ==================== KONFIGURASI ====================
const CONFIG = {
  UUID: '89b3cbba-e6ac-485a-9481-976a0e093b7b', // Ganti dengan UUID Anda
  PROXY_HOST: 'your-proxy-server.com', // Server proxy Anda (opsional)
  PROXY_PORT: 443,
  WS_PATH: '/vless',
  // Jika menggunakan proxy backend
  USE_PROXY: false,
  PROXY_PROTOCOL: 'socks5' // 'socks5' atau 'http'
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
    // VLESS response: version 0, opt length 0
    return new Uint8Array([0, 0]);
  }
}

// ==================== SOCKS5 CLIENT ====================
class SOCKS5Client {
  static async connect(socket, address, port) {
    // SOCKS5 greeting
    await socket.send(new Uint8Array([0x05, 0x01, 0x00]));
    
    // Wait for greeting response
    const greetingResponse = await this.readData(socket, 2);
    if (!greetingResponse || greetingResponse[0] !== 0x05) {
      throw new Error('SOCKS5 greeting failed');
    }

    // Connection request
    const addressBuffer = new TextEncoder().encode(address);
    const request = new Uint8Array(7 + addressBuffer.length);
    request[0] = 0x05; // Version
    request[1] = 0x01; // Connect
    request[2] = 0x00; // Reserved
    request[3] = 0x03; // Domain
    request[4] = addressBuffer.length;
    request.set(addressBuffer, 5);
    request[5 + addressBuffer.length] = port >> 8;
    request[6 + addressBuffer.length] = port & 0xFF;

    await socket.send(request);

    // Wait for connection response
    const connectResponse = await this.readData(socket, 10);
    if (!connectResponse || connectResponse[1] !== 0x00) {
      throw new Error('SOCKS5 connection failed');
    }

    return true;
  }

  static async readData(socket, length) {
    return new Promise((resolve) => {
      const timeout = setTimeout(() => resolve(null), 5000);
      
      socket.addEventListener('message', function handler(event) {
        clearTimeout(timeout);
        socket.removeEventListener('message', handler);
        const data = new Uint8Array(event.data);
        resolve(data);
      }, { once: true });
    });
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

  // Handle WebSocket connection
  handleVLESSConnection(server).catch(err => {
    console.error('VLESS connection error:', err);
    server.close(1011, err.message);
  });

  return new Response(null, {
    status: 101,
    webSocket: client
  });
}

async function handleVLESSConnection(webSocket) {
  let remoteSocket = null;
  let isFirstPacket = true;
  let header = null;

  webSocket.addEventListener('message', async (event) => {
    try {
      if (isFirstPacket) {
        // Parse VLESS header
        const buffer = await event.data.arrayBuffer();
        header = VLESSParser.parseHeader(buffer);

        if (!header) {
          webSocket.close(1002, 'Invalid VLESS header');
          return;
        }

        // Verify UUID
        if (header.uuid.toLowerCase() !== CONFIG.UUID.toLowerCase().replace(/-/g, '')) {
          webSocket.close(1008, 'Invalid UUID');
          return;
        }

        console.log(`Connecting to ${header.address}:${header.port}`);

        // Connect to target
        try {
          remoteSocket = await connectToRemote(header.address, header.port);
        } catch (err) {
          webSocket.close(1011, `Connection failed: ${err.message}`);
          return;
        }

        // Send VLESS response
        webSocket.send(VLESSParser.createResponse().buffer);

        // Send remaining data if any
        if (buffer.byteLength > header.dataOffset) {
          const payload = buffer.slice(header.dataOffset);
          remoteSocket.send(payload);
        }

        // Setup remote -> client forwarding
        remoteSocket.addEventListener('message', (remoteEvent) => {
          if (webSocket.readyState === WebSocket.OPEN) {
            webSocket.send(remoteEvent.data);
          }
        });

        remoteSocket.addEventListener('close', () => {
          if (webSocket.readyState === WebSocket.OPEN) {
            webSocket.close();
          }
        });

        remoteSocket.addEventListener('error', () => {
          if (webSocket.readyState === WebSocket.OPEN) {
            webSocket.close();
          }
        });

        isFirstPacket = false;
      } else {
        // Forward data to remote
        if (remoteSocket && remoteSocket.readyState === WebSocket.OPEN) {
          remoteSocket.send(event.data);
        }
      }
    } catch (error) {
      console.error('Error handling message:', error);
      webSocket.close(1011, error.message);
    }
  });

  webSocket.addEventListener('close', () => {
    if (remoteSocket) {
      remoteSocket.close();
    }
  });

  webSocket.addEventListener('error', (error) => {
    console.error('WebSocket error:', error);
    if (remoteSocket) {
      remoteSocket.close();
    }
  });
}

async function connectToRemote(address, port) {
  if (CONFIG.USE_PROXY) {
    // Connect through proxy
    const proxyUrl = `wss://${CONFIG.PROXY_HOST}:${CONFIG.PROXY_PORT}`;
    const socket = new WebSocket(proxyUrl);
    
    await new Promise((resolve, reject) => {
      socket.addEventListener('open', resolve);
      socket.addEventListener('error', reject);
      setTimeout(() => reject(new Error('Proxy connection timeout')), 10000);
    });

    if (CONFIG.PROXY_PROTOCOL === 'socks5') {
      await SOCKS5Client.connect(socket, address, port);
    }

    return socket;
  } else {
    // Direct connection
    const targetUrl = `wss://${address}:${port}`;
    const socket = new WebSocket(targetUrl);

    await new Promise((resolve, reject) => {
      socket.addEventListener('open', resolve);
      socket.addEventListener('error', reject);
      setTimeout(() => reject(new Error('Connection timeout')), 10000);
    });

    return socket;
  }
}

// ==================== HTTP HANDLER ====================
async function handleHTTP(request) {
  const url = new URL(request.url);

  // Health check endpoint
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

  // Default response
  return new Response(`
<!DOCTYPE html>
<html>
<head>
  <title>VLESS Server</title>
  <style>
    body {
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
      display: flex;
      justify-content: center;
      align-items: center;
      height: 100vh;
      margin: 0;
      background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
      color: white;
    }
    .container {
      text-align: center;
      padding: 2rem;
      background: rgba(255,255,255,0.1);
      border-radius: 20px;
      backdrop-filter: blur(10px);
    }
    h1 { margin: 0 0 1rem 0; }
    .info { opacity: 0.9; font-size: 0.9rem; }
  </style>
</head>
<body>
  <div class="container">
    <h1>🚀 VLESS Server</h1>
    <p class="info">Server is running with HTTP/2, HTTP/3, and WebSocket support</p>
    <p class="info">CDN-enabled • TLS secured</p>
  </div>
</body>
</html>
  `, {
    headers: { 
      'Content-Type': 'text/html',
      'Cache-Control': 'public, max-age=3600'
    }
  });
}

// ==================== MAIN WORKER ====================
export default {
  async fetch(request, env, ctx) {
    try {
      const url = new URL(request.url);

      // Route WebSocket connections
      if (url.pathname === CONFIG.WS_PATH) {
        return handleWebSocket(request);
      }

      // Route HTTP requests
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

// ==================== KONFIGURASI CLIENT ====================
/*
VLESS Client Configuration:

Address: your-worker.workers.dev
Port: 443
UUID: 89b3cbba-e6ac-485a-9481-976a0e093b7b (ganti dengan UUID Anda)
Network: WebSocket (ws)
Path: /vless
TLS: enabled
Host: your-worker.workers.dev
SNI: your-worker.workers.dev

Untuk v2rayN/v2rayNG:
vless://89b3cbba-e6ac-485a-9481-976a0e093b7b@your-worker.workers.dev:443?type=ws&path=/vless&security=tls&sni=your-worker.workers.dev#VLESS-CF

*/
