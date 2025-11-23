// Cloudflare Worker for VLESS with Hybrid Storage (KV + Fallback)
//
// Features:
// - Supports HTTP/1.1, HTTP/2, HTTP/3 (QUIC) automatically via Cloudflare Edge.
// - Multi-user support via Cloudflare KV (if configured).
// - Fallback to single user (Default UUID) if KV is missing (No Crash).
// - Web-based Admin Panel.

import { connect } from 'cloudflare:sockets';

const DEFAULT_UUID = '841d0c38-1352-4090-95ad-3516c53170b0';

export default {
  async fetch(request, env, ctx) {
    try {
      const upgradeHeader = request.headers.get('Upgrade');
      const url = new URL(request.url);

      // 1. Handle WebSocket (VLESS Traffic)
      if (upgradeHeader === 'websocket') {
        return await vlessOverWSHandler(request, env);
      }

      // 2. Handle Admin Panel & API
      if (url.pathname.startsWith('/admin') || url.pathname.startsWith('/api')) {
         return await handleAdminRequest(request, env, url);
      }

      // 3. Root Path - Public Dashboard or Login
      return new Response(getLoginPage(env), {
        status: 200,
        headers: { "Content-Type": "text/html;charset=utf-8" }
      });

    } catch (err) {
      return new Response(`Worker Error: ${err.toString()}\nStack: ${err.stack}`, {
        status: 500,
        headers: { "Content-Type": "text/plain" }
      });
    }
  },
};

// --- Admin & UI Logic ---

async function handleAdminRequest(request, env, url) {
  const correctPassword = env.ADMIN_PASSWORD || 'admin';

  // Simple API for the frontend
  if (url.pathname === '/api/login') {
    try {
      const { password } = await request.json();
      if (password === correctPassword) {
        return new Response(JSON.stringify({ success: true }), { 
          headers: { 'Content-Type': 'application/json' }
        });
      }
      return new Response(JSON.stringify({ success: false }), { 
        status: 401,
        headers: { 'Content-Type': 'application/json' }
      });
    } catch (e) {
      return new Response(JSON.stringify({ success: false, error: 'Invalid request' }), { 
        status: 400,
        headers: { 'Content-Type': 'application/json' }
      });
    }
  }

  const authHeader = request.headers.get('Authorization');
  if (authHeader !== correctPassword) {
     if (url.pathname.startsWith('/api/')) {
         return new Response('Unauthorized', { status: 401 });
     }
     return new Response(getLoginPage(env), { 
       headers: { 'Content-Type': 'text/html;charset=utf-8' }
     });
  }

  // KV Operations
  if (url.pathname === '/api/users') {
    // Check if KV is available
    if (!env.VLESS_KV) {
        return new Response(JSON.stringify({ 
          error: "KV Namespace 'VLESS_KV' is not bound. Check wrangler.toml." 
        }), { 
          status: 500,
          headers: { 'Content-Type': 'application/json' }
        });
    }

    try {
      if (request.method === 'GET') {
         const list = await env.VLESS_KV.list();
         const users = [];
         for (const key of list.keys) {
           const meta = await env.VLESS_KV.get(key.name);
           users.push({ uuid: key.name, name: meta || 'User' });
         }
         return new Response(JSON.stringify(users), {
           headers: { 'Content-Type': 'application/json' }
         });
      }
      if (request.method === 'POST') {
         const { uuid, name } = await request.json();
         if (!uuid) {
           return new Response(JSON.stringify({ error: 'UUID required' }), { 
             status: 400,
             headers: { 'Content-Type': 'application/json' }
           });
         }
         await env.VLESS_KV.put(uuid, name || 'User');
         return new Response(JSON.stringify({ success: true }), {
           headers: { 'Content-Type': 'application/json' }
         });
      }
      if (request.method === 'DELETE') {
         const { uuid } = await request.json();
         if (!uuid) {
           return new Response(JSON.stringify({ error: 'UUID required' }), { 
             status: 400,
             headers: { 'Content-Type': 'application/json' }
           });
         }
         await env.VLESS_KV.delete(uuid);
         return new Response(JSON.stringify({ success: true }), {
           headers: { 'Content-Type': 'application/json' }
         });
      }
    } catch (e) {
      return new Response(JSON.stringify({ error: e.toString() }), {
        status: 500,
        headers: { 'Content-Type': 'application/json' }
      });
    }
  }

  if (url.pathname === '/admin/dashboard') {
     return new Response(getAdminDashboard(request.headers.get('Host') || 'your-worker.workers.dev', correctPassword, !!env.VLESS_KV), {
        headers: { "Content-Type": "text/html;charset=utf-8" }
     });
  }

  return new Response('Not Found', { status: 404 });
}

// --- VLESS Logic ---

async function vlessOverWSHandler(request, env) {
  const webSocketPair = new WebSocketPair();
  const [client, webSocket] = Object.values(webSocketPair);

  webSocket.accept();

  let address = '';
  let portWithRandomLog = '';
  const log = (info, event) => {
    console.log(`[${address}:${portWithRandomLog}] ${info}`, event || '');
  };
  const earlyDataHeader = request.headers.get('sec-websocket-protocol') || '';

  const readableWebSocketStream = makeReadableWebSocketStream(webSocket, earlyDataHeader, log);

  let remoteSocketWrapper = {
    value: null,
  };

  let isRemoteConnected = false;

  readableWebSocketStream.pipeTo(new WritableStream({
    async write(chunk, controller) {
      if (isRemoteConnected && remoteSocketWrapper.value) {
        const writer = remoteSocketWrapper.value.writable.getWriter();
        await writer.write(chunk);
        writer.releaseLock();
        return;
      }

      const {
        hasError,
        message,
        portRemote = 443,
        addressRemote = '',
        rawDataIndex,
        vlessVersion = new Uint8Array([0, 0]),
        isUDP,
        uuid
      } = processVlessHeader(chunk);

      // AUTHENTICATION CHECK
      let isValid = false;

      // 1. Check Default UUID (Always valid)
      if (uuid === (env.UUID || DEFAULT_UUID)) {
          isValid = true;
      }
      // 2. Check KV (if available)
      else if (env.VLESS_KV) {
         try {
           const user = await env.VLESS_KV.get(uuid);
           if (user) isValid = true;
         } catch (e) {
           console.log(`KV lookup error: ${e}`);
         }
      }

      if (!isValid) {
           console.log(`Blocked invalid UUID: ${uuid}`);
           try {
             webSocket.close(1008, "Invalid User");
           } catch (e) {
             console.log('Error closing websocket:', e);
           }
           return;
      }

      address = addressRemote;
      portWithRandomLog = `${portRemote}--${Math.random()} ${isUDP ? 'udp ' : 'tcp '}`;
      
      if (hasError) {
        log(`processVlessHeader error: ${message}`);
        try {
          webSocket.close(1003, message);
        } catch (e) {
          console.log('Error closing websocket:', e);
        }
        return;
      }

      if (isUDP) {
        log('UDP not supported, closing connection');
        try {
          webSocket.close(1003, "UDP not supported");
        } catch (e) {
          console.log('Error closing websocket:', e);
        }
        return;
      }

      const vlessResponseHeader = new Uint8Array([vlessVersion[0], 0]);
      const rawClientData = chunk.slice(rawDataIndex);

      isRemoteConnected = true;
      await handleTCPOutBound(remoteSocketWrapper, addressRemote, portRemote, rawClientData, webSocket, vlessResponseHeader, log);
    },
    close() {
      log(`readableWebSocketStream is closed`);
    },
    abort(reason) {
      log(`readableWebSocketStream is aborted`, JSON.stringify(reason));
    },
  })).catch((err) => {
    log('readableWebSocketStream pipeTo error', err);
  });

  return new Response(null, {
    status: 101,
    webSocket: client,
  });
}

async function handleTCPOutBound(remoteSocket, addressRemote, portRemote, rawClientData, webSocket, vlessResponseHeader, log) {
  async function connectAndWrite(address, port) {
    try {
      const tcpSocket = connect({
        hostname: address,
        port: port,
      });
      remoteSocket.value = tcpSocket;
      log(`connected to ${address}:${port}`);
      const writer = tcpSocket.writable.getWriter();
      await writer.write(rawClientData);
      writer.releaseLock();
      return tcpSocket;
    } catch (error) {
      log(`Connection failed to ${address}:${port}`, error.message);
      throw error;
    }
  }

  try {
    const tcpSocket = await connectAndWrite(addressRemote, portRemote);
    await vlessRemoteSocketToWS(tcpSocket, webSocket, vlessResponseHeader, null, log);
  } catch (error) {
    log('handleTCPOutBound error', error.message);
    safeCloseWebSocket(webSocket);
  }
}

async function vlessRemoteSocketToWS(remoteSocket, webSocket, vlessResponseHeader, retry, log) {
  let hasHeaderSent = false;
  try {
    await remoteSocket.readable.pipeTo(new WritableStream({
      async write(chunk, controller) {
        if (webSocket.readyState !== 1) { // 1 = OPEN
          controller.error('WebSocket not open');
          return;
        }
        
        if (hasHeaderSent) {
          webSocket.send(chunk);
        } else {
          const newChunk = new Uint8Array(vlessResponseHeader.length + chunk.byteLength);
          newChunk.set(vlessResponseHeader);
          newChunk.set(new Uint8Array(chunk), vlessResponseHeader.length);
          webSocket.send(newChunk);
          hasHeaderSent = true;
        }
      },
      close() {
        log(`remoteConnection readable is closed`);
        safeCloseWebSocket(webSocket);
      },
      abort(reason) {
        log(`remoteConnection readable aborted`, reason);
        safeCloseWebSocket(webSocket);
      },
    }));
  } catch (err) {
    log(`remoteSocketToWS error:`, err.message);
    safeCloseWebSocket(webSocket);
  }
}

function safeCloseWebSocket(socket) {
  try {
    if (socket.readyState === 1 || socket.readyState === 2) { // OPEN or CLOSING
      socket.close();
    }
  } catch (e) {
    console.error('safeCloseWebSocket error', e);
  }
}

function makeReadableWebSocketStream(webSocketServer, earlyDataHeader, log) {
  let readableStreamCancel = false;
  const stream = new ReadableStream({
    start(controller) {
      webSocketServer.addEventListener('message', (event) => {
        if (readableStreamCancel) {
          return;
        }
        const message = event.data;
        controller.enqueue(message);
      });
      webSocketServer.addEventListener('close', () => {
        safeCloseWebSocket(webSocketServer);
        if (readableStreamCancel) {
          return;
        }
        controller.close();
      });
      webSocketServer.addEventListener('error', (err) => {
        log('webSocketServer has error');
        controller.error(err);
      });
      const { earlyData, error } = base64ToArrayBuffer(earlyDataHeader);
      if (error) {
        controller.error(error);
      } else if (earlyData) {
        controller.enqueue(earlyData);
      }
    },
    cancel(reason) {
      if (readableStreamCancel) {
        return;
      }
      log(`ReadableStream was canceled, due to ${reason}`);
      readableStreamCancel = true;
      safeCloseWebSocket(webSocketServer);
    }
  });
  return stream;
}

function processVlessHeader(vlessBuffer) {
  if (vlessBuffer.byteLength < 24) {
    return { hasError: true, message: 'invalid data' };
  }
  const version = new Uint8Array(vlessBuffer.slice(0, 1));
  const uuid = stringify(new Uint8Array(vlessBuffer.slice(1, 17)));
  const optLength = new Uint8Array(vlessBuffer.slice(17, 18))[0];
  const command = new Uint8Array(vlessBuffer.slice(18 + optLength, 18 + optLength + 1))[0];

  let isUDP = command === 2;
  if (command !== 1 && command !== 2) {
    return { hasError: true, message: `command ${command} is not supported` };
  }

  const portIndex = 18 + optLength + 1;
  const portDecoder = new DataView(vlessBuffer.slice(portIndex, portIndex + 2));
  const portRemote = portDecoder.getUint16(0);

  let addressIndex = portIndex + 2;
  const addressBuffer = new Uint8Array(vlessBuffer.slice(addressIndex, addressIndex + 1));

  const addressType = addressBuffer[0];
  let addressLength = 0;
  let addressValueIndex = addressIndex + 1;
  let addressValue = '';

  switch (addressType) {
    case 1:
      addressLength = 4;
      addressValue = new Uint8Array(vlessBuffer.slice(addressValueIndex, addressValueIndex + addressLength)).join('.');
      break;
    case 2:
      addressLength = new Uint8Array(vlessBuffer.slice(addressValueIndex, addressValueIndex + 1))[0];
      addressValueIndex += 1;
      addressValue = new TextDecoder().decode(vlessBuffer.slice(addressValueIndex, addressValueIndex + addressLength));
      break;
    case 3:
      addressLength = 16;
      const dataView = new DataView(vlessBuffer.slice(addressValueIndex, addressValueIndex + addressLength));
      const ipv6 = [];
      for (let i = 0; i < 8; i++) {
        ipv6.push(dataView.getUint16(i * 2).toString(16));
      }
      addressValue = ipv6.join(':');
      break;
    default:
      return { hasError: true, message: `invalid addressType is ${addressType}` };
  }

  return {
    hasError: false,
    addressRemote: addressValue,
    addressType,
    portRemote,
    rawDataIndex: addressValueIndex + addressLength,
    vlessVersion: version,
    isUDP,
    uuid
  };
}

function base64ToArrayBuffer(base64Str) {
  if (!base64Str) {
    return { error: null };
  }
  try {
    base64Str = base64Str.replace(/-/g, '+').replace(/_/g, '/');
    const decode = atob(base64Str);
    const arryBuffer = Uint8Array.from(decode, (c) => c.charCodeAt(0));
    return { earlyData: arryBuffer.buffer, error: null };
  } catch (error) {
    return { error };
  }
}

function stringify(buffer) {
    const byteToHex = [];
    for (let i = 0; i < 256; ++i) {
        byteToHex.push((i + 0x100).toString(16).substring(1));
    }
    const r = buffer;
    return [
        byteToHex[r[0]], byteToHex[r[1]], byteToHex[r[2]], byteToHex[r[3]], '-',
        byteToHex[r[4]], byteToHex[r[5]], '-',
        byteToHex[r[6]], byteToHex[r[7]], '-',
        byteToHex[r[8]], byteToHex[r[9]], '-',
        byteToHex[r[10]], byteToHex[r[11]], byteToHex[r[12]], byteToHex[r[13]], byteToHex[r[14]], byteToHex[r[15]]
    ].join('').toLowerCase();
}

// HTML Templates

function getLoginPage(env) {
  const kvStatus = env.VLESS_KV ? "Online" : "Offline (Fallback Mode)";
  const defaultUUID = env.UUID || DEFAULT_UUID;

  return `
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>VLESS Login</title>
  <style>
    body{background:#0f172a;color:white;display:flex;align-items:center;justify-content:center;min-height:100vh;font-family:sans-serif;margin:0;padding:1rem;}
    .card{background:#1e293b;padding:2rem;border-radius:1rem;width:100%;max-width:400px;box-shadow:0 4px 6px rgba(0,0,0,0.1);}
    input{width:100%;padding:0.75rem;margin:1rem 0;background:#020617;border:1px solid #334155;color:white;border-radius:0.5rem;box-sizing:border-box;}
    button{width:100%;padding:0.75rem;background:#3b82f6;color:white;border:none;border-radius:0.5rem;cursor:pointer;font-size:1rem;}
    button:hover{background:#2563eb;}
    .status{margin-top:1rem;padding:1rem;background:#334155;border-radius:0.5rem;font-size:0.85rem;}
    .status-ok{color:#4ade80;} .status-bad{color:#f87171;}
  </style>
</head>
<body>
  <div class="card">
    <h2 style="text-align:center;margin-top:0;">VLESS Panel</h2>
    <p style="text-align:center;color:#94a3b8;">Login to manage users</p>
    <input type="password" id="pwd" placeholder="Admin Password">
    <button onclick="login()">Login</button>

    <div class="status">
        <div><strong>System Status:</strong></div>
        <div>KV Storage: <span class="${env.VLESS_KV ? 'status-ok' : 'status-bad'}">${kvStatus}</span></div>
        <div>Protocol: <span class="status-ok">HTTP/1.1, HTTP/2, HTTP/3</span></div>
        <div style="margin-top:0.5rem;word-break:break-all;">Default UUID: <br>${defaultUUID}</div>
    </div>
  </div>
  <script>
    async function login() {
      const pwd = document.getElementById('pwd').value;
      if (!pwd) {
        alert('Please enter password');
        return;
      }
      try {
        const res = await fetch('/api/login', { 
          method: 'POST', 
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({password: pwd}) 
        });
        const data = await res.json();
        if(data.success) {
          localStorage.setItem('vless_admin_key', pwd);
          window.location.href = '/admin/dashboard';
        } else {
          alert('Invalid Password');
        }
      } catch (e) {
        alert('Login failed: ' + e.message);
      }
    }
  </script>
</body>
</html>
`;
}

function getAdminDashboard(host, password, hasKV) {
  return `
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>VLESS Manager</title>
<style>
:root { --primary: #3b82f6; --bg: #0f172a; --card: #1e293b; --text: #e2e8f0; }
body { font-family: system-ui, sans-serif; background: var(--bg); color: var(--text); margin: 0; padding: 2rem; }
.container { max-width: 800px; margin: 0 auto; }
h1 { text-align: center; color: #60a5fa; }
.card { background: var(--card); border-radius: 1rem; padding: 1.5rem; margin-bottom: 1.5rem; border: 1px solid #334155; }
.btn { background: var(--primary); color: white; border: none; padding: 0.5rem 1rem; border-radius: 0.5rem; cursor: pointer; }
.btn:hover { background: #2563eb; }
.btn-danger { background: #ef4444; }
.btn-danger:hover { background: #dc2626; }
input { background: #020617; border: 1px solid #334155; color: white; padding: 0.5rem; border-radius: 0.5rem; }
table { width: 100%; border-collapse: collapse; margin-top: 1rem; }
th, td { text-align: left; padding: 0.75rem; border-bottom: 1px solid #334155; word-break: break-all; }
.copy-box { background: black; padding: 0.5rem; font-family: monospace; font-size: 0.8rem; overflow-x: auto; cursor: pointer; border-radius: 0.5rem; margin-bottom: 0.5rem; }
.copy-box:hover { background: #1a1a1a; }
.alert { background: #7f1d1d; color: #fecaca; padding: 1rem; border-radius: 0.5rem; margin-bottom: 1rem; }
</style>
</head>
<body>
<div class="container">
    <h1>⚡ VLESS Manager</h1>

    ${!hasKV ? '<div class="alert">⚠️ KV Namespace Not Configured. User management disabled. Only Default UUID works.</div>' : ''}

    <div class="card">
        <h3>Add User</h3>
        <div style="display:flex;gap:1rem;flex-wrap:wrap;">
            <input type="text" id="newName" placeholder="User Name (e.g. User1)" style="flex:1;min-width:200px;">
            <button class="btn" onclick="addUser()">Generate & Add</button>
        </div>
    </div>

    <div class="card">
        <h3>Active Users</h3>
        <div style="overflow-x:auto;">
          <table>
              <thead><tr><th>Name</th><th>UUID</th><th>Action</th></tr></thead>
              <tbody id="userTable"></tbody>
          </table>
        </div>
    </div>

    <div class="card" id="configCard" style="display:none;">
        <h3>Generated Config</h3>
        <p>SNI/Bug Host:</p>
        <input type="text" id="sni" placeholder="${host}" onkeyup="updateLinks()" style="width:100%;margin-bottom:1rem;">
        <p style="margin-bottom:0.5rem;">TLS (Port 443):</p>
        <div class="copy-box" id="tlsLink" onclick="copy(this)"></div>
        <p style="margin-bottom:0.5rem;">Non-TLS (Port 80):</p>
        <div class="copy-box" id="nontlsLink" onclick="copy(this)"></div>
    </div>
</div>

<script>
    const host = "${host}";
    const auth = localStorage.getItem('vless_admin_key');
    const hasKV = ${hasKV};

    if(!auth) {
      window.location.href = '/';
    }

    async function loadUsers() {
        if (!hasKV) {
             document.getElementById('userTable').innerHTML = '<tr><td colspan="3">KV Storage not available.</td></tr>';
             return;
        }
        try {
          const res = await fetch('/api/users', { headers: { 'Authorization': auth } });
          if (!res.ok) throw new Error('Failed to load users');
          const users = await res.json();
          const tbody = document.getElementById('userTable');
          tbody.innerHTML = '';
          if (users.length === 0) {
            tbody.innerHTML = '<tr><td colspan="3">No users yet. Add one above.</td></tr>';
            return;
          }
          users.forEach(u => {
              tbody.innerHTML += \`
                  <tr>
                      <td>\${u.name}</td>
                      <td style="font-family:monospace;font-size:0.85em;">\${u.uuid}</td>
                      <td style="white-space:nowrap;">
                          <button class="btn" onclick="showConfig('\${u.uuid}', '\${u.name}')">Link</button>
                          <button class="btn btn-danger" onclick="deleteUser('\${u.uuid}')">Del</button>
                      </td>
                  </tr>
              \`;
          });
        } catch (e) {
          alert('Error loading users: ' + e.message);
        }
    }

    async function addUser() {
        if (!hasKV) return alert("KV not set up!");
        const name = document.getElementById('newName').value || 'User';
        const uuid = crypto.randomUUID();
        try {
          await fetch('/api/users', {
              method: 'POST',
              headers: { 'Authorization': auth, 'Content-Type': 'application/json' },
              body: JSON.stringify({ uuid, name })
          });
          loadUsers();
          document.getElementById('newName').value = '';
          alert('User added successfully!');
        } catch (e) {
          alert('Error adding user: ' + e.message);
        }
    }

    async function deleteUser(uuid) {
        if(!confirm('Delete this user?')) return;
        try {
          await fetch('/api/users', {
              method: 'DELETE',
              headers: { 'Authorization': auth, 'Content-Type': 'application/json' },
              body: JSON.stringify({ uuid })
          });
          loadUsers();
          document.getElementById('configCard').style.display = 'none';
        } catch (e) {
          alert('Error deleting user: ' + e.message);
        }
    }

    let currentUUID = '';
    function showConfig(uuid, name) {
        currentUUID = uuid;
        document.getElementById('configCard').style.display = 'block';
        document.getElementById('sni').value = host;
        updateLinks();
        setTimeout(() => {
          document.getElementById('configCard').scrollIntoView({ behavior: 'smooth' });
        }, 100);
    }

    function updateLinks() {
        const sni = document.getElementById('sni').value || host;
        const tls = \`vless://\${currentUUID}@\${sni}:443?encryption=none&security=tls&sni=\${host}&fp=chrome&type=ws&host=\${host}&path=%2F#\${encodeURIComponent(host)}-TLS\`;
        const nontls = \`vless://\${currentUUID}@\${sni}:80?encryption=none&security=none&type=ws&host=\${host}&path=%2F#\${encodeURIComponent(host)}-HTTP\`;

        document.getElementById('tlsLink').textContent = tls;
        document.getElementById('nontlsLink').textContent = nontls;
    }

    function copy(el) {
        navigator.clipboard.writeText(el.textContent).then(() => {
          alert('Copied to clipboard!');
        }).catch(() => {
          alert('Failed to copy');
        });
    }

    loadUsers();
</script>
</body>
</html>
`;
}
