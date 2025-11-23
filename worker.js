
// Cloudflare Worker for VLESS
// Supports VLESS over WebSocket
//
// Instructions:
// 1. Set your UUID in the script below or in Cloudflare Worker Environment Variables as 'UUID'.
// 2. Deploy to Cloudflare Workers.
// 3. Access the worker URL to see the dashboard and get config links.

import { connect } from 'cloudflare:sockets';

const DEFAULT_USER_ID = '841d0c38-1352-4090-95ad-3516c53170b0';

export default {
  /**
   * @param {Request} request
   * @param {any} env
   * @param {ExecutionContext} ctx
   * @returns {Promise<Response>}
   */
  async fetch(request, env, ctx) {
    const userID = env.UUID || DEFAULT_USER_ID;
    try {
      const upgradeHeader = request.headers.get('Upgrade');
      if (!upgradeHeader || upgradeHeader !== 'websocket') {
        const url = new URL(request.url);
        switch (url.pathname) {
          case '/':
            return new Response(getDashboard(request.headers.get('Host'), userID), {
              status: 200,
              headers: {
                "Content-Type": "text/html;charset=utf-8",
              }
            });
          default:
            return new Response('Not Found', { status: 404 });
        }
      }

      // Handle WebSocket for VLESS
      return await vlessOverWSHandler(request, userID);

    } catch (err) {
      return new Response(err.toString(), { status: 500 });
    }
  },
};

async function vlessOverWSHandler(request, userID) {
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

  /** @type {{ value: import("@cloudflare/workers-types").Socket | null}}*/
  let remoteSocketWapper = {
    value: null,
  };
  let isDns = false;

  // VLESS processing
  readableWebSocketStream.pipeTo(new WritableStream({
    async write(chunk, controller) {
      if (remoteSocketWapper.value) {
        const writer = remoteSocketWapper.value.writable.getWriter();
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
      } = processVlessHeader(chunk, userID);

      address = addressRemote;
      portWithRandomLog = `${portRemote}--${Math.random()} ${isUDP ? 'udp ' : 'tcp '}`;
      if (hasError) {
        return;
      }

      // Prepeare response header
      const vlessResponseHeader = new Uint8Array([vlessVersion[0], 0]);
      const rawClientData = chunk.slice(rawDataIndex);

      // Handle TCP Outbound
      await handleTCPOutBound(remoteSocketWapper, addressRemote, portRemote, rawClientData, webSocket, vlessResponseHeader, log);
    },
    close() {
      log(`readableWebSocketStream is close`);
    },
    abort(reason) {
      log(`readableWebSocketStream is abort`, JSON.stringify(reason));
    },
  })).catch((err) => {
    log('readableWebSocketStream pipeTo error', err);
  });

  return new Response(null, {
    status: 101,
    webSocket: client,
  });
}

/**
 *
 * @param {import("@cloudflare/workers-types").WebSocket} webSocketServer
 * @param {string} earlyDataHeader
 * @param {(info: string)=> void} log
 */
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
    pull(controller) {
    },
    cancel(reason) {
      if (readableStreamCancel) {
        return;
      }
      log(`ReadableStream was canceled, due to ${reason}`)
      readableStreamCancel = true;
      safeCloseWebSocket(webSocketServer);
    }
  });

  return stream;
}

function processVlessHeader(vlessBuffer, userID) {
  if (vlessBuffer.byteLength < 24) {
    return {
      hasError: true,
      message: 'invalid data',
    };
  }
  const version = new Uint8Array(vlessBuffer.slice(0, 1));
  let isValidUser = false;
  let isUDP = false;

  const uuid = stringify(new Uint8Array(vlessBuffer.slice(1, 17)));
  if (uuid === userID) {
      isValidUser = true;
  }

  if (!isValidUser) {
    return {
      hasError: true,
      message: 'invalid user',
    };
  }

  const optLength = new Uint8Array(vlessBuffer.slice(17, 18))[0];

  const command = new Uint8Array(
    vlessBuffer.slice(18 + optLength, 18 + optLength + 1)
  )[0];

  if (command === 1) {
  } else if (command === 2) {
    isUDP = true;
  } else {
    return {
      hasError: true,
      message: `command ${command} is not support, command 01-tcp, 02-udp `,
    };
  }

  const portIndex = 18 + optLength + 1;
  const portDecoder = new DataView(vlessBuffer.slice(portIndex, portIndex + 2));
  const portRemote = portDecoder.getUint16(0);

  let addressIndex = portIndex + 2;
  const addressBuffer = new Uint8Array(
    vlessBuffer.slice(addressIndex, addressIndex + 1)
  );

  const addressType = addressBuffer[0];
  let addressLength = 0;
  let addressValueIndex = addressIndex + 1;
  let addressValue = '';
  switch (addressType) {
    case 1:
      addressLength = 4;
      addressValue = new Uint8Array(
        vlessBuffer.slice(addressValueIndex, addressValueIndex + addressLength)
      ).join('.');
      break;
    case 2:
      addressLength = new Uint8Array(
        vlessBuffer.slice(addressValueIndex, addressValueIndex + 1)
      )[0];
      addressValueIndex += 1;
      addressValue = new TextDecoder().decode(
        vlessBuffer.slice(addressValueIndex, addressValueIndex + addressLength)
      );
      break;
    case 3:
      addressLength = 16;
      const dataView = new DataView(
        vlessBuffer.slice(addressValueIndex, addressValueIndex + addressLength)
      );
      const ipv6 = [];
      for (let i = 0; i < 8; i++) {
        ipv6.push(dataView.getUint16(i * 2).toString(16));
      }
      addressValue = ipv6.join(':');
      break;
    default:
      return {
        hasError: true,
        message: `invild  addressType is ${addressType}`,
      };
  }
  if (!addressValue) {
    return {
      hasError: true,
      message: `addressValue is empty, addressType is ${addressType}`,
    };
  }

  return {
    hasError: false,
    addressRemote: addressValue,
    addressType,
    portRemote,
    rawDataIndex: addressValueIndex + addressLength,
    vlessVersion: version,
    isUDP,
  };
}

async function handleTCPOutBound(remoteSocket, addressRemote, portRemote, rawClientData, webSocket, vlessResponseHeader, log,) {
  async function connectAndWrite(address, port) {
    // @ts-ignore
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
  }

  const tcpSocket = await connectAndWrite(addressRemote, portRemote);

  await vlessRemoteSocketToWS(tcpSocket, webSocket, vlessResponseHeader, null, log);
}

async function vlessRemoteSocketToWS(remoteSocket, webSocket, vlessResponseHeader, retry, log) {
  let hasHeaderSent = false;

  await remoteSocket.readable.pipeTo(new WritableStream({
    start() {
    },
    async write(chunk, controller) {
      if (hasHeaderSent) {
        webSocket.send(chunk);
      } else {
        const newChunk = new Uint8Array(vlessResponseHeader.length + chunk.byteLength);
        newChunk.set(vlessResponseHeader);
        newChunk.set(chunk, vlessResponseHeader.length);
        webSocket.send(newChunk);
        hasHeaderSent = true;
      }
    },
    close() {
      log(`remoteConnection!.readable is close`);
    },
    abort(reason) {
      console.error(`remoteConnection!.readable abort`, reason);
    },
  })).catch((err) => {
    console.error(`remoteSocketToWS error:`, err);
    safeCloseWebSocket(webSocket);
  });
}

function safeCloseWebSocket(socket) {
  try {
    if (socket.readyState === WebSocket.OPEN || socket.readyState === WebSocket.CLOSING) {
      socket.close();
    }
  } catch (e) {
    console.error('safeCloseWebSocket error', e);
  }
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
        byteToHex.push((i + 0x100).toString(16).substr(1));
    }
    const r = buffer;
    const uuid = [
        byteToHex[r[0]], byteToHex[r[1]], byteToHex[r[2]], byteToHex[r[3]], '-',
        byteToHex[r[4]], byteToHex[r[5]], '-',
        byteToHex[r[6]], byteToHex[r[7]], '-',
        byteToHex[r[8]], byteToHex[r[9]], '-',
        byteToHex[r[10]], byteToHex[r[11]], byteToHex[r[12]], byteToHex[r[13]], byteToHex[r[14]], byteToHex[r[15]]
    ].join('');
    return uuid;
}

function getDashboard(host, userID) {
  return `
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>VLESS Cloudflare Worker</title>
<style>
:root {
    --primary: #3b82f6;
    --bg: #0f172a;
    --card: #1e293b;
    --text: #e2e8f0;
    --text-muted: #94a3b8;
}
body {
    font-family: system-ui, -apple-system, sans-serif;
    background: var(--bg);
    color: var(--text);
    margin: 0;
    min-height: 100vh;
    display: flex;
    flex-direction: column;
    align-items: center;
    padding: 2rem 1rem;
}
.container {
    max-width: 800px;
    width: 100%;
}
h1 {
    text-align: center;
    margin-bottom: 2rem;
    background: linear-gradient(to right, #60a5fa, #a78bfa);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
}
.card {
    background: var(--card);
    border-radius: 1rem;
    padding: 1.5rem;
    margin-bottom: 1.5rem;
    box-shadow: 0 4px 6px -1px rgb(0 0 0 / 0.1);
    border: 1px solid #334155;
}
.card-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 1rem;
    border-bottom: 1px solid #334155;
    padding-bottom: 1rem;
}
.card-title {
    font-size: 1.25rem;
    font-weight: 600;
    color: #f8fafc;
    margin: 0;
}
.grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
    gap: 1rem;
}
.info-item {
    background: rgba(0,0,0,0.2);
    padding: 1rem;
    border-radius: 0.5rem;
    overflow: hidden;
}
.label {
    color: var(--text-muted);
    font-size: 0.875rem;
    margin-bottom: 0.25rem;
}
.value {
    font-family: monospace;
    word-break: break-all;
}
.input-group {
    margin-bottom: 1rem;
}
input {
    width: 100%;
    background: #020617;
    border: 1px solid #334155;
    padding: 0.75rem;
    border-radius: 0.5rem;
    color: white;
    box-sizing: border-box;
    margin-top: 0.5rem;
}
button {
    background: var(--primary);
    color: white;
    border: none;
    padding: 0.5rem 1rem;
    border-radius: 0.5rem;
    cursor: pointer;
    font-weight: 500;
    transition: opacity 0.2s;
}
button:hover {
    opacity: 0.9;
}
.config-box {
    background: #020617;
    padding: 1rem;
    border-radius: 0.5rem;
    font-family: monospace;
    font-size: 0.875rem;
    word-break: break-all;
    margin-bottom: 1rem;
    border: 1px solid #334155;
    max-height: 100px;
    overflow-y: auto;
}
.tabs {
    display: flex;
    gap: 1rem;
    margin-bottom: 1rem;
}
.tab {
    padding: 0.5rem 1rem;
    border-radius: 0.5rem;
    cursor: pointer;
    background: transparent;
    border: 1px solid #334155;
}
.tab.active {
    background: var(--primary);
    border-color: var(--primary);
}
</style>
</head>
<body>
<div class="container">
    <h1>⚡ VLESS Worker Dashboard</h1>

    <div class="card">
        <div class="card-header">
            <h2 class="card-title">Server Information</h2>
        </div>
        <div class="grid">
            <div class="info-item">
                <div class="label">Host Domain</div>
                <div class="value" id="host-display">${host}</div>
            </div>
            <div class="info-item">
                <div class="label">UUID</div>
                <div class="value">${userID}</div>
            </div>
        </div>
    </div>

    <div class="card">
        <div class="card-header">
            <h2 class="card-title">Configuration Generator</h2>
        </div>

        <div class="input-group">
            <label class="label">Custom Address (SNI / ISP Bug) - Optional</label>
            <input type="text" id="sni-input" placeholder="e.g., tsel.me, quiz.vidio.com" onkeyup="updateConfigs()">
        </div>

        <div class="tabs">
            <button class="tab active" onclick="switchTab('tls')">TLS (443)</button>
            <button class="tab" onclick="switchTab('nontls')">No TLS (80)</button>
        </div>

        <div id="tls-content">
            <div class="label">VLESS TLS Config</div>
            <div class="config-box" id="vless-tls"></div>
            <button onclick="copyToClipboard('vless-tls')">Copy TLS Config</button>
        </div>

        <div id="nontls-content" style="display: none;">
            <div class="label">VLESS Non-TLS Config</div>
            <div class="config-box" id="vless-nontls"></div>
            <button onclick="copyToClipboard('vless-nontls')">Copy Non-TLS Config</button>
        </div>
    </div>

    <div class="card">
        <div class="card-header">
            <h2 class="card-title">Clash / Meta YAML</h2>
        </div>
        <div class="config-box" id="clash-config"></div>
        <button onclick="copyToClipboard('clash-config')">Copy Clash Config</button>
    </div>

</div>

<script>
    const host = "${host}";
    const uuid = "${userID}";

    function updateConfigs() {
        const sni = document.getElementById('sni-input').value || host;
        const address = document.getElementById('sni-input').value ? '${host}' : host;
        const serverForLink = document.getElementById('sni-input').value || host;

        // TLS Config
        const bug = document.getElementById('sni-input').value;

        let tlsAddr = host;
        let tlsSni = host;
        let tlsHost = host;

        if (bug) {
             tlsAddr = bug;
             tlsSni = host;
             tlsHost = host;
        }

        const vlessTls = \`vless://\${uuid}@\${tlsAddr}:443?encryption=none&security=tls&sni=\${tlsSni}&fp=randomized&type=ws&host=\${tlsHost}&path=%2F#\${host}-TLS\`;

        // Non-TLS Config

        let nonTlsAddr = host;
        let nonTlsHost = host;

        if (bug) {
            nonTlsAddr = bug;
            nonTlsHost = host;
        }

        const vlessNonTls = \`vless://\${uuid}@\${nonTlsAddr}:80?encryption=none&security=none&type=ws&host=\${nonTlsHost}&path=%2F#\${host}-HTTP\`;

        document.getElementById('vless-tls').textContent = vlessTls;
        document.getElementById('vless-nontls').textContent = vlessNonTls;

        // Clash Config
        const clash = \`
- name: \${host}-TLS
  type: vless
  server: \${tlsAddr}
  port: 443
  uuid: \${uuid}
  cipher: auto
  tls: true
  udp: true
  skip-cert-verify: true
  network: ws
  servername: \${tlsSni}
  ws-opts:
    path: /
    headers:
      Host: \${tlsHost}

- name: \${host}-HTTP
  type: vless
  server: \${nonTlsAddr}
  port: 80
  uuid: \${uuid}
  cipher: auto
  tls: false
  udp: true
  skip-cert-verify: true
  network: ws
  ws-opts:
    path: /
    headers:
      Host: \${nonTlsHost}
\`;
        document.getElementById('clash-config').textContent = clash.trim();
    }

    function switchTab(type) {
        document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
        document.querySelectorAll('#tls-content, #nontls-content').forEach(c => c.style.display = 'none');

        if (type === 'tls') {
            document.querySelector('button[onclick="switchTab(\\'tls\\')"]').classList.add('active');
            document.getElementById('tls-content').style.display = 'block';
        } else {
            document.querySelector('button[onclick="switchTab(\\'nontls\\')"]').classList.add('active');
            document.getElementById('nontls-content').style.display = 'block';
        }
    }

    function copyToClipboard(id) {
        const text = document.getElementById(id).textContent;
        navigator.clipboard.writeText(text).then(() => {
            const btn = document.querySelector(\`button[onclick="copyToClipboard('\${id}')"]\`);
            const original = btn.textContent;
            btn.textContent = 'Copied!';
            setTimeout(() => btn.textContent = original, 2000);
        });
    }

    // Init
    updateConfigs();
</script>
</body>
</html>
`;
}
