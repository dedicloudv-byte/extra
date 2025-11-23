
// Cloudflare Worker for VLESS
// Supports VLESS over WebSocket
//
// Instructions:
// 1. Set your UUID below (const userID = ...).
// 2. Deploy to Cloudflare Workers.
// 3. Use a VLESS client (e.g., v2rayN, Shadowrocket) with:
//    - Address: Your Worker domain
//    - Port: 443
//    - User ID: The UUID set below
//    - Transport: ws
//    - Path: / (or whatever path you handle, this script handles all paths for WS)
//    - TLS: on

const userID = '841d0c38-1352-4090-95ad-3516c53170b0';

let proxyIP = '';

export default {
  /**
   * @param {Request} request
   * @param {any} env
   * @param {ExecutionContext} ctx
   * @returns {Promise<Response>}
   */
  async fetch(request, env, ctx) {
    try {
      const upgradeHeader = request.headers.get('Upgrade');
      if (!upgradeHeader || upgradeHeader !== 'websocket') {
        const url = new URL(request.url);
        switch (url.pathname) {
          case '/':
            return new Response('VLESS Worker is running', { status: 200 });
          default:
            return new Response('Not Found', { status: 404 });
        }
      }

      // Handle WebSocket for VLESS
      return await vlessOverWSHandler(request);

    } catch (err) {
      return new Response(err.toString(), { status: 500 });
    }
  },
};

async function vlessOverWSHandler(request) {
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
        // console.error(message);
        // controller.error(message);
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

  // In a real implementation, we should validate the UUID.
  // For this simple script, we skip strict UUID validation against the userID constant
  // to allow flexibility or we can strictly check it.
  // Let's strictly check it for security.
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
  // skip opt for now

  const command = new Uint8Array(
    vlessBuffer.slice(18 + optLength, 18 + optLength + 1)
  )[0];

  // 0x01 TCP, 0x02 UDP
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

  // 1--> ipv4  addressLength =4
  // 2--> domain name addressLength=addressBuffer[1]
  // 3--> ipv6  addressLength =16
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
      // 2001:0db8:85a3:0000:0000:8a2e:0370:7334
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
    // Cloudflare connect()
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

  // if the VLESS client sends data immediately, we send it to the remote.
  // If not, we still connect.

  const tcpSocket = await connectAndWrite(addressRemote, portRemote);

  // Send the VLESS response header back to client indicating success
  // This is critical for VLESS
  // Version + 0 (success)
  // We assume successful connection if we reached here.
  // Note: Cloudflare connect returns immediately, actual connection might happen on write.
  // But we need to send the response header to client so it knows to proceed.

  /*
     The VLESS response:
     1 byte version
     1 byte addon length (0)
     ...
  */

  // Note: Some scripts send the response header only after first data from remote?
  // Standard VLESS over WS usually expects the response header.

  // However, if we look at standard implementations, often we simply pipe data back.
  // But strict VLESS requires the response header "Version + AddonsLength(0)" at the beginning of the stream from server.

  // Let's inject the response header into the stream back to the client.

  // However, since we are using `tcpSocket.readable.pipeTo(webSocket)`, we need to be careful.
  // We can write to the websocket directly first.

  // But `webSocket` is already being read from in `makeReadableWebSocketStream`...
  // No, `webSocket` is full-duplex. We can write to it.

  // Wait, `vlessResponseHeader` passed in is [version, 0].
  // We should send this first.
  // webSocket.send(vlessResponseHeader); // This might not be reliable if we are piping.

  // Let's use a TransformStream or just write to the websocket.
  // But `webSocket` in Cloudflare Workers doesn't have a standard WritableStream interface directly attached
  // in the way `pipeTo` expects unless we wrap it?
  // Actually `webSocket` object has `send()`.

  // But better to use `webSocketPair` semantics.
  // `client` is returned to user. `webSocket` is our side.

  // We can do:
  // tcpSocket.readable.pipeTo(new WritableStream({ write(chunk) { webSocket.send(chunk) } }))

  // To inject header:
  // webSocket.send(vlessResponseHeader);
  // tcpSocket.readable....

  // But we need to ensure order.

  await vlessRemoteSocketToWS(tcpSocket, webSocket, vlessResponseHeader, null, log);
}

async function vlessRemoteSocketToWS(remoteSocket, webSocket, vlessResponseHeader, retry, log) {
  // remoteSocket is the TCP socket from `connect()`.
  // It has `readable` and `writable`.

  let hasHeaderSent = false;

  await remoteSocket.readable.pipeTo(new WritableStream({
    start() {
    },
    async write(chunk, controller) {
      if (hasHeaderSent) {
        webSocket.send(chunk);
      } else {
        // combine header and chunk
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
    // go use modified Base64 for URL rfc4648 which js atob not support
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
