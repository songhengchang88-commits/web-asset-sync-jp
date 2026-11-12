import { connect } from 'cloudflare:sockets';

// 专属固定 UUID（与你的客户端配置完全匹配）
const USER_ID = '7c9e6679-7425-40de-944b-e07fc1f90ae7';

// 实测存活的日本东京/横滨落地节点池
const JP_PROXY_POOL = [
  'ProxyIP.JP.CMLiussss.net',
  'proxyip.jp.fxxk.dedyn.io',
  '138.2.31.37',
  '45.66.216.47',
  '54.95.106.234',
];

export default {
  async fetch(request, env, ctx) {
    try {
      const upgradeHeader = request.headers.get('Upgrade');
      if (!upgradeHeader || upgradeHeader.toLowerCase() !== 'websocket') {
        const url = new URL(request.url);
        if (url.pathname === '/health') {
          return new Response(JSON.stringify({ status: 'ok', region: 'JP' }), {
            status: 200,
            headers: { 'Content-Type': 'application/json' },
          });
        }
        return new Response(
          '<!DOCTYPE html><html><head><title>Welcome to nginx!</title></head><body><h1>Welcome to nginx!</h1><p>If you see this page, the web server is successfully installed and working.</p></body></html>',
          { status: 200, headers: { 'Content-Type': 'text/html; charset=utf-8' } }
        );
      }

      const url = new URL(request.url);
      const customProxy = url.searchParams.get('proxyip') || env.PROXYIP || '';
      const activeProxyIP = customProxy
        ? customProxy.split(',')[0].trim()
        : JP_PROXY_POOL[Math.floor(Math.random() * JP_PROXY_POOL.length)];
      const activeUserID = (env.UUID || USER_ID).toLowerCase();

      return await handleVlessWebSocket(request, activeUserID, activeProxyIP);
    } catch (err) {
      return new Response(err.toString(), { status: 500 });
    }
  },
};

async function handleVlessWebSocket(request, userID, proxyIP) {
  const webSocketPair = new WebSocketPair();
  const [client, webSocket] = Object.values(webSocketPair);

  webSocket.accept();

  let address = '';
  let portWithRandomLog = '';
  const log = (info, event) => {
    // quiet in production
  };
  const earlyDataHeader = request.headers.get('sec-websocket-protocol') || '';
  const readableWebSocketStream = makeReadableWebSocketStream(webSocket, earlyDataHeader, log);

  let remoteSocketWapper = { value: null };
  let udpStreamWrite = null;
  let isDns = false;

  readableWebSocketStream
    .pipeTo(
      new WritableStream({
        async write(chunk, controller) {
          if (isDns && udpStreamWrite) {
            return udpStreamWrite(chunk);
          }
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
            throw new Error(message);
          }

          if (isUDP) {
            if (portRemote === 53) {
              isDns = true;
            } else {
              throw new Error('UDP proxy only enable for DNS which is port 53');
            }
          }

          const vlessResponseHeader = new Uint8Array([vlessVersion[0], 0]);
          const rawClientData = chunk.slice(rawDataIndex);

          if (isDns) {
            const { write } = await handleUDPOutBound(webSocket, vlessResponseHeader, log);
            udpStreamWrite = write;
            udpStreamWrite(rawClientData);
            return;
          }

          await handleTCPOutBound(
            remoteSocketWapper,
            addressRemote,
            portRemote,
            rawClientData,
            webSocket,
            vlessResponseHeader,
            proxyIP,
            log
          );
        },
        close() {},
        abort() {},
      })
    )
    .catch(() => {});

  return new Response(null, {
    status: 101,
    webSocket: client,
  });
}

async function handleTCPOutBound(
  remoteSocket,
  addressRemote,
  portRemote,
  rawClientData,
  webSocket,
  vlessResponseHeader,
  proxyIP,
  log
) {
  async function connectAndWrite(address, port) {
    const tcpSocket = connect({ hostname: address, port: port });
    remoteSocket.value = tcpSocket;
    const writer = tcpSocket.writable.getWriter();
    await writer.write(rawClientData);
    writer.releaseLock();
    return tcpSocket;
  }

  async function fallbackDirect() {
    const tcpSocket = await connectAndWrite(addressRemote, portRemote);
    tcpSocket.closed.catch(() => {}).finally(() => {
      safeCloseWebSocket(webSocket);
    });
    remoteSocketToWS(tcpSocket, webSocket, vlessResponseHeader, null, log);
  }

  // 默认强制优先从日本东京 ProxyIP 出站（确保 Polymarket 与 Gemini 全量解锁且归属地为日本）
  const targetHost = proxyIP || JP_PROXY_POOL[0];
  const [pHost, pPort] = targetHost.includes(':') ? targetHost.split(':') : [targetHost, portRemote];
  try {
    const tcpSocket = await connectAndWrite(pHost, Number(pPort) || portRemote);
    remoteSocketToWS(tcpSocket, webSocket, vlessResponseHeader, fallbackDirect, log);
  } catch (err) {
    await fallbackDirect();
  }
}

function makeReadableWebSocketStream(webSocketServer, earlyDataHeader, log) {
  let readableStreamCancel = false;
  return new ReadableStream({
    start(controller) {
      webSocketServer.addEventListener('message', (event) => {
        if (readableStreamCancel) return;
        controller.enqueue(event.data);
      });
      webSocketServer.addEventListener('close', () => {
        safeCloseWebSocket(webSocketServer);
        if (readableStreamCancel) return;
        controller.close();
      });
      webSocketServer.addEventListener('error', (err) => {
        controller.error(err);
      });
      const { earlyData, error } = base64ToArrayBuffer(earlyDataHeader);
      if (error) {
        controller.error(error);
      } else if (earlyData) {
        controller.enqueue(earlyData);
      }
    },
    cancel() {
      readableStreamCancel = true;
      safeCloseWebSocket(webSocketServer);
    },
  });
}

function processVlessHeader(vlessBuffer, userID) {
  if (vlessBuffer.byteLength < 24) {
    return { hasError: true, message: 'invalid data' };
  }
  const version = new Uint8Array(vlessBuffer.slice(0, 1));
  const slicedBuffer = new Uint8Array(vlessBuffer.slice(1, 17));
  const slicedHex = Array.from(slicedBuffer)
    .map((b) => b.toString(16).padStart(2, '0'))
    .join('');
  const formattedUUID = `${slicedHex.slice(0, 8)}-${slicedHex.slice(8, 12)}-${slicedHex.slice(12, 16)}-${slicedHex.slice(16, 20)}-${slicedHex.slice(20)}`;
  if (formattedUUID !== userID) {
    return { hasError: true, message: 'invalid user' };
  }
  const optLength = new Uint8Array(vlessBuffer.slice(17, 18))[0];
  const command = new Uint8Array(vlessBuffer.slice(18 + optLength, 18 + optLength + 1))[0];
  let isUDP = false;
  if (command === 1) {
    isUDP = false;
  } else if (command === 2) {
    isUDP = true;
  } else {
    return { hasError: true, message: `command ${command} is not support` };
  }
  const portIndex = 18 + optLength + 1;
  const portBuffer = vlessBuffer.slice(portIndex, portIndex + 2);
  const portRemote = new DataView(portBuffer).getUint16(0);

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
    case 3: {
      addressLength = 16;
      const dataView = new DataView(vlessBuffer.slice(addressValueIndex, addressValueIndex + addressLength));
      const ipv6 = [];
      for (let i = 0; i < 8; i++) {
        ipv6.push(dataView.getUint16(i * 2).toString(16));
      }
      addressValue = ipv6.join(':');
      break;
    }
    default:
      return { hasError: true, message: `invild addressType is ${addressType}` };
  }
  if (!addressValue) {
    return { hasError: true, message: `addressValue is empty` };
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

async function remoteSocketToWS(remoteSocket, webSocket, vlessResponseHeader, retry, log) {
  let vlessHeader = vlessResponseHeader;
  let hasIncomingData = false;
  await remoteSocket.readable
    .pipeTo(
      new WritableStream({
        async write(chunk) {
          hasIncomingData = true;
          if (webSocket.readyState !== 1) return;
          if (vlessHeader) {
            const combined = new Uint8Array(vlessHeader.byteLength + chunk.byteLength);
            combined.set(new Uint8Array(vlessHeader), 0);
            combined.set(new Uint8Array(chunk), vlessHeader.byteLength);
            webSocket.send(combined.buffer);
            vlessHeader = null;
          } else {
            webSocket.send(chunk);
          }
        },
        close() {},
        abort() {},
      })
    )
    .catch(() => {
      safeCloseWebSocket(webSocket);
    });

  if (!hasIncomingData && retry) {
    await retry();
  }
}

function base64ToArrayBuffer(base64Str) {
  if (!base64Str) return { error: null };
  try {
    const b64 = base64Str.replace(/-/g, '+').replace(/_/g, '/');
    const decode = atob(b64);
    const arryBuffer = Uint8Array.from(decode, (c) => c.charCodeAt(0));
    return { earlyData: arryBuffer.buffer, error: null };
  } catch (error) {
    return { error };
  }
}

function safeCloseWebSocket(socket) {
  try {
    if (socket.readyState === 1 || socket.readyState === 2) {
      socket.close();
    }
  } catch (e) {}
}

async function handleUDPOutBound(webSocket, vlessResponseHeader, log) {
  let isVlessHeaderSent = false;
  const transformStream = new TransformStream({
    transform(chunk, controller) {
      for (let index = 0; index < chunk.byteLength; ) {
        const lengthBuffer = chunk.slice(index, index + 2);
        const udpPaketLength = new DataView(lengthBuffer).getUint16(0);
        const udpData = new Uint8Array(chunk.slice(index + 2, index + 2 + udpPaketLength));
        index = index + 2 + udpPaketLength;
        controller.enqueue(udpData);
      }
    },
  });

  transformStream.readable
    .pipeTo(
      new WritableStream({
        async write(chunk) {
          const resp = await fetch('https://1.1.1.1/dns-query', {
            method: 'POST',
            headers: { 'content-type': 'application/dns-message' },
            body: chunk,
          });
          const dnsQueryResult = await resp.arrayBuffer();
          const udpSize = dnsQueryResult.byteLength;
          const udpSizeBuffer = new Uint8Array([(udpSize >> 8) & 0xff, udpSize & 0xff]);
          if (webSocket.readyState === 1) {
            if (isVlessHeaderSent) {
              const combined = new Uint8Array(udpSizeBuffer.byteLength + dnsQueryResult.byteLength);
              combined.set(udpSizeBuffer, 0);
              combined.set(new Uint8Array(dnsQueryResult), udpSizeBuffer.byteLength);
              webSocket.send(combined.buffer);
            } else {
              const combined = new Uint8Array(vlessResponseHeader.byteLength + udpSizeBuffer.byteLength + dnsQueryResult.byteLength);
              combined.set(new Uint8Array(vlessResponseHeader), 0);
              combined.set(udpSizeBuffer, vlessResponseHeader.byteLength);
              combined.set(new Uint8Array(dnsQueryResult), vlessResponseHeader.byteLength + udpSizeBuffer.byteLength);
              webSocket.send(combined.buffer);
              isVlessHeaderSent = true;
            }
          }
        },
      })
    )
    .catch(() => {});

  const writer = transformStream.writable.getWriter();
  return {
    write(chunk) {
      writer.write(chunk);
    },
  };
}
