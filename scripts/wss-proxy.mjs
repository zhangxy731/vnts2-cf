import { execFileSync } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
import https from "node:https";
import http from "node:http";

const backendHost = process.env.BACKEND_HOST || "127.0.0.1";
const backendPort = Number(process.env.BACKEND_PORT || 8786);
const proxyPort = Number(process.env.PROXY_PORT || 8787);
const certPath = process.env.PROXY_CERT || "/tmp/vnts2-cf-proxy.crt";
const keyPath = process.env.PROXY_KEY || "/tmp/vnts2-cf-proxy.key";

if (!existsSync(certPath) || !existsSync(keyPath)) {
  execFileSync("openssl", [
    "req",
    "-x509",
    "-newkey",
    "rsa:2048",
    "-nodes",
    "-keyout",
    keyPath,
    "-out",
    certPath,
    "-days",
    "7",
    "-subj",
    "/CN=vnts2-cf-local"
  ]);
}

const server = https.createServer(
  {
    key: readFileSync(keyPath),
    cert: readFileSync(certPath)
  },
  (req, res) => {
    const upstream = http.request(
      {
        host: backendHost,
        port: backendPort,
        method: req.method,
        path: req.url,
        headers: req.headers
      },
      (upstreamRes) => {
        res.writeHead(upstreamRes.statusCode || 502, upstreamRes.headers);
        upstreamRes.pipe(res);
      }
    );
    upstream.on("error", (error) => {
      res.writeHead(502, { "content-type": "text/plain; charset=utf-8" });
      res.end(`代理请求失败：${error.message}`);
    });
    req.pipe(upstream);
  }
);

server.on("upgrade", (req, socket, head) => {
  const upstreamReq = http.request({
    host: backendHost,
    port: backendPort,
    method: req.method,
    path: req.url,
    headers: req.headers
  });
  upstreamReq.on("upgrade", (upstreamRes, upstreamSocket, upstreamHead) => {
    socket.write(`HTTP/${upstreamRes.httpVersion} ${upstreamRes.statusCode} ${upstreamRes.statusMessage}\r\n`);
    for (const [name, value] of Object.entries(upstreamRes.headers)) {
      if (Array.isArray(value)) {
        for (const item of value) socket.write(`${name}: ${item}\r\n`);
      } else if (value !== undefined) {
        socket.write(`${name}: ${value}\r\n`);
      }
    }
    socket.write("\r\n");
    if (upstreamHead.length) socket.write(upstreamHead);
    if (head.length) upstreamSocket.write(head);
    socket.pipe(upstreamSocket);
    upstreamSocket.pipe(socket);
  });
  upstreamReq.on("response", (upstreamRes) => {
    socket.write(`HTTP/${upstreamRes.httpVersion} ${upstreamRes.statusCode} ${upstreamRes.statusMessage}\r\n`);
    for (const [name, value] of Object.entries(upstreamRes.headers)) {
      if (Array.isArray(value)) {
        for (const item of value) socket.write(`${name}: ${item}\r\n`);
      } else if (value !== undefined) {
        socket.write(`${name}: ${value}\r\n`);
      }
    }
    socket.write("\r\n");
    upstreamRes.pipe(socket);
  });
  upstreamReq.on("error", (error) => {
    console.error(`[vnts2-cf] WSS Upgrade 代理失败：${error.message}`);
    socket.destroy();
  });
  upstreamReq.end();
});

server.listen(proxyPort, "0.0.0.0", () => {
  console.log(`[vnts2-cf] WSS 本地代理已启动：0.0.0.0:${proxyPort} -> http://${backendHost}:${backendPort}`);
});
