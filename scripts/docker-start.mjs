import { execFileSync, spawn } from "node:child_process";
import { readFileSync, writeFileSync } from "node:fs";

const configurableVars = [
  "NETWORKS",
  "LEASE_DURATION",
  "SERVER_VERSION",
  "LOCATION_HINT",
  "PEER_SERVERS",
  "SERVER_TOKEN",
  "MAINTENANCE_INTERVAL",
  "LOG_LEVEL",
  "LOG_PASSWORD",
  "DISABLE_RELAY"
];

let toml = readFileSync("wrangler.toml", "utf8");
const changed = [];

for (const key of configurableVars) {
  if (!Object.prototype.hasOwnProperty.call(process.env, key)) continue;
  const value = escapeTomlString(process.env[key] ?? "");
  const pattern = new RegExp(`^${key}\\s*=\\s*".*"$`, "m");
  const line = `${key} = "${value}"`;
  if (pattern.test(toml)) toml = toml.replace(pattern, line);
  else toml = toml.replace(/\[vars\]\n/, `[vars]\n${line}\n`);
  changed.push(key);
}

if (changed.length) {
  writeFileSync("wrangler.toml", toml);
  console.log(`[vnts2-cf] Docker 启动参数已写入 wrangler.toml：${changed.join(", ")}`);
}

if (process.env.LOCAL_PROTOCOL === "http-wss-proxy") {
  const internalPort = process.env.INTERNAL_PORT || "8786";
  const publicPort = process.env.PUBLIC_PORT || "8787";
  console.log(`[vnts2-cf] Docker 本地测试模式：Worker HTTP=${internalPort}，WSS 代理=${publicPort}`);
  const worker = spawn("./node_modules/.bin/wrangler", ["dev", "--ip", "0.0.0.0", "--port", internalPort, "--local-protocol", "http"], {
    stdio: "inherit"
  });
  const proxy = spawn("node", ["scripts/wss-proxy.mjs"], {
    stdio: "inherit",
    env: { ...process.env, BACKEND_PORT: internalPort, PROXY_PORT: publicPort }
  });
  const stop = (code = 0) => {
    worker.kill("SIGTERM");
    proxy.kill("SIGTERM");
    process.exit(code);
  };
  worker.on("exit", (code) => stop(code ?? 0));
  proxy.on("exit", (code) => stop(code ?? 0));
} else {
  execFileSync("npm", ["run", "dev"], { stdio: "inherit" });
}

function escapeTomlString(value) {
  return String(value).replace(/\\/g, "\\\\").replace(/"/g, '\\"');
}
