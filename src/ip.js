export function ipToInt(ip) {
  const parts = String(ip).split(".").map((v) => Number(v));
  if (parts.length !== 4 || parts.some((v) => !Number.isInteger(v) || v < 0 || v > 255)) {
    throw new Error(`IPv4 地址无效：${ip}`);
  }
  return (((parts[0] << 24) >>> 0) | (parts[1] << 16) | (parts[2] << 8) | parts[3]) >>> 0;
}

export function intToIp(v) {
  const n = v >>> 0;
  return `${n >>> 24}.${(n >>> 16) & 255}.${(n >>> 8) & 255}.${n & 255}`;
}

export function parseCidr(cidr) {
  const [ip, prefixText] = String(cidr).split("/");
  const prefix = Number(prefixText);
  if (!Number.isInteger(prefix) || prefix < 1 || prefix > 30) throw new Error(`CIDR 网段无效：${cidr}`);
  const base = ipToInt(ip);
  const mask = (0xffffffff << (32 - prefix)) >>> 0;
  const network = (base & mask) >>> 0;
  const broadcast = (network | (~mask >>> 0)) >>> 0;
  return { cidr, prefix, network, broadcast, gateway: (network + 1) >>> 0 };
}

export function contains(net, ip) {
  const n = ip >>> 0;
  return n >= net.network && n <= net.broadcast;
}

export function parseNetworks(env) {
  const result = new Set();
  const raw = env.NETWORKS || "";
  for (const item of raw.split(",").map((v) => v.trim()).filter(Boolean)) {
    const code = item.split("=")[0].trim();
    if (!code || code.length > 32) throw new Error(`网络编号无效：${code}`);
    result.add(code);
  }
  return result;
}

export function isNetworkAllowed(allowedNetworks, code) {
  return !allowedNetworks.size || allowedNetworks.has(code);
}

export function defaultNetworkConfig(gatewayIp) {
  return networkConfigFromGateway(ipToInt(gatewayIp || "10.46.0.1"));
}

export function networkConfigFromClientIp(ip, defaultGateway) {
  if (ip === undefined || ip === null) return defaultNetworkConfig(defaultGateway);
  const gateway = ((ip >>> 0) & 0xffffff00) + 1;
  return networkConfigFromGateway(gateway >>> 0);
}

function networkConfigFromGateway(gateway) {
  const network = (gateway & 0xffffff00) >>> 0;
  const broadcast = (network | 0xff) >>> 0;
  const prefix = 24;
  return {
    cidr: `${intToIp(network)}/${prefix}`,
    prefix,
    network,
    broadcast,
    gateway: gateway >>> 0
  };
}
