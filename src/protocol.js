import { ProtoReader, ProtoWriter } from "./protobuf.js";

export const HEAD_LENGTH = 16;
export const MSG = {
  TURN: 1,
  BROADCAST: 2,
  EXCLUDE_BROADCAST: 3,
  TARGET_BROADCAST: 4,
  PING: 5,
  PONG: 6,
  PING_TURN: 7,
  PONG_TURN: 8,
  PUNCH_START_1: 9,
  PUNCH_START_2: 10,
  PUNCH_REQ: 11,
  PUNCH_RES: 12,
  PUSH_CLIENT_IPS: 13,
  RPC_REQ: 14,
  RPC_RES: 15,
  RELAY_PROBE: 16,
  QUIC: 17,
  RELAY_PROBE_REPLY: 18,
  RELAY_PROBE_CLIENT: 18,
  RELAY_PROBE_REPLY_CLIENT: 19
};

export const FLAG_COMPRESSED = 0x80;
export const FLAG_GATEWAY = 0x40;
export const FLAG_FEC = 0x20;

export function parseRequestMessage(bytes) {
  const r = new ProtoReader(bytes);
  const out = {};
  while (!r.eof()) {
    const { field, wire } = r.readTag();
    if (field === 1 && wire === 2) out.reg = parseRegRequest(r.readBytes());
    else if (field === 2 && wire === 2) {
      r.readBytes();
      out.confirmReg = {};
    } else r.skip(wire);
  }
  return out;
}

function parseRegRequest(bytes) {
  const r = new ProtoReader(bytes);
  const reg = { ip: undefined, keySign: undefined, ipVariable: false, serverId: 0, registrationMode: 0 };
  while (!r.eof()) {
    const { field, wire } = r.readTag();
    if (field === 1 && wire === 2) reg.networkCode = r.readString();
    else if (field === 2 && wire === 2) reg.deviceId = r.readString();
    else if (field === 3 && wire === 5) reg.ip = r.readFixed32();
    else if (field === 4 && wire === 2) reg.name = r.readString();
    else if (field === 5 && wire === 2) reg.version = r.readString();
    else if (field === 6 && wire === 2) reg.keySign = r.readString();
    else if (field === 7 && wire === 0) reg.ipVariable = r.readBool();
    else if (field === 8 && wire === 5) reg.serverId = r.readFixed32();
    else if (field === 9 && wire === 0) reg.registrationMode = Number(r.readVarint());
    else r.skip(wire);
  }
  return reg;
}

export function encodeRegResponse({ ip, prefixLen, gateway, serverVersion }) {
  const m = new ProtoWriter();
  m.fixed32(1, ip);
  m.uint(2, prefixLen);
  m.fixed32(3, gateway);
  m.string(4, serverVersion);
  const w = new ProtoWriter();
  w.message(1, m.finish());
  return w.finish();
}

export function encodeErrorResponse(code, message) {
  const e = new ProtoWriter();
  e.uint(1, code);
  e.string(2, message);
  const w = new ProtoWriter();
  w.message(2, e.finish());
  return w.finish();
}

export function encodeConfirmRegResponse(success) {
  const c = new ProtoWriter();
  c.bool(1, success);
  const w = new ProtoWriter();
  w.message(3, c.finish());
  return w.finish();
}

export function encodeClientSimpleInfoList({ dataVersion, list, isAll, time }) {
  const w = new ProtoWriter();
  w.uint64(1, dataVersion);
  for (const item of list) {
    const c = new ProtoWriter();
    c.fixed32(1, item.ip);
    c.bool(2, item.online);
    w.message(2, c.finish());
  }
  w.bool(3, isAll);
  w.int64(4, time || 0);
  return w.finish();
}

export function parseSelectiveBroadcast(bytes) {
  const r = new ProtoReader(bytes);
  const out = { ips: new Set(), data: new Uint8Array(0) };
  while (!r.eof()) {
    const { field, wire } = r.readTag();
    if (field === 1 && wire === 5) out.ips.add(r.readFixed32());
    else if (field === 1 && wire === 2) {
      const packed = new ProtoReader(r.readBytes());
      while (!packed.eof()) out.ips.add(packed.readFixed32());
    } else if (field === 2 && wire === 2) out.data = r.readBytes();
    else r.skip(wire);
  }
  return out;
}

export function parseRpcRequest(bytes) {
  const r = new ProtoReader(bytes);
  const req = { id: 0, clientListReq: false };
  while (!r.eof()) {
    const { field, wire } = r.readTag();
    if (field === 1 && wire === 0) req.id = Number(r.readVarint());
    else if (field === 2 && wire === 2) {
      r.readBytes();
      req.clientListReq = true;
    } else r.skip(wire);
  }
  return req;
}

export function encodeRpcClientListResponse(id, list) {
  const listMsg = new ProtoWriter();
  for (const item of list) {
    const c = new ProtoWriter();
    c.string(1, item.name);
    c.string(2, item.version);
    c.fixed32(3, item.ip);
    c.string(4, item.keySign);
    c.bool(5, item.online);
    c.int64(6, item.lastConnectedTime);
    c.string(7, item.id);
    listMsg.message(1, c.finish());
  }
  const res = new ProtoWriter();
  res.uint64(1, id);
  res.message(2, listMsg.finish());
  return res.finish();
}

export function encodeServerMessage(message) {
  const w = new ProtoWriter();
  if (message.authReq) {
    const m = new ProtoWriter();
    m.string(1, message.authReq.tokenHash);
    w.message(1, m.finish());
  } else if (message.authRes) {
    const m = new ProtoWriter();
    m.bool(1, !!message.authRes.success);
    m.string(2, message.authRes.message || "");
    w.message(2, m.finish());
  } else if (message.pingReq) {
    const m = new ProtoWriter();
    m.uint64(1, message.pingReq.timestamp || 0);
    w.message(4, m.finish());
  } else if (message.pingRes) {
    const m = new ProtoWriter();
    m.uint64(1, message.pingRes.requestTimestamp || 0);
    m.uint64(2, message.pingRes.responseTimestamp || 0);
    w.message(5, m.finish());
  } else if (message.forwardData) {
    const m = new ProtoWriter();
    m.string(1, message.forwardData.networkCode || "");
    m.bytes(2, message.forwardData.data || new Uint8Array(0));
    w.message(6, m.finish());
  } else if (message.clientInfoReq) {
    const m = new ProtoWriter();
    for (const code of message.clientInfoReq.networkCodes || []) m.string(1, code);
    w.message(7, m.finish());
  } else if (message.clientInfoRes) {
    const m = new ProtoWriter();
    for (const network of message.clientInfoRes.networks || []) m.message(1, encodeServerNetworkInfo(network));
    w.message(8, m.finish());
  }
  return w.finish();
}

function encodeServerNetworkInfo(network) {
  const w = new ProtoWriter();
  w.string(1, network.networkCode || "");
  for (const client of network.clients || []) {
    const c = new ProtoWriter();
    c.fixed32(1, client.ip);
    c.uint(2, client.latencyMs || client.latency_ms || 0);
    w.message(2, c.finish());
  }
  return w.finish();
}

export function parseServerMessage(bytes) {
  const r = new ProtoReader(bytes);
  const out = {};
  while (!r.eof()) {
    const { field, wire } = r.readTag();
    if (wire !== 2) {
      r.skip(wire);
      continue;
    }
    const payload = r.readBytes();
    if (field === 1) out.authReq = parseServerAuthRequest(payload);
    else if (field === 2) out.authRes = parseServerAuthResponse(payload);
    else if (field === 4) out.pingReq = parseServerPingRequest(payload);
    else if (field === 5) out.pingRes = parseServerPingResponse(payload);
    else if (field === 6) out.forwardData = parseServerForwardData(payload);
    else if (field === 7) out.clientInfoReq = parseServerClientInfoRequest(payload);
    else if (field === 8) out.clientInfoRes = parseServerClientInfoResponse(payload);
  }
  return out;
}

function parseServerAuthRequest(bytes) {
  const r = new ProtoReader(bytes);
  const out = { tokenHash: "" };
  while (!r.eof()) {
    const { field, wire } = r.readTag();
    if (field === 1 && wire === 2) out.tokenHash = r.readString();
    else r.skip(wire);
  }
  return out;
}

function parseServerAuthResponse(bytes) {
  const r = new ProtoReader(bytes);
  const out = { success: false, message: "" };
  while (!r.eof()) {
    const { field, wire } = r.readTag();
    if (field === 1 && wire === 0) out.success = r.readBool();
    else if (field === 2 && wire === 2) out.message = r.readString();
    else r.skip(wire);
  }
  return out;
}

function parseServerPingRequest(bytes) {
  const r = new ProtoReader(bytes);
  const out = { timestamp: 0 };
  while (!r.eof()) {
    const { field, wire } = r.readTag();
    if (field === 1 && wire === 0) out.timestamp = Number(r.readVarint());
    else r.skip(wire);
  }
  return out;
}

function parseServerPingResponse(bytes) {
  const r = new ProtoReader(bytes);
  const out = { requestTimestamp: 0, responseTimestamp: 0 };
  while (!r.eof()) {
    const { field, wire } = r.readTag();
    if (field === 1 && wire === 0) out.requestTimestamp = Number(r.readVarint());
    else if (field === 2 && wire === 0) out.responseTimestamp = Number(r.readVarint());
    else r.skip(wire);
  }
  return out;
}

function parseServerForwardData(bytes) {
  const r = new ProtoReader(bytes);
  const out = { networkCode: "", data: new Uint8Array(0) };
  while (!r.eof()) {
    const { field, wire } = r.readTag();
    if (field === 1 && wire === 2) out.networkCode = r.readString();
    else if (field === 2 && wire === 2) out.data = r.readBytes();
    else r.skip(wire);
  }
  return out;
}

function parseServerClientInfoRequest(bytes) {
  const r = new ProtoReader(bytes);
  const out = { networkCodes: [] };
  while (!r.eof()) {
    const { field, wire } = r.readTag();
    if (field === 1 && wire === 2) out.networkCodes.push(r.readString());
    else r.skip(wire);
  }
  return out;
}

function parseServerClientInfoResponse(bytes) {
  const r = new ProtoReader(bytes);
  const out = { networks: [] };
  while (!r.eof()) {
    const { field, wire } = r.readTag();
    if (field === 1 && wire === 2) out.networks.push(parseServerNetworkInfo(r.readBytes()));
    else r.skip(wire);
  }
  return out;
}

function parseServerNetworkInfo(bytes) {
  const r = new ProtoReader(bytes);
  const out = { networkCode: "", clients: [] };
  while (!r.eof()) {
    const { field, wire } = r.readTag();
    if (field === 1 && wire === 2) out.networkCode = r.readString();
    else if (field === 2 && wire === 2) out.clients.push(parseClientLatencyInfo(r.readBytes()));
    else r.skip(wire);
  }
  return out;
}

function parseClientLatencyInfo(bytes) {
  const r = new ProtoReader(bytes);
  const out = { ip: 0, latencyMs: 10 };
  while (!r.eof()) {
    const { field, wire } = r.readTag();
    if (field === 1 && wire === 5) out.ip = r.readFixed32();
    else if (field === 2 && wire === 0) out.latencyMs = r.readU32();
    else r.skip(wire);
  }
  return out;
}

export function readPacket(buf) {
  const data = buf instanceof Uint8Array ? buf : new Uint8Array(buf);
  if (data.length < HEAD_LENGTH) throw new Error("数据包长度不足");
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  return {
    data,
    msgType: data[0] & 0x7f,
    maxTtl: data[1] >> 4,
    ttl: data[1] & 0x0f,
    flags: data[2],
    seq: view.getUint32(4, false),
    srcId: view.getUint32(8, false),
    destId: view.getUint32(12, false),
    payload: data.slice(HEAD_LENGTH),
    isGateway: (data[2] & FLAG_GATEWAY) !== 0
  };
}

export function decrementTtl(buf) {
  if ((buf[1] & 0x0f) <= 1) return false;
  buf[1] = (buf[1] & 0xf0) | ((buf[1] & 0x0f) - 1);
  return true;
}

export function makePacket(msgType, payload = new Uint8Array(0), { gateway = false, ttl = 1, src = 0, dest = 0, seq = 0 } = {}) {
  const out = new Uint8Array(HEAD_LENGTH + payload.length);
  const view = new DataView(out.buffer);
  out[0] = 0x80 | (msgType & 0x7f);
  out[1] = ((ttl & 0x0f) << 4) | (ttl & 0x0f);
  out[2] = gateway ? FLAG_GATEWAY : 0;
  view.setUint32(4, seq >>> 0, false);
  view.setUint32(8, src >>> 0, false);
  view.setUint32(12, dest >>> 0, false);
  out.set(payload, HEAD_LENGTH);
  return out;
}
