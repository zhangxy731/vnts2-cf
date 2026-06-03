import test from "node:test";
import assert from "node:assert/strict";
import { encodeRegResponse, encodeRpcClientListResponse, encodeServerMessage, parseRequestMessage, parseServerMessage, readPacket, makePacket, MSG } from "../src/protocol.js";
import { ProtoReader, ProtoWriter } from "../src/protobuf.js";
import { defaultNetworkConfig, intToIp, ipToInt, isNetworkAllowed, networkConfigFromClientIp, parseNetworks } from "../src/ip.js";

test("解码 vnts2 注册请求", () => {
  const reg = new ProtoWriter();
  reg.string(1, "default");
  reg.string(2, "dev-1");
  reg.fixed32(3, ipToInt("10.26.0.20"));
  reg.string(4, "node");
  reg.string(5, "0.2.0");
  reg.string(6, "Use-A-Long-Strong-Secret-At-Least-24-Chars!");
  reg.bool(7, true);
  reg.fixed32(8, 2);
  reg.uint(9, 1);
  const req = new ProtoWriter();
  req.message(1, reg.finish());

  const msg = parseRequestMessage(req.finish());
  assert.equal(msg.reg.networkCode, "default");
  assert.equal(msg.reg.deviceId, "dev-1");
  assert.equal(msg.reg.ip, ipToInt("10.26.0.20"));
  assert.equal(msg.reg.ipVariable, true);
  assert.equal(msg.reg.registrationMode, 1);
});

test("编码注册响应 oneof", () => {
  const bytes = encodeRegResponse({
    ip: ipToInt("10.26.0.2"),
    prefixLen: 24,
    gateway: ipToInt("10.26.0.1"),
    serverVersion: "test"
  });
  assert.equal(bytes[0], 0x0a);
  assert.ok(bytes.length > 8);
});

test("空客户端列表 RPC 仍保留 oneof 字段", () => {
  const bytes = encodeRpcClientListResponse(1, []);
  assert.deepEqual(Array.from(bytes), [0x08, 0x01, 0x12, 0x00]);
});

test("解析 vnts2 16 字节数据包头", () => {
  const packet = makePacket(MSG.TURN, new Uint8Array([1, 2, 3]), {
    ttl: 6,
    src: ipToInt("10.26.0.2"),
    dest: ipToInt("10.26.0.3")
  });
  const parsed = readPacket(packet);
  assert.equal(parsed.msgType, MSG.TURN);
  assert.equal(parsed.ttl, 6);
  assert.equal(parsed.srcId, ipToInt("10.26.0.2"));
  assert.equal(parsed.destId, ipToInt("10.26.0.3"));
  assert.deepEqual(Array.from(parsed.payload), [1, 2, 3]);
});

test("编码和解析 vnts2 服务端互联 ServerMessage", () => {
  const packet = makePacket(MSG.TURN, new Uint8Array([9, 8, 7]), {
    ttl: 6,
    src: ipToInt("10.26.0.2"),
    dest: ipToInt("10.26.0.3")
  });
  const bytes = encodeServerMessage({
    forwardData: {
      networkCode: "default",
      data: packet
    }
  });
  const msg = parseServerMessage(bytes);
  assert.equal(msg.forwardData.networkCode, "default");
  const parsedPacket = readPacket(msg.forwardData.data);
  assert.equal(parsedPacket.msgType, MSG.TURN);
  assert.equal(parsedPacket.destId, ipToInt("10.26.0.3"));

  const infoBytes = encodeServerMessage({
    clientInfoRes: {
      networks: [{ networkCode: "default", clients: [{ ip: ipToInt("10.26.0.2"), latencyMs: 12 }] }]
    }
  });
  const info = parseServerMessage(infoBytes);
  assert.equal(info.clientInfoRes.networks[0].clients[0].latencyMs, 12);
});

test("NETWORKS 只限制网络编号", () => {
  const allowed = parseNetworks({ NETWORKS: "default,1234,office=legacy-cidr=legacy-secret" });
  assert.equal(isNetworkAllowed(allowed, "default"), true);
  assert.equal(isNetworkAllowed(allowed, "1234"), true);
  assert.equal(isNetworkAllowed(allowed, "office"), true);
  assert.equal(isNetworkAllowed(allowed, "other"), false);

  const open = parseNetworks({ NETWORKS: "" });
  assert.equal(isNetworkAllowed(open, "any-room"), true);
});

test("网络配置由默认值或首个客户端 IP 推导", () => {
  const fallback = defaultNetworkConfig();
  assert.equal(fallback.cidr, "10.46.0.0/24");
  assert.equal(intToIp(fallback.gateway), "10.46.0.1");

  const custom = networkConfigFromClientIp(ipToInt("10.88.9.20"));
  assert.equal(custom.cidr, "10.88.9.0/24");
  assert.equal(intToIp(custom.gateway), "10.88.9.1");
});

test("畸形 protobuf 和数据包会快速报错", () => {
  assert.throws(() => new ProtoReader(new Uint8Array([0x80])).readVarint(), /意外 EOF/);
  assert.throws(() => new ProtoReader(new Uint8Array(12).fill(0x80)).readVarint(), /varint 过长/);
  assert.throws(() => readPacket(new Uint8Array(15)), /数据包长度不足/);
});

test("ProtoWriter 安全处理大字段并限制消息大小", () => {
  const writer = new ProtoWriter();
  writer.bytes(1, new Uint8Array(256 * 1024));
  assert.ok(writer.finish().length > 256 * 1024);

  const oversized = new ProtoWriter();
  assert.throws(() => oversized.bytes(1, new Uint8Array(2 * 1024 * 1024 + 1)), /超过大小限制/);
});
