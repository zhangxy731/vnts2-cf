import {
  MSG,
  decrementTtl,
  encodeClientSimpleInfoList,
  encodeConfirmRegResponse,
  encodeErrorResponse,
  encodeRegResponse,
  encodeRpcClientListResponse,
  encodeServerMessage,
  makePacket,
  parseServerMessage,
  parseRequestMessage,
  parseRpcRequest,
  parseSelectiveBroadcast,
  readPacket
} from "./protocol.js";
import { contains, defaultNetworkConfig, intToIp, isNetworkAllowed, networkConfigFromClientIp, parseNetworks } from "./ip.js";
import { SERVER_VERSION as GEN_VERSION } from "./version.js";

const REG_NORMAL = 0;
const REG_PRE_REGISTER = 1;
const STORAGE_KEY = "vnts2-state";
const MAX_MESSAGE_BYTES = 1024 * 1024;
const MAX_SESSIONS = 1024;
const MAX_NETWORKS = 1024;
const MAX_STORED_DEVICES = 4096;

export class Vnts2Room {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.configErrors = [];
    try {
      this.allowedNetworks = parseNetworks(env);
    } catch (error) {
      this.allowedNetworks = new Set(["__invalid_network_configuration__"]);
      this.configErrors.push(`NETWORKS 配置无效：${errorMessage(error)}`);
    }
    this.networks = new Map();
    this.sessions = new Map();
    this.nextSessionId = 1;
    this.initialized = false;
    this.serverVersion = env.SERVER_VERSION || GEN_VERSION;
    this.defaultGatewayIp = env.GATEWAY || "10.46.0.1";
    this.leaseDurationMs = positiveSeconds(env.LEASE_DURATION, 86400, 10) * 1000;
    this.peerServers = String(env.PEER_SERVERS || "")
      .split(",")
      .map((v) => v.trim().replace(/\/+$/, ""))
      .filter(Boolean);
    this.peerToken = env.SERVER_TOKEN || "";
    this.peerTokenHash = "";
    this.peerClientCache = new Map();
    this.peerClientCacheTime = 0;
    this.peerClientCacheSignature = "";
    this.peerServerStatus = new Map();
    this.logLevel = String(env.LOG_LEVEL || "info").toLowerCase();
    this.maintenanceIntervalMs = positiveSeconds(env.MAINTENANCE_INTERVAL, 15, 5) * 1000;
    this.logs = [];
    this.logPassword = String(env.LOG_PASSWORD || "");
    this.disableRelay = parseBool(env.DISABLE_RELAY || "");
    this.lastError = "";
    this.startTime = Date.now();
    this._dataDirty = false;
    this._logPersistScheduled = false;
  }

  async fetch(request) {
    try {
      await this.init();
      const url = new URL(request.url);
      if (url.pathname.startsWith("/peer/")) return await this.handlePeerRequest(request, url);
      if (url.pathname === "/peer") return await this.handlePeerPage(request, url);
      if (request.headers.get("Upgrade") === "websocket") return this.acceptWebSocket(request);
      if (url.pathname === "/test") return await this.handleTestPage(request, url);
      if (url.pathname === "/room") return await this.handleRoomPage(request, url);
      if (url.pathname === "/log") return await this.handleLogPage(request, url);
      if (url.pathname === "/log/clear") return await this.handleLogClear(request, url);
      return Response.redirect("https://github.com/lmq8267/vnts2-cf", 302);
    } catch (error) {
      this.reportError("请求处理失败", error);
      return Response.json({ ok: false, error: "请求处理失败" }, { status: 500 });
    }
  }

  async init() {
    if (this.initialized) return;
    const saved = await this.state.storage.get(STORAGE_KEY);
    let restoredDevices = 0;
    if (saved?.networks && typeof saved.networks === "object" && !Array.isArray(saved.networks)) {
      for (const [code, value] of Object.entries(saved.networks)) {
        try {
          if (this.networks.size >= MAX_NETWORKS || restoredDevices >= MAX_STORED_DEVICES) break;
          if (!code || code.length > 32 || !isNetworkAllowed(this.allowedNetworks, code) || !value || typeof value !== "object") continue;
          const net = this.ensureNetwork(code, storedNetworkConfig(value.config, this.defaultGatewayIp));
          net.dataVersion = safeNonNegativeInteger(value.dataVersion);
          for (const record of Array.isArray(value.devices) ? value.devices : []) {
            if (restoredDevices >= MAX_STORED_DEVICES) break;
            if (!record || typeof record.deviceId !== "string" || !record.deviceId || record.deviceId.length > 64 || !isUint32(record.ip)) continue;
            if (record.ip === net.config.gateway || !contains(net.config, record.ip) || net.ipToDevice.has(record.ip >>> 0)) continue;
            net.devices.set(record.deviceId, {
              ...record,
              ip: record.ip >>> 0,
              online: false,
              socket: undefined,
              sessionId: undefined,
              disconnectTime: safeTimestamp(record.disconnectTime, Date.now())
            });
            net.ipToDevice.set(record.ip >>> 0, record.deviceId);
            restoredDevices += 1;
          }
        } catch (error) {
          this.reportError(`恢复网络状态失败 网络编号=${code}`, error);
        }
      }
    }
    this.initialized = true;
    if (this.peerToken) this.peerTokenHash = await sha256Hex(this.peerToken);
    // 先恢复日志，再记录启动日志，避免被 restoreLogs 覆盖
    await this.restoreLogs();
    this.logDebug(`存储恢复完毕 网络数=${this.networks.size} 设备数=${this.totalDeviceCount()}`);
    await this.scheduleAlarm();
    for (const message of this.configErrors) this.reportError("配置错误", new Error(message));
  }

  async alarm() {
    try {
      await this.cleanupExpired();
      await this.refreshPeerClientCache(true);
      this.pingLocalClients();
    } catch (error) {
      this.reportError("维护任务失败", error);
    } finally {
      try {
        // 持久化网络状态
        const dirty = this._dataDirty;
        await this.persist();
        if (dirty) this.logDebug(`持久化保存 网络数=${this.networks.size} 设备数=${this.totalDeviceCount()}`);
        // 持久化日志（兜底，防止日志在 DO 休眠前未写入）
        if (!this._logPersistScheduled && this.logs.length > 0) {
          this._logPersistScheduled = true;
          await this.state.storage.put("vnts2-logs", this.logs.slice());
          this._logPersistScheduled = false;
        }
      } catch (error) {
        this.reportError("数据持久化保存失败", error);
      }
      await this.scheduleAlarm();
    }
  }

  acceptWebSocket(request) {
    if (this.sessions.size >= MAX_SESSIONS) return Response.json({ ok: false, error: "服务端连接数已达到上限" }, { status: 503 });
    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];
    server.accept();
    const sessionId = this.nextSessionId++;
    const remote = getClientIp(request);
    this.sessions.set(sessionId, { id: sessionId, socket: server, registered: false, remote });
    this.logInfo(`收到 WebSocket 连接，会话=${sessionId} 来源=${remote || "未知"}`);
    server.addEventListener("message", (event) => this.handleMessage(sessionId, event.data).catch((error) => this.closeWithError(sessionId, error).catch((closeError) => this.reportError(`关闭异常会话失败 会话=${sessionId}`, closeError))));
    server.addEventListener("close", (event) => {
      const promise = this.offline(sessionId);
      if (typeof this.state.waitUntil === "function") this.state.waitUntil(promise);
      promise.catch((error) => this.reportError(`处理客户端离线失败 会话=${sessionId}`, error));
    });
    server.addEventListener("error", (event) => {
      const promise = this.offline(sessionId);
      if (typeof this.state.waitUntil === "function") this.state.waitUntil(promise);
      promise.catch((error) => this.reportError(`处理 WebSocket 错误失败 会话=${sessionId}`, error));
    });
    return new Response(null, { status: 101, webSocket: client });
  }

  async handleMessage(sessionId, raw) {
    const session = this.sessions.get(sessionId);
    if (!session) return;
    const bytes = await toBytes(raw);
    if (bytes.length > MAX_MESSAGE_BYTES) throw new Error(`消息超过大小限制：${bytes.length} > ${MAX_MESSAGE_BYTES}`);
    if (!session.registered) {
      await this.handleRegister(session, bytes);
      return;
    }
    if (session.pendingConfirmation) {
      const req = tryParseRequest(bytes);
      if (req?.confirmReg) {
        session.pendingConfirmation = false;
        session.registrationStatus = "confirmed";
        this.send(session, encodeConfirmRegResponse(true));
        await this.persistSoon();
      }
      return;
    }
    await this.handleData(session, bytes);
  }

  async handleRegister(session, bytes) {
    const req = parseRequestMessage(bytes);
    if (!req.reg) throw new Error("首包必须是注册请求");
    const reg = req.reg;
    this.validateReg(reg);
    if (!isNetworkAllowed(this.allowedNetworks, reg.networkCode)) {
      throw new Error(`network_code '${reg.networkCode}' is not allowed by server configuration or database`);
    }

    const isNewNetwork = !this.networks.has(reg.networkCode);
    if (isNewNetwork && this.networks.size >= MAX_NETWORKS) throw new Error("服务端网络数量已达到上限");
    const net = this.ensureNetwork(reg.networkCode, networkConfigFromClientIp(reg.ip, this.defaultGatewayIp));
    const cfg = net.config;
    let allocation;
    try {
      if (!net.devices.has(reg.deviceId) && this.totalDeviceCount() >= MAX_STORED_DEVICES) throw new Error("服务端设备数量已达到上限");
      allocation = this.allocateIp(net, cfg, reg, session.id);
    } catch (error) {
      if (isNewNetwork && !net.devices.size) this.networks.delete(reg.networkCode);
      throw error;
    }
    const { ip, device } = allocation;
    this._dataDirty = true;
    session.registered = true;
    session.networkCode = reg.networkCode;
    session.deviceId = reg.deviceId;
    session.ip = ip;
    session.pendingConfirmation = reg.registrationMode === REG_PRE_REGISTER;
    session.registrationStatus = session.pendingConfirmation ? "pending" : "confirmed";
    device.socket = session.socket;
    device.sessionId = session.id;
    device.online = true;

    this.send(session, encodeRegResponse({ ip, prefixLen: cfg.prefix, gateway: cfg.gateway, serverVersion: this.serverVersion }));
    this.logInfo(`注册成功 网络编号=${reg.networkCode} 设备ID=${reg.deviceId} 虚拟IP=${intToIp(ip)} 注册模式=${session.pendingConfirmation ? "预注册" : "普通"} 客户端版本=${reg.version || "未知"}${reg.name ? ` 设备名称=${reg.name}` : ""}`);
    if (!session.pendingConfirmation) {
      await this.persistSoon();
      this.logDebug(`注册后持久化完成 网络编号=${reg.networkCode} 设备ID=${reg.deviceId} 虚拟IP=${intToIp(ip)}`);
    }
  }

  validateReg(reg) {
    if (!reg.networkCode) throw new Error("网络编号不能为空");
    if (reg.networkCode.length > 32) throw new Error("网络编号长度不能超过 32 个字符");
    if (!reg.deviceId) throw new Error("设备 ID 不能为空");
    if (reg.deviceId.length > 64) throw new Error("设备 ID 长度不能超过 64 个字符");
    if ((reg.name || "").length > 128) throw new Error("设备名称长度不能超过 128 个字符");
    if ((reg.version || "").length > 32) throw new Error("客户端版本长度不能超过 32 个字符");
  }

  allocateIp(net, cfg, reg, sessionId) {
    const existing = net.devices.get(reg.deviceId);
    const expected = reg.ip;
    const currentMatches = existing && (expected === undefined || existing.ip === expected);
    if (currentMatches) {
      if (!existing.ip) existing.ip = this.findAvailableIp(net, cfg);
      existing.name = reg.name || "";
      existing.version = reg.version || "";
      existing.keySign = reg.keySign;
      existing.lastConnectedTime = unixSeconds();
      existing.disconnectTime = undefined;
      existing.sessionId = sessionId;
      existing.online = true;
      net.ipToDevice.set(existing.ip, reg.deviceId);
      this.bump(net, existing);
      return { ip: existing.ip, device: existing };
    }

    const oldIp = existing?.ip;
    let ip = expected;
    if (ip !== undefined) {
      if (ip === cfg.gateway) {
        if (!reg.ipVariable) throw new Error("此IP为网关IP，不允许使用");
        ip = undefined;
      } else if (!contains(cfg, ip)) {
        if (!reg.ipVariable) throw new Error(`IP网段错误，应使用${cfg.cidr}网段中的IP`);
        ip = undefined;
      } else if (net.ipToDevice.has(ip) && net.ipToDevice.get(ip) !== reg.deviceId) {
        if (!reg.ipVariable) {
          const deviceId = net.ipToDevice.get(ip);
          const device = net.devices.get(deviceId);
          if (device) throw new Error(`IP重复，设备${device.name || ""}[${device.deviceId}]已使用此IP`);
          throw new Error("IP重复，服务端数据错误");
        }
        ip = undefined;
      }
    }
    if (ip === undefined) ip = this.findAvailableIp(net, cfg);
    if (oldIp) net.ipToDevice.delete(oldIp);

    const device = {
      deviceId: reg.deviceId,
      ip,
      name: reg.name || "",
      version: reg.version || "",
      keySign: reg.keySign,
      online: true,
      lastConnectedTime: unixSeconds(),
      disconnectTime: undefined,
      dataVersion: 0,
      txBytes: existing?.txBytes || 0,
      rxBytes: existing?.rxBytes || 0,
      sessionId
    };
    net.devices.set(reg.deviceId, device);
    net.ipToDevice.set(ip, reg.deviceId);
    this.bump(net, device);
    return { ip, device };
  }

  findAvailableIp(net, cfg) {
    for (let i = cfg.network + 1; i < cfg.broadcast; i++) {
      const ip = i >>> 0;
      if (ip !== cfg.gateway && !net.ipToDevice.has(ip)) return ip;
    }
    throw new Error("IP exhaustion");
  }

  async handleData(session, bytes) {
    const packet = readPacket(bytes);
    const net = this.ensureNetwork(session.networkCode);
    const srcDevice = net.devices.get(session.deviceId);
    if (srcDevice) srcDevice.txBytes = (srcDevice.txBytes || 0) + bytes.length;

    if (packet.isGateway) {
      await this.handleGateway(session, packet, bytes);
      return;
    }

    if ([MSG.TURN, MSG.PING, MSG.PONG, MSG.PUNCH_START_1, MSG.PUNCH_START_2, MSG.QUIC, MSG.RELAY_PROBE, MSG.RELAY_PROBE_CLIENT, MSG.RELAY_PROBE_REPLY_CLIENT].includes(packet.msgType)) {
      if (this.disableRelay && isRelayDataMessage(packet.msgType)) {
        this.logDebug(`禁止中转已启用，丢弃数据中转 网络编号=${session.networkCode} 类型=${packet.msgType} 目标=${intToIp(packet.destId)}`);
        return;
      }
      const copy = new Uint8Array(bytes);
      if (!decrementTtl(copy)) return;
      if (this.peerServers.length) await this.refreshPeerClientCache();
      if (!this.forwardToIp(net, packet.destId, copy)) await this.forwardToPeers(session.networkCode, packet.destId, copy);
    } else if (packet.msgType === MSG.BROADCAST) {
      if (this.disableRelay) {
        this.logDebug(`禁止中转已启用，丢弃广播中转 网络编号=${session.networkCode} 来源=${intToIp(packet.srcId)}`);
        return;
      }
      const copy = new Uint8Array(bytes);
      if (!decrementTtl(copy)) return;
      this.broadcast(net, packet.srcId, copy);
    } else if (packet.msgType === MSG.EXCLUDE_BROADCAST || packet.msgType === MSG.TARGET_BROADCAST) {
      if (this.disableRelay) {
        this.logDebug(`禁止中转已启用，丢弃选择性广播中转 网络编号=${session.networkCode} 来源=${intToIp(packet.srcId)}`);
        return;
      }
      const selective = parseSelectiveBroadcast(packet.payload);
      const inner = new Uint8Array(selective.data);
      const innerPacket = readPacket(inner);
      if (!decrementTtl(inner)) return;
      if (packet.msgType === MSG.EXCLUDE_BROADCAST) {
        for (const device of net.devices.values()) {
          if (device.online && device.ip !== packet.srcId && !selective.ips.has(device.ip)) this.sendDevice(device, inner);
        }
      } else {
        for (const ip of selective.ips) {
          if (ip !== packet.srcId && !this.forwardToIp(net, ip, inner, innerPacket.destId)) await this.forwardToPeers(session.networkCode, ip, inner);
        }
      }
    }
  }

  async handleGateway(session, packet, bytes) {
    if (packet.msgType === MSG.TURN) {
      const reply = makeIcmpEchoReply(packet);
      if (reply) this.send(session, reply);
    } else if (packet.msgType === MSG.PING_TURN) {
      if (packet.payload.length === 16) {
        const view = new DataView(packet.payload.buffer, packet.payload.byteOffset, packet.payload.byteLength);
        const time = Number(view.getBigUint64(0, false));
        const dataVersion = Number(view.getBigUint64(8, false));
        await this.refreshPeerClientCache();
        const changed = this.changedClientSimpleList(session, dataVersion, time);
        if (changed) this.send(session, makePacket(MSG.PUSH_CLIENT_IPS, encodeClientSimpleInfoList(changed), { gateway: true, ttl: 1 }));
        else {
          const pong = new Uint8Array(bytes);
          pong[0] = 0x80 | MSG.PONG_TURN;
          this.send(session, pong);
        }
      } else if (packet.payload.length === 8) {
        const pong = new Uint8Array(bytes);
        pong[0] = 0x80 | MSG.PONG_TURN;
        this.send(session, pong);
      }
    } else if (packet.msgType === MSG.PONG && packet.payload.length === 8) {
      const device = this.ensureNetwork(session.networkCode).devices.get(session.deviceId);
      if (device) device.latencyMs = Math.max(0, Math.floor((Date.now() - Number(new DataView(packet.payload.buffer, packet.payload.byteOffset, 8).getBigUint64(0, false))) / 2));
    } else if (packet.msgType === MSG.RPC_REQ) {
      const req = parseRpcRequest(packet.payload);
      if (!req.clientListReq) return;
      await this.refreshPeerClientCache();
      const payload = encodeRpcClientListResponse(req.id, this.clientInfoList(session));
      this.send(session, makePacket(MSG.RPC_RES, payload, { gateway: true, ttl: 1 }));
    }
  }

  changedClientSimpleList(session, dataVersion, time) {
    const net = this.ensureNetwork(session.networkCode);
    if (dataVersion === net.dataVersion) return null;
    const isAll = dataVersion > net.dataVersion;
    const list = [];
    for (const device of net.devices.values()) {
      if (!device.ip || device.ip === session.ip) continue;
      if (isAll || device.dataVersion > dataVersion) list.push({ ip: device.ip, online: !!device.online });
    }
    for (const remote of this.remoteClients(session.networkCode)) {
      if (remote.ip !== session.ip) list.push({ ip: remote.ip, online: !!remote.online });
    }
    return { dataVersion: net.dataVersion, list, isAll, time };
  }

  clientInfoList(session) {
    const net = this.ensureNetwork(session.networkCode);
    const localKeySign = net.devices.get(session.deviceId)?.keySign || "";
    const cfg = net.config;
    const list = [];
    list.push({
      name: this.env.GATEWAY_NAME || "服务器",
      version: this.serverVersion,
      ip: cfg.gateway,
      keySign: localKeySign,
      online: true,
      lastConnectedTime: unixSeconds(),
      id: `gateway-${session.networkCode}`
    });
    for (const device of net.devices.values()) {
      if (!device.ip || device.ip === session.ip) continue;
      list.push({
        name: device.name || "",
        version: device.version || "",
        ip: device.ip,
        keySign: device.keySign,
        online: !!device.online,
        lastConnectedTime: device.lastConnectedTime || 0,
        id: device.deviceId
      });
    }
    for (const remote of this.remoteClients(session.networkCode)) {
      if (remote.ip === session.ip) continue;
      list.push({
        name: remote.name || "",
        version: remote.version || "",
        ip: remote.ip,
        keySign: remote.keySign || localKeySign,
        online: !!remote.online,
        lastConnectedTime: remote.lastConnectedTime || 0,
        id: remote.deviceId || ""
      });
    }
    return list;
  }

  forwardToIp(net, ip, bytes) {
    const deviceId = net.ipToDevice.get(ip >>> 0);
    const device = deviceId ? net.devices.get(deviceId) : undefined;
    if (!device?.online) return false;
    device.rxBytes = (device.rxBytes || 0) + bytes.length;
    this.sendDevice(device, bytes);
    this.logDebug(`本地转发 目标=${intToIp(ip)} 字节=${bytes.length}`);
    return true;
  }

  broadcast(net, srcIp, bytes) {
    for (const device of net.devices.values()) {
      if (device.online && device.ip !== srcIp) this.sendDevice(device, bytes);
    }
  }

  sendDevice(device, bytes) {
    if (device.socket?.readyState !== 1) return false;
    try {
      device.socket.send(bytes);
      return true;
    } catch (error) {
      this.reportError(`发送客户端数据失败 设备ID=${device.deviceId || "未知"}`, error);
      return false;
    }
  }

  send(session, bytes) {
    if (session.socket?.readyState !== 1) return false;
    try {
      session.socket.send(bytes);
      return true;
    } catch (error) {
      this.reportError(`发送会话数据失败 会话ID=${session.id}`, error);
      return false;
    }
  }

  async closeWithError(sessionId, error) {
    const session = this.sessions.get(sessionId);
    if (!session) return;
    this.logInfo(`会话异常关闭 会话=${sessionId} 原因=${error.message || String(error)}`);
    if (!session.registered) {
      try {
        this.send(session, encodeErrorResponse(400, error.message || String(error)));
      } catch {}
    }
    try {
      session.socket.close(1011, "server error");
    } catch {}
    await this.offline(sessionId);
  }

  async handlePeerRequest(request, url) {
    if (url.pathname === "/peer/message") return this.handlePeerMessage(request);
    if (!this.peerAuthorized(request)) return new Response("未授权", { status: 401 });
    if (url.pathname === "/peer/ping") return Response.json({ ok: true, server: this.serverVersion, now: Date.now() });
    if (url.pathname === "/peer/client-info") return Response.json({ networks: this.exportPeerClientInfo() });
    if (url.pathname === "/peer/forward") {
      const networkCode = url.searchParams.get("network") || "";
      const dest = Number(url.searchParams.get("dest") || 0) >>> 0;
      const net = this.networks.get(networkCode);
      if (!net) return Response.json({ delivered: false });
      const bytes = await readRequestBytes(request);
      const delivered = this.forwardToIp(net, dest, bytes);
      this.logDebug(`互联转发接收 网络=${networkCode} 目标=${intToIp(dest)} 已投递=${delivered}`);
      return Response.json({ delivered });
    }
    return Response.redirect("https://github.com/lmq8267/vnts2-cf", 302);
  }

  peerAuthorized(request) {
    if (!this.peerToken) return false;
    return request.headers.get("X-Peer-Token") === this.peerToken || request.headers.get("X-Peer-Token-Hash") === this.peerTokenHash;
  }

  async handlePeerMessage(request) {
    const msg = parseServerMessage(await readRequestBytes(request));
    if (msg.authReq) {
      const success = !!this.peerTokenHash && msg.authReq.tokenHash === this.peerTokenHash;
      return peerProtoResponse({ authRes: { success, message: success ? "OK" : "Invalid token" } });
    }
    if (!this.peerAuthorized(request)) return new Response("未授权", { status: 401 });

    if (msg.pingReq) {
      return peerProtoResponse({
        pingRes: {
          requestTimestamp: msg.pingReq.timestamp,
          responseTimestamp: Date.now()
        }
      });
    }

    if (msg.clientInfoReq) {
      return peerProtoResponse({ clientInfoRes: this.peerClientInfoResponse(msg.clientInfoReq.networkCodes) });
    }

    if (msg.forwardData) {
      const networkCode = msg.forwardData.networkCode || "";
      const net = this.networks.get(networkCode);
      let delivered = false;
      if (net) {
        const packet = readPacket(msg.forwardData.data);
        delivered = this.forwardToIp(net, packet.destId, msg.forwardData.data);
        this.logDebug(`互联 protobuf 转发接收 网络编号=${networkCode} 目标=${intToIp(packet.destId)} 已投递=${delivered}`);
      }
      return peerProtoResponse({ authRes: { success: delivered, message: delivered ? "delivered" : "not delivered" } });
    }

    return peerProtoResponse({ authRes: { success: false, message: "unsupported peer message" } });
  }

  async forwardToPeers(networkCode, dest, bytes) {
    if (!this.peerServers.length || !this.peerToken) return false;
    let delivered = false;
    const init = {
      method: "POST",
      headers: {
        "X-Peer-Token": this.peerToken,
        "X-Peer-Token-Hash": this.peerTokenHash,
        "Content-Type": "application/octet-stream"
      },
      body: encodeServerMessage({ forwardData: { networkCode, data: bytes } }),
      signal: AbortSignal.timeout(15000)
    };
    for (const peer of this.peerServers) {
      try {
        const url = `${peer}/peer/message`;
        const res = await fetch(url, init);
        if (res.ok) {
          const body = parseServerMessage(await readResponseBytes(res));
          const peerDelivered = !!body.authRes?.success;
          delivered = delivered || peerDelivered;
          this.logDebug(`互联 protobuf 转发发送 网络编号=${networkCode} 目标=${intToIp(dest)} 节点=${peer} 已投递=${peerDelivered}`);
        }
      } catch (error) {
        this.logInfo(`互联转发失败 网络=${networkCode} 目标=${intToIp(dest)} 节点=${peer} 原因=${error.message || String(error)}`);
      }
    }
    return delivered;
  }

  async refreshPeerClientCache(force = false) {
    if (!this.peerServers.length || !this.peerToken) return;
    const now = Date.now();
    if (!force && now - this.peerClientCacheTime < 5000) return;
    const next = new Map();
    const networkCodes = Array.from(this.networks.keys());
    const newPeerStatus = new Map();
    for (const peer of this.peerServers) {
      let online = false;
      try {
        const res = await fetch(`${peer}/peer/message`, {
          method: "POST",
          headers: {
            "X-Peer-Token": this.peerToken,
            "X-Peer-Token-Hash": this.peerTokenHash,
            "Content-Type": "application/octet-stream"
          },
          body: encodeServerMessage({ clientInfoReq: { networkCodes } }),
          signal: AbortSignal.timeout(15000)
        });
        if (!res.ok) continue;
        online = true;
        const body = parseServerMessage(await readResponseBytes(res));
        for (const network of body.clientInfoRes?.networks || []) {
          if (!isNetworkAllowed(this.allowedNetworks, network.networkCode)) continue;
          const list = next.get(network.networkCode) || [];
          for (const client of network.clients || []) {
            list.push({ ip: client.ip >>> 0, latencyMs: client.latencyMs || 10, online: true, remotePeer: peer });
          }
          next.set(network.networkCode, list);
        }
      } catch (error) {
        this.logInfo(`互联客户端列表刷新失败 节点=${peer} 原因=${error.message || String(error)}`);
      }
      newPeerStatus.set(peer, online);
    }
    this.peerServerStatus = newPeerStatus;
    const signature = JSON.stringify(Array.from(next.entries()).map(([code, list]) => [code, list.map((c) => [c.ip, c.online, c.dataVersion || 0, c.remotePeer]).sort()]).sort());
    if (signature !== this.peerClientCacheSignature) {
      for (const code of next.keys()) {
        if (!isNetworkAllowed(this.allowedNetworks, code)) continue;
        const net = this.ensureNetwork(code);
        net.dataVersion += 1;
      }
      this.peerClientCacheSignature = signature;
      this.logDebug(`互联客户端列表已更新 网络数=${next.size}`);
    }
    this.peerClientCache = next;
    this.peerClientCacheTime = now;
  }

  remoteClients(networkCode) {
    return this.peerClientCache.get(networkCode) || [];
  }

  exportPeerClientInfo() {
    const networks = [];
    for (const [code, net] of this.networks.entries()) {
      networks.push({
        networkCode: code,
        dataVersion: net.dataVersion,
          clients: Array.from(net.devices.values())
          .filter((d) => d.ip && d.online)
          .map((d) => ({
            deviceId: d.deviceId,
            name: d.name,
            version: d.version,
            ip: d.ip,
            keySign: d.keySign,
            online: !!d.online,
            lastConnectedTime: d.lastConnectedTime || 0,
            dataVersion: d.dataVersion || 0
          }))
      });
    }
    return networks;
  }

  peerClientInfoResponse(networkCodes) {
    const requested = new Set((networkCodes || []).filter(Boolean));
    const networks = [];
    for (const [code, net] of this.networks.entries()) {
      if (requested.size && !requested.has(code)) continue;
      networks.push({
        networkCode: code,
        clients: Array.from(net.devices.values())
          .filter((d) => d.ip && d.online)
          .map((d) => ({ ip: d.ip, latencyMs: d.latencyMs || 10 }))
      });
    }
    return { networks };
  }

  async offline(sessionId) {
    const session = this.sessions.get(sessionId);
    if (!session) return;
    this.sessions.delete(sessionId);
    if (!session.registered) return;
    const net = this.ensureNetwork(session.networkCode);
    const device = net.devices.get(session.deviceId);
    if (!device || device.sessionId !== sessionId) return;
    if (session.pendingConfirmation) {
      net.devices.delete(session.deviceId);
      net.ipToDevice.delete(session.ip);
      this.logInfo(`预注册未确认，释放地址 网络编号=${session.networkCode} 设备ID=${session.deviceId} 虚拟IP=${intToIp(session.ip)}`);
    } else {
      device.online = false;
      device.socket = undefined;
      device.sessionId = undefined;
      device.disconnectTime = Date.now();
      this._dataDirty = true;
      this.bump(net, device);
      this.logInfo(`客户端离线 网络编号=${session.networkCode} 设备ID=${session.deviceId} 虚拟IP=${intToIp(session.ip)}`);
    }
    await this.persistSoon();
    this.logDebug(`离线后持久化完成 网络编号=${session.networkCode} 设备ID=${session.deviceId} 虚拟IP=${intToIp(session.ip)}`);
  }

  ensureNetwork(code, config) {
    if (!config) config = defaultNetworkConfig(this.defaultGatewayIp);
    let net = this.networks.get(code);
    if (!net) {
      net = { code, config, dataVersion: 0, devices: new Map(), ipToDevice: new Map() };
      this.networks.set(code, net);
    }
    return net;
  }

  bump(net, device) {
    net.dataVersion += 1;
    device.dataVersion = net.dataVersion;
  }

  async cleanupExpired() {
    const now = Date.now();
    let deletedDevices = 0;
    let deletedNetworks = 0;
    for (const [code, net] of this.networks.entries()) {
      for (const [deviceId, device] of Array.from(net.devices.entries())) {
        if (!device.online && device.disconnectTime && now - device.disconnectTime > this.leaseDurationMs) {
          net.devices.delete(deviceId);
          net.ipToDevice.delete(device.ip);
          net.dataVersion += 1;
          this._dataDirty = true;
          deletedDevices++;
          this.logInfo(`租约过期，释放地址 网络编号=${net.code} 设备ID=${deviceId} 虚拟IP=${intToIp(device.ip)}`);
        }
      }
      if (!net.devices.size && !this.remoteClients(code).length) {
        if (this.networks.has(code)) this._dataDirty = true;
        this.networks.delete(code);
        deletedNetworks++;
      }
    }
    if (deletedDevices || deletedNetworks) {
      this.logDebug(`过期清理 删除设备=${deletedDevices} 删除网络=${deletedNetworks}`);
    }
  }

  totalDeviceCount() {
    let count = 0;
    for (const net of this.networks.values()) count += net.devices.size;
    return count;
  }

  pingLocalClients() {
    const timestamp = BigInt(Date.now());
    const payload = new Uint8Array(8);
    new DataView(payload.buffer).setBigUint64(0, timestamp, false);
    const packet = makePacket(MSG.PING, payload, { gateway: true, ttl: 1 });
    for (const net of this.networks.values()) {
      for (const device of net.devices.values()) {
        if (device.online) this.sendDevice(device, packet);
      }
    }
  }

  async persistSoon() {
    try {
      await this.persist();
    } catch (error) {
      this.reportError("数据持久化状态保存失败", error);
    }
  }

  async persist() {
    if (!this._dataDirty) return;
    // this.logDebug(`正在持久化保存 网络数=${this.networks.size} 设备数=${this.totalDeviceCount()}`);
    const out = { networks: {} };
    for (const [code, net] of this.networks.entries()) {
      out.networks[code] = { dataVersion: net.dataVersion, config: net.config, devices: [] };
      for (const device of net.devices.values()) {
        out.networks[code].devices.push({
          deviceId: device.deviceId,
          ip: device.ip,
          name: device.name,
          version: device.version,
          keySign: device.keySign,
          online: false,
          lastConnectedTime: device.lastConnectedTime,
          disconnectTime: device.disconnectTime,
          dataVersion: device.dataVersion,
          txBytes: device.txBytes || 0,
          rxBytes: device.rxBytes || 0
        });
      }
    }
    await this.state.storage.put(STORAGE_KEY, out);
    this._dataDirty = false;
    // this.logDebug(`持久化完成 网络数=${this.networks.size} 设备数=${this.totalDeviceCount()}`);
  }

  async scheduleAlarm() {
    try {
      await this.state.storage.setAlarm(Date.now() + this.maintenanceIntervalMs);
    } catch (error) {
      this.reportError("调度维护任务失败", error);
    }
  }

  status() {
    let totalOnlineClients = 0;
    let totalOfflineClients = 0;
    for (const net of this.networks.values()) {
      for (const device of net.devices.values()) {
        if (device.online) totalOnlineClients++;
        else totalOfflineClients++;
      }
    }
    // 统计互联服务端上的客户端
    let peerOnlineClients = 0;
    for (const clients of this.peerClientCache.values()) {
      peerOnlineClients += clients.length;
    }
    // 统计互联服务端在线/离线
    let peerOnline = 0;
    let peerOffline = 0;
    for (const online of this.peerServerStatus.values()) {
      if (online) peerOnline++;
      else peerOffline++;
    }
    return {
      "WebSocket服务": "正常",
      "服务端版本": this.serverVersion,
      "启动时间": this.getStartTimeBeijing(),
      "已运行": this.getRunningDuration(),
      "支持协议": "VNT2 WebSocket",
      "服务状态": "可用",
      "网络编号数": this.networks.size,
      "在线客户端": totalOnlineClients + peerOnlineClients,
      "离线客户端": totalOfflineClients,
      "互联服务端在线": peerOnline,
      "互联服务端离线": peerOffline,
      "服务端中转": this.disableRelay ? "已禁止" : "已启用"
    };
  }

  getStartTimeBeijing() {
    const d = new Date(this.startTime);
    const bj = new Date(d.getTime() + 8 * 60 * 60 * 1000);
    const p = (n) => String(n).padStart(2, "0");
    return `${bj.getFullYear()}-${p(bj.getMonth() + 1)}-${p(bj.getDate())} ${p(bj.getHours())}:${p(bj.getMinutes())}:${p(bj.getSeconds())}`;
  }

  getRunningDuration() {
    const diff = Date.now() - this.startTime;
    const s = Math.floor(diff / 1000);
    const d = Math.floor(s / 86400);
    const h = Math.floor((s % 86400) / 3600);
    const m = Math.floor((s % 3600) / 60);
    const sec = s % 60;
    const parts = [];
    if (d > 0) parts.push(`${d}天`);
    if (h > 0) parts.push(`${h}小时`);
    if (m > 0) parts.push(`${m}分`);
    parts.push(`${sec}秒`);
    return parts.join("");
  }

  roomInfo() {
    const networks = [];
    const gateways = [];
    for (const [code, net] of this.networks.entries()) {
      const cfg = net.config;
      gateways.push({
        networkCode: code,
        deviceId: `gateway-${code}`,
        name: this.env.GATEWAY_NAME || "服务器",
        version: this.serverVersion,
        ip: intToIp(cfg.gateway),
        online: true,
        role: "gateway"
      });
      networks.push({
        networkCode: code,
        dataVersion: net.dataVersion,
        devices: Array.from(net.devices.values()).map((d) => ({
          deviceId: d.deviceId,
          name: d.name,
          version: d.version,
          ip: intToIp(d.ip),
          online: !!d.online,
          keySign: d.keySign,
          lastConnectedTime: d.lastConnectedTime,
          txBytes: d.txBytes || 0,
          rxBytes: d.rxBytes || 0
        }))
      });
    }
    return { networks, gateways };
  }

  async handleTestPage(request, url) {
    const data = this.status();
    if (wantsJson(request, url)) return Response.json(data);
    return htmlResponse(renderTestHtml(data));
  }

  async handleRoomPage(request, url) {
    const auth = await this.authorizeStatusRequest(request, url);
    if (!auth.ok) return htmlResponse(renderLoginHtml(auth.message || "请输入正确的网络编号和网关"));
    const info = this.roomInfo();
    // 互联服务端上客户端
    info.peerClients = Array.from(this.peerClientCache.entries()).map(([networkCode, clients]) => ({
      networkCode,
      clients: clients.map((client) => ({
        ip: intToIp(client.ip),
        peer: client.remotePeer
      }))
    }));
    // 构建设备列表：网关 + 本地客户端 + 互联客户端
    const devices = [];
    for (const g of info.gateways || []) {
      devices.push({ 类型: "网关", 虚拟IP: g.ip, 名称: g.name, 版本: g.version, 状态: "在线", 上线时间: formatTime(Math.floor(this.startTime / 1000)) });
    }
    for (const net of info.networks || []) {
      for (const d of net.devices) {
        devices.push({
          类型: "客户端",
          虚拟IP: d.ip, 名称: d.name || d.deviceId, 版本: d.version || "",
          状态: d.online ? "在线" : "离线",
          设备ID: d.deviceId,
          加密: d.keySign ? "是" : "否",
          上传: formatBytes(d.txBytes),
          下载: formatBytes(d.rxBytes),
          上线时间: formatTime(d.lastConnectedTime)
        });
      }
    }
    // 互联服务端上客户端
    for (const pc of info.peerClients || []) {
      for (const c of pc.clients || []) {
        devices.push({
          类型: "互联", 虚拟IP: c.ip, 状态: "在线", 服务端: c.peer
        });
      }
    }
    const onlineCount = devices.filter(d => d.状态 === "在线" && d.类型 !== "网关").length;
    const offlineCount = devices.filter(d => d.状态 === "离线").length;
    if (wantsJson(request, url)) return jsonAuthResponse({ devices, peerServers: this.peerServers }, auth);
    return htmlResponse(renderRoomHtml({ devices, peerServers: this.peerServers, onlineCount, offlineCount }), auth);
  }

  async handlePeerPage(request, url) {
    if (!this.peerToken) return Response.redirect("https://github.com/lmq8267/vnts2-cf", 302);
    const auth = this.authorizePeerRequest(request, url);
    if (!auth.ok) return htmlResponse(renderPeerLoginHtml(auth.message || "请输入互联令牌"));
    // 构建互联服务端列表
    const list = this.peerServers.map((addr) => ({
      addr,
      online: this.peerServerStatus.get(addr) || false
    }));
    if (wantsJson(request, url)) return Response.json({ servers: list });
    return htmlResponse(renderPeerHtml(list), auth);
  }

  async handleLogPage(request, url) {
    if (!this.logPassword) return new Response(null, { status: 404 });
    const auth = this.authorizeLogRequest(request, url);
    if (!auth.ok) return htmlResponse(renderLogLoginHtml(auth.message || "请输入日志密码"));
    const logs = this.logs.slice().reverse();
    if (wantsJson(request, url)) return Response.json({ logs, count: logs.length });
    return htmlResponse(renderLogHtml({ logs }), auth);
  }

  async handleLogClear(request, url) {
    if (!this.logPassword) return new Response(null, { status: 404 });
    const auth = this.authorizeLogRequest(request, url);
    if (!auth.ok) return Response.json({ error: "未授权" }, { status: 401 });
    try {
      this.logs = [];
      await this.state.storage.delete("vnts2-logs");
      return Response.json({ status: "ok", message: "日志已清空" });
    } catch (error) {
      return Response.json({ error: "清空失败: " + errorMessage(error) }, { status: 500 });
    }
  }

  async authorizeStatusRequest(request, url) {
    const cookies = parseCookies(request.headers.get("Cookie") || "");
    const networkCode = url.searchParams.get("network") || cookies.network_code || "";
    const gateway = url.searchParams.get("gateway") || url.searchParams.get("ip") || cookies.gateway_ip || "";
    if (!isNetworkAllowed(this.allowedNetworks, networkCode)) return { ok: false, message: `网络编号 ${networkCode} 未被服务端允许` };
    const cfg = this.networks.get(networkCode)?.config || defaultNetworkConfig(this.defaultGatewayIp);
    if (gateway !== intToIp(cfg.gateway)) return { ok: false, message: "网关地址不匹配" };
    return { ok: true, networkCode, gateway };
  }

  authorizeLogRequest(request, url) {
    if (!this.logPassword) return { ok: true, logPassword: "" };
    const cookies = parseCookies(request.headers.get("Cookie") || "");
    const provided = url.searchParams.get("password") || cookies.log_auth || "";
    if (provided !== this.logPassword) return { ok: false, message: "日志密码不正确" };
    return { ok: true, logPassword: provided };
  }

  authorizePeerRequest(request, url) {
    const cookies = parseCookies(request.headers.get("Cookie") || "");
    const provided = url.searchParams.get("token") || cookies.peer_auth || "";
    if (provided !== this.peerToken) return { ok: false, message: "互联令牌不正确" };
    return { ok: true, peerToken: provided };
  }

  logInfo(message) {
    if (this.logLevel !== "off" && this.logPassword) {
      const text = `[vnts2-cf] ${message}`;
      this.appendLog("info", text);
      console.log(text);
    }
  }

  logDebug(message) {
    if (this.logLevel === "debug" && this.logPassword) {
      const text = `[vnts2-cf][调试] ${message}`;
      this.appendLog("debug", text);
      console.log(text);
    }
  }

  appendLog(level, message) {
    if (!this.logPassword) return;
    this.logs.push({
      timestamp: toBeijingTime(new Date()),
      level,
      message: limitText(message, 4096)
    });
    if (this.logs.length > 500) this.logs.splice(0, this.logs.length - 500);
    // 持久化保存到存储
    this._scheduleLogPersist();
  }

  async restoreLogs() {
    try {
      if (!this.logPassword) {
        await this.state.storage.delete("vnts2-logs");
        return;
      }
      const savedLogs = await this.state.storage.get("vnts2-logs");
      if (Array.isArray(savedLogs) && savedLogs.length > 0) {
        this.logs = savedLogs.map((entry) => ({
          timestamp: entry.timestamp || (entry.time ? toBeijingTime(new Date(entry.time)) : toBeijingTime(new Date())),
          level: entry.level,
          message: entry.message
        }));
      }
    } catch (error) {
      console.error("[vnts2-cf] 恢复日志失败", error);
    }
  }

  _scheduleLogPersist() {
    if (this._logPersistScheduled) return;
    this._logPersistScheduled = true;
    this.state.storage.put("vnts2-logs", this.logs.slice()).then(() => {
      this._logPersistScheduled = false;
    }).catch((error) => {
      this._logPersistScheduled = false;
      console.error("[vnts2-cf] 日志持久化保存失败", error);
    });
  }

  reportError(context, error) {
    const text = limitText(`[vnts2-cf] ${context}：${errorMessage(error)}`, 4096);
    this.lastError = text;
    this.appendLog("error", text);
    if (this.logPassword) console.error(text, error);
  }
}

function tryParseRequest(bytes) {
  try {
    return parseRequestMessage(bytes);
  } catch {
    return null;
  }
}

async function toBytes(value) {
  if (value instanceof Uint8Array) return value;
  if (value instanceof ArrayBuffer) return new Uint8Array(value);
  if (typeof value === "string") return new TextEncoder().encode(value);
  return new Uint8Array(await value.arrayBuffer());
}

async function readRequestBytes(request) {
  const contentLength = Number(request.headers.get("Content-Length") || 0);
  if (Number.isFinite(contentLength) && contentLength > MAX_MESSAGE_BYTES) {
    throw new Error(`请求体超过大小限制：${contentLength} > ${MAX_MESSAGE_BYTES}`);
  }
  const bytes = new Uint8Array(await request.arrayBuffer());
  if (bytes.length > MAX_MESSAGE_BYTES) throw new Error(`请求体超过大小限制：${bytes.length} > ${MAX_MESSAGE_BYTES}`);
  return bytes;
}

async function readResponseBytes(response) {
  const contentLength = Number(response.headers.get("Content-Length") || 0);
  if (Number.isFinite(contentLength) && contentLength > MAX_MESSAGE_BYTES) {
    throw new Error(`响应体超过大小限制：${contentLength} > ${MAX_MESSAGE_BYTES}`);
  }
  const bytes = new Uint8Array(await response.arrayBuffer());
  if (bytes.length > MAX_MESSAGE_BYTES) throw new Error(`响应体超过大小限制：${bytes.length} > ${MAX_MESSAGE_BYTES}`);
  return bytes;
}

function positiveSeconds(raw, fallback, minimum) {
  const value = Number(raw);
  return Number.isFinite(value) && value >= minimum ? value : fallback;
}

function isUint32(value) {
  return Number.isInteger(value) && value >= 0 && value <= 0xffffffff;
}

function safeNonNegativeInteger(value) {
  return Number.isInteger(value) && value >= 0 ? value : 0;
}

function safeTimestamp(value, fallback) {
  return Number.isFinite(Number(value)) && Number(value) > 0 ? Number(value) : fallback;
}

function storedNetworkConfig(config, defaultGateway) {
  return config && isUint32(config.gateway) ? networkConfigFromClientIp(config.gateway) : defaultNetworkConfig(defaultGateway);
}

function errorMessage(error) {
  return limitText(error instanceof Error ? error.message : String(error), 1024);
}

function limitText(value, maxLength) {
  const text = String(value ?? "");
  return text.length > maxLength ? `${text.slice(0, maxLength)}...` : text;
}

function toBeijingTime(date) {
  const bj = new Date(date.getTime() + 8 * 60 * 60 * 1000);
  const p = (n) => String(n).padStart(2, "0");
  return `${bj.getFullYear()}-${p(bj.getMonth() + 1)}-${p(bj.getDate())} ${p(bj.getHours())}:${p(bj.getMinutes())}:${p(bj.getSeconds())}`;
}

function unixSeconds() {
  return Math.floor(Date.now() / 1000);
}

function makeIcmpEchoReply(packet) {
  const payload = packet.payload;
  if (payload.length < 28) return null;
  const ihl = (payload[0] & 0x0f) * 4;
  if ((payload[0] >> 4) !== 4 || ihl < 20 || payload.length < ihl + 8) return null;
  const totalLength = ((payload[2] << 8) | payload[3]) >>> 0;
  if (totalLength < ihl + 8 || payload.length < totalLength) return null;
  if (payload[9] !== 1) return null;
  const icmpOffset = ihl;
  if (payload[icmpOffset] !== 8) return null;

  const replyIp = payload.slice(0, totalLength);
  const src = replyIp.slice(12, 16);
  replyIp.set(replyIp.slice(16, 20), 12);
  replyIp.set(src, 16);
  replyIp[10] = 0;
  replyIp[11] = 0;
  const ipSum = checksum16(replyIp.slice(0, ihl));
  replyIp[10] = (ipSum >>> 8) & 0xff;
  replyIp[11] = ipSum & 0xff;

  replyIp[icmpOffset] = 0;
  replyIp[icmpOffset + 1] = 0;
  replyIp[icmpOffset + 2] = 0;
  replyIp[icmpOffset + 3] = 0;
  const icmpSum = checksum16(replyIp.slice(icmpOffset));
  replyIp[icmpOffset + 2] = (icmpSum >>> 8) & 0xff;
  replyIp[icmpOffset + 3] = icmpSum & 0xff;

  return makePacket(MSG.TURN, replyIp, { gateway: true, ttl: 1, seq: packet.seq });
}

function checksum16(bytes) {
  let sum = 0;
  for (let i = 0; i < bytes.length; i += 2) {
    const word = ((bytes[i] << 8) | (bytes[i + 1] || 0)) >>> 0;
    sum += word;
    while (sum > 0xffff) sum = (sum & 0xffff) + (sum >>> 16);
  }
  return (~sum) & 0xffff;
}

function parseBool(raw) {
  return ["1", "true", "yes", "on"].includes(String(raw || "").trim().toLowerCase());
}

function isRelayDataMessage(msgType) {
  return msgType === MSG.TURN || msgType === MSG.QUIC;
}

async function sha256Hex(value) {
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(String(value)));
  return Array.from(new Uint8Array(digest)).map((b) => b.toString(16).padStart(2, "0")).join("");
}

function peerProtoResponse(message) {
  return new Response(encodeServerMessage(message), {
    headers: { "Content-Type": "application/octet-stream" }
  });
}

function wantsJson(request, url) {
  return url.searchParams.get("format") === "json" || request.headers.get("Accept")?.includes("application/json");
}

function htmlResponse(html, auth) {
  const headers = new Headers({ "Content-Type": "text/html; charset=utf-8" });
  if (auth?.ok) {
    if (auth.networkCode) headers.append("Set-Cookie", `network_code=${encodeURIComponent(auth.networkCode)}; path=/; max-age=86400; SameSite=Lax`);
    if (auth.gateway) headers.append("Set-Cookie", `gateway_ip=${encodeURIComponent(auth.gateway)}; path=/; max-age=86400; SameSite=Lax`);
    if (auth.logPassword) headers.append("Set-Cookie", `log_auth=${encodeURIComponent(auth.logPassword)}; path=/; max-age=86400; SameSite=Lax`);
    if (auth.peerToken) headers.append("Set-Cookie", `peer_auth=${encodeURIComponent(auth.peerToken)}; path=/; max-age=86400; SameSite=Lax`);
  }
  return new Response(html, { headers });
}

function jsonAuthResponse(value, auth) {
  const response = Response.json(value);
  if (auth?.ok) {
    response.headers.append("Set-Cookie", `network_code=${encodeURIComponent(auth.networkCode)}; path=/; max-age=86400; SameSite=Lax`);
    response.headers.append("Set-Cookie", `gateway_ip=${encodeURIComponent(auth.gateway)}; path=/; max-age=86400; SameSite=Lax`);
  }
  return response;
}

function jsonLogResponse(value, auth) {
  const response = Response.json(value);
  if (auth?.ok && auth.logPassword) {
    response.headers.append("Set-Cookie", `log_auth=${encodeURIComponent(auth.logPassword)}; path=/; max-age=86400; SameSite=Lax`);
  }
  return response;
}

function parseCookies(raw) {
  const out = {};
  for (const part of String(raw).split(";")) {
    const idx = part.indexOf("=");
    if (idx <= 0) continue;
    const key = part.slice(0, idx).trim();
    const value = part.slice(idx + 1).trim();
    try {
      out[key] = decodeURIComponent(value);
    } catch {
      out[key] = value;
    }
  }
  return out;
}

function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function getClientIp(request) {
  const h = {};
  for (const [k, v] of request.headers.entries()) h[k.toLowerCase()] = v;
  return h["cf-connecting-ip"] || h["x-real-ip"] || (h["x-forwarded-for"] || "").split(",")[0]?.trim() || "";
}

function renderTestHtml(status) {
  return `<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>服务状态 - VNT2</title>
  <script src="https://unpkg.com/vue@3/dist/vue.global.js"></script>
  <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body {
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
      background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
      min-height: 100vh;
      display: flex;
      align-items: center;
      justify-content: center;
    }
    .modal {
      background: rgba(255, 255, 255, 0.96);
      width: calc(100vw - 48px);
      max-width: 600px;
      padding: 36px 32px;
      border-radius: 16px;
      box-shadow: 0 20px 60px rgba(0, 0, 0, 0.28);
      backdrop-filter: blur(10px);
    }
    .modal h2 {
      margin-bottom: 30px;
      text-align: center;
      background: linear-gradient(45deg, #667eea, #764ba2);
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;
      font-size: 24px;
    }
    .status-item {
      display: flex;
      justify-content: space-between;
      align-items: center;
      padding: 12px 0;
      border-bottom: 1px solid #e0e0e0;
    }
    .status-item:last-child { border-bottom: none; }
    .status-label { font-weight: 500; color: #333; }
    .status-value { color: #666; }
    .status-value.online { color: #4caf50; font-weight: 500; }
    .latency-display {
      text-align: center; margin: 20px 0; padding: 20px;
      border-radius: 12px; transition: all .3s ease;
      background: #f5f5f5;
    }
    .latency-value { font-size: 36px; font-weight: 700; margin-bottom: 8px; }
    .latency-label { font-size: 14px; color: #666; }
    .latency-excellent { background: linear-gradient(135deg, #4caf50, #45a049); color: #fff; }
    .latency-good { background: linear-gradient(135deg, #8bc34a, #7cb342); color: #fff; }
    .latency-fair { background: linear-gradient(135deg, #ff9800, #f57c00); color: #fff; }
    .latency-poor { background: linear-gradient(135deg, #f44336, #d32f2f); color: #fff; }
    .test-btn {
      display: block; margin: 20px auto 0; padding: 12px 40px;
      background: linear-gradient(45deg, #667eea, #764ba2); color: #fff;
      border: none; border-radius: 8px; font-size: 16px; font-weight: 500;
      cursor: pointer; transition: all .3s ease;
    }
    .test-btn:hover { transform: translateY(-2px); box-shadow: 0 5px 15px rgba(102,126,234,.3); }
    .test-btn:disabled { opacity: .6; cursor: not-allowed; transform: none; }
    .test-btn.stop { background: linear-gradient(45deg, #f44336, #e91e63); }
    .test-btn.stop:hover { box-shadow: 0 5px 15px rgba(244,67,54,.3); }
    @media (max-width: 767px) {
      .modal { padding: 30px 20px; width: 95%; }
      .latency-value { font-size: 28px; }
    }
  </style>
</head>
<body>
  <div id="app">
    <div class="modal">
      <h2>服务状态检测</h2>
      <div class="status-item">
        <span class="status-label">WebSocket 服务：</span>
        <span class="status-value online">{{ status['WebSocket服务'] }}</span>
      </div>
      <div class="status-item">
        <span class="status-label">服务状态：</span>
        <span class="status-value online">{{ status['服务状态'] }}</span>
      </div>
      <div class="status-item">
        <span class="status-label">支持协议：</span>
        <span class="status-value">{{ status['支持协议'] }}</span>
      </div>
      <div class="status-item">
        <span class="status-label">服务端版本：</span>
        <span class="status-value">{{ status['服务端版本'] }}</span>
      </div>
      <div class="status-item">
        <span class="status-label">启动时间：</span>
        <span class="status-value">{{ status['启动时间'] }}</span>
      </div>
      <div class="status-item">
        <span class="status-label">已运行：</span>
        <span class="status-value">{{ status['已运行'] }}</span>
      </div>
      <div class="status-item">
        <span class="status-label">网络编号数：</span>
        <span class="status-value">{{ status['网络编号数'] }} 个</span>
      </div>
      <div class="status-item">
        <span class="status-label">客户端在线：</span>
        <span class="status-value online">{{ status['在线客户端'] }} 个</span>
      </div>
      <div class="status-item">
        <span class="status-label">客户端离线：</span>
        <span class="status-value">{{ status['离线客户端'] }} 个</span>
      </div>
      <div class="status-item">
        <span class="status-label">服务端中转：</span>
        <span class="status-value">{{ status['服务端中转'] }}</span>
      </div>
      <div class="status-item">
        <span class="status-label">互联服务端在线：</span>
        <span class="status-value online">{{ status['互联服务端在线'] }} 个</span>
      </div>
      <div class="status-item">
        <span class="status-label">互联服务端离线：</span>
        <span class="status-value">{{ status['互联服务端离线'] }} 个</span>
      </div>
      <div class="latency-display" :class="latencyClass">
        <div class="latency-value">{{ latency }}ms</div>
        <div class="latency-label">{{ latencyText }}</div>
      </div>
      <button class="test-btn" :class="{ stop: autoDetecting }" @click="toggleAutoDetection" :disabled="testing">
        {{ autoDetecting ? '停止自动检测延迟' : '开始自动检测延迟' }}
      </button>
    </div>
  </div>
  <script>
    const { createApp } = Vue;
    createApp({
      data() {
        return {
          status: ${JSON.stringify(status)},
          latency: 0,
          testing: false,
          autoDetecting: true,
          latencyClass: '',
          latencyText: '点击检测延迟',
          autoDetectInterval: null
        };
      },
      mounted() {
        this.testLatency();
        this.startAutoDetection();
      },
      beforeUnmount() { this.stopAutoDetection(); },
      methods: {
        async testLatency() {
          this.testing = true;
          const start = performance.now();
          try {
            const r = await fetch(window.location.href, { method: 'HEAD', cache: 'no-cache' });
            const t = Math.round(performance.now() - start);
            this.latency = t;
            if (t < 50) { this.latencyClass = 'latency-excellent'; this.latencyText = '连接极佳'; }
            else if (t < 100) { this.latencyClass = 'latency-good'; this.latencyText = '连接良好'; }
            else if (t < 200) { this.latencyClass = 'latency-fair'; this.latencyText = '连接一般'; }
            else { this.latencyClass = 'latency-poor'; this.latencyText = '连接较差'; }
          } catch (e) {
            this.latency = 999;
            this.latencyClass = 'latency-poor';
            this.latencyText = '检测失败';
          } finally { this.testing = false; }
        },
        startAutoDetection() {
          if (this.autoDetectInterval) return;
          this.autoDetecting = true;
          this.autoDetectInterval = setInterval(() => {
            if (!this.testing && this.autoDetecting) this.testLatency();
          }, 5000);
        },
        stopAutoDetection() {
          if (this.autoDetectInterval) { clearInterval(this.autoDetectInterval); this.autoDetectInterval = null; }
          this.autoDetecting = false;
        },
        toggleAutoDetection() {
          this.autoDetecting ? this.stopAutoDetection() : this.startAutoDetection();
        }
      }
    }).mount('#app');
  </script>
</body>
</html>`;
}

function renderLoginHtml(message) {
  return `<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>房间认证 - VNT2</title>
  <script src="https://unpkg.com/vue@3/dist/vue.global.js"></script>
  <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body {
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
      background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
      min-height: 100vh; display: flex; align-items: center; justify-content: center;
    }
    .modal {
      background: rgba(255,255,255,0.96); width: calc(100vw - 48px); max-width: 420px;
      padding: 36px 32px; border-radius: 16px;
      box-shadow: 0 20px 60px rgba(0,0,0,0.28); backdrop-filter: blur(10px);
    }
    .modal h2 {
      margin-bottom: 30px; text-align: center;
      background: linear-gradient(45deg,#667eea,#764ba2);
      -webkit-background-clip: text; -webkit-text-fill-color: transparent;
      font-size: 24px;
    }
    .form-group { margin-bottom: 20px; }
    .form-group label { display: block; margin-bottom: 8px; font-weight: 500; color: #333; }
    .form-group input {
      width: 100%; padding: 12px; border: 2px solid #e0e0e0;
      border-radius: 8px; font-size: 14px;
    }
    .form-group input:focus { outline: none; border-color: #667eea; }
    .submit-btn {
      display: block; margin: 0 auto; padding: 12px 40px;
      background: linear-gradient(45deg,#667eea,#764ba2); color: #fff;
      border: none; border-radius: 8px; font-size: 16px; font-weight: 500; cursor: pointer;
    }
    .submit-btn:hover { transform: translateY(-2px); box-shadow: 0 5px 15px rgba(102,126,234,.3); }
    .error-message {
      background: #ffebee; color: #c62828; padding: 12px; border-radius: 8px;
      margin-bottom: 20px; border: 1px solid #ef5350; font-size: 14px;
    }
  </style>
</head>
<body>
  <div id="app">
    <div class="modal">
      <h2>查询验证</h2>
      <div v-if="showError" class="error-message">{{ errorMessage }}</div>
      <div class="form-group">
        <label>网络编号</label>
        <input v-model="form.network" placeholder="vnt2 组网编号" @keyup.enter="login" />
      </div>
      <div class="form-group">
        <label>网关地址</label>
        <input v-model="form.gateway" placeholder="组网编号对应的网关 IP" @keyup.enter="login" />
      </div>
      <button class="submit-btn" @click="login">进入</button>
    </div>
  </div>
  <script>
    const { createApp } = Vue;
    createApp({
      data() {
        return {
          form: { network: '', gateway: '' },
          showError: ${message ? "true" : "false"},
          errorMessage: ${message ? JSON.stringify(message) : "''"}
        };
      },
      methods: {
        login() {
          if (!this.form.gateway) { this.showError = true; this.errorMessage = '请输入网关地址'; return; }
          const p = new URLSearchParams();
          p.set('network', this.form.network || '');
          p.set('gateway', this.form.gateway);
          location.href = '/room?' + p.toString();
        }
      }
    }).mount('#app');
  </script>
</body>
</html>`;
}

function formatBytes(bytes) {
  if (!bytes || bytes === 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  let i = 0;
  let v = bytes;
  while (v >= 1024 && i < units.length - 1) { v /= 1024; i++; }
  return v.toFixed(i === 0 ? 0 : 1) + " " + units[i];
}

function formatTime(ts) {
  if (!ts || ts === 0) return "-";
  const d = new Date(ts * 1000);
  const bj = new Date(d.getTime() + 8 * 60 * 60 * 1000);
  const p = (n) => String(n).padStart(2, "0");
  return `${bj.getFullYear()}-${p(bj.getMonth() + 1)}-${p(bj.getDate())} ${p(bj.getHours())}:${p(bj.getMinutes())}:${p(bj.getSeconds())}`;
}

function renderRoomHtml(data) {
  const devices = data.devices || [];
  const peerServers = data.peerServers || [];
  const onlineCount = data.onlineCount || 0;
  const offlineCount = data.offlineCount || 0;
  return `<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>客户端列表 - VNT2</title>
  <script src="https://unpkg.com/vue@3/dist/vue.global.js"></script>
  <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body {
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
      background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
      min-height: 100vh;
    }
    .container { max-width: 1200px; margin: 0 auto; padding: 20px; }
    .header {
      background: rgba(255,255,255,0.95); border-radius: 15px; padding: 20px;
      margin-bottom: 20px; box-shadow: 0 10px 30px rgba(0,0,0,0.1);
      backdrop-filter: blur(10px); display: flex; justify-content: space-between; align-items: center;
    }
    .title {
      font-size: 24px; font-weight: bold;
      background: linear-gradient(45deg,#667eea,#764ba2);
      -webkit-background-clip: text; -webkit-text-fill-color: transparent;
    }
    .logout-btn {
      background: linear-gradient(45deg,#f44336,#e91e63); color: #fff;
      border: none; padding: 10px 20px; border-radius: 25px; cursor: pointer;
      font-weight: 500; transition: all .3s ease;
    }
    .logout-btn:hover { transform: translateY(-2px); box-shadow: 0 5px 15px rgba(244,67,54,.3); }
    .main-content {
      background: rgba(255,255,255,0.95); border-radius: 15px; padding: 20px;
      box-shadow: 0 10px 30px rgba(0,0,0,0.1); backdrop-filter: blur(10px);
    }
    .filters { display: flex; gap: 10px; margin-bottom: 20px; flex-wrap: wrap; }
    .filter-btn {
      padding: 8px 16px; border: 2px solid #667eea; background: #fff;
      color: #667eea; border-radius: 20px; cursor: pointer; font-weight: 500;
      transition: all .3s ease;
    }
    .filter-btn.active { background: linear-gradient(45deg,#667eea,#764ba2); color: #fff; }
    .filter-btn:hover { transform: translateY(-1px); }
    .table-container { overflow-x: auto; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.05); }
    table { width: 100%; border-collapse: collapse; background: #fff; }
    th, td { padding: 12px; text-align: center; border-bottom: 1px solid #f0f0f0; border-right: 1px solid #e8e8e8; }
    th {
      background: linear-gradient(45deg,#1761ea,#066ce9); color: #fff;
      font-weight: 600; position: sticky; top: 0; z-index: 10;
      border-right: 1px solid rgba(255,255,255,0.3);
    }
    tr[data-type="网关"] td { background: #eacfdd; font-weight: 600; }
    tr[data-type="互联"] td { background: #d5e8d4; }
    tr:nth-child(even) td { background: #c3c9ed; }
    tr:nth-child(odd) td { background: #b6aaf1; }
    tr[data-type="网关"] td { background: #eacfdd !important; font-weight: 600; }
    tr[data-type="互联"] td { background: #d5e8d4 !important; }
    tr:hover td { background: #ee6fdf !important; }
    .status-badge { padding: 4px 12px; border-radius: 15px; font-size: 12px; font-weight: 500; display: inline-block; }
    .status-online { background: linear-gradient(45deg,#4caf50,#45a049); color: #fff; }
    .status-offline { background: linear-gradient(45deg,#f44336,#e91e63); color: #fff; }
    .stats-container { display: flex; gap: 20px; margin-bottom: 20px; flex-wrap: wrap; justify-content: center; }
    .stat-card {
      background: linear-gradient(135deg,rgba(255,255,255,0.9),rgba(255,255,255,0.7));
      padding: 15px 25px; border-radius: 12px; box-shadow: 0 4px 15px rgba(0,0,0,0.1);
      backdrop-filter: blur(10px); text-align: center; min-width: 120px;
      cursor: pointer; border: 2px solid transparent; transition: all .3s ease;
    }
    .stat-card:hover { transform: translateY(-2px); box-shadow: 0 6px 20px rgba(0,0,0,0.15); }
    .stat-card.active { background: linear-gradient(135deg,#667eea,#764ba2); }
    .stat-card.active .stat-value { -webkit-text-fill-color: #fff; }
    .stat-card.active .stat-label { color: #fff; }
    .stat-value { font-size: 24px; font-weight: bold; margin-bottom: 5px; }
    .stat-label { font-size: 14px; color: #666; font-weight: 500; }
    .stat-total { border-left: 4px solid #667eea; }
    .stat-online { border-left: 4px solid #4caf50; }
    .stat-offline { border-left: 4px solid #f44336; }
    .pagination { display: flex; justify-content: space-between; align-items: center; margin-top: 20px; flex-wrap: wrap; gap: 15px; }
    .page-btn {
      padding: 8px 16px; border: 1px solid #667eea; background: #fff;
      color: #667eea; border-radius: 5px; cursor: pointer;
    }
    .page-btn:hover:not(:disabled) { background: #667eea; color: #fff; }
    .page-btn:disabled { opacity: .5; cursor: not-allowed; }
    .page-size-selector select { padding: 8px; border: 1px solid #667eea; border-radius: 5px; }
    @media (max-width: 768px) {
      .container { padding: 10px; }
      .header { flex-direction: column; gap: 15px; text-align: center; }
      .filters { justify-content: center; }
      .pagination { flex-direction: column; }
      th, td { padding: 8px 4px; font-size: 13px; }
    }
  </style>
</head>
<body>
  <div id="app">
    <div class="container">
      <div class="header">
        <h1 class="title">客户端列表</h1>
        <button class="logout-btn" @click="logout">退出</button>
      </div>
      <div class="stats-container">
        <div class="stat-card stat-total" :class="{ active: filter === 'all' }" @click="filter = 'all'">
          <div class="stat-value">{{ totalClients }}</div>
          <div class="stat-label">客户端总数</div>
        </div>
        <div class="stat-card stat-online" :class="{ active: filter === 'online' }" @click="filter = 'online'">
          <div class="stat-value">{{ onlineClients }}</div>
          <div class="stat-label">在线客户端</div>
        </div>
        <div class="stat-card stat-offline" :class="{ active: filter === 'offline' }" @click="filter = 'offline'">
          <div class="stat-value">{{ offlineClients }}</div>
          <div class="stat-label">离线客户端</div>
        </div>
      </div>
      <div class="main-content">
        <div class="filters">
          <button class="filter-btn" :class="{ active: filter === 'all' }" @click="filter = 'all'">全部</button>
          <button class="filter-btn" :class="{ active: filter === 'online' }" @click="filter = 'online'">在线</button>
          <button class="filter-btn" :class="{ active: filter === 'offline' }" @click="filter = 'offline'">离线</button>
        </div>
        <div class="table-container">
          <table>
            <thead>
              <tr>
                <th @click="sortBy('虚拟IP')" class="sortable" style="cursor:pointer">
                  虚拟IP
                  <span class="sort-indicator">
                    <span v-if="sortKey === '虚拟IP' && sortOrder === 'asc'">↑</span>
                    <span v-else-if="sortKey === '虚拟IP' && sortOrder === 'desc'">↓</span>
                    <span v-else style="color:#fff9">↕</span>
                  </span>
                </th>
                <th>主机名</th>
                <th>版本</th>
                <th>状态</th>
                <th>设备ID</th>
                <th>加密</th>
                <th>上传流量</th>
                <th>下载流量</th>
                <th @click="sortBy('上线时间')" class="sortable" style="cursor:pointer">
                  上线时间
                  <span class="sort-indicator">
                    <span v-if="sortKey === '上线时间' && sortOrder === 'asc'">↑</span>
                    <span v-else-if="sortKey === '上线时间' && sortOrder === 'desc'">↓</span>
                    <span v-else style="color:#fff9">↕</span>
                  </span>
                </th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="d in pageDevices" :key="d.虚拟IP + (d.设备ID || '')" :data-type="d.类型">
                <td>{{ d.虚拟IP }}</td>
                <td>{{ d.名称 }}</td>
                <td>{{ d.版本 || '-' }}</td>
                <td><span :class="'status-badge ' + (d.状态 === '在线' ? 'status-online' : 'status-offline')">{{ d.状态 }}</span></td>
                <td style="font-family:monospace;font-size:12px">{{ d.设备ID || '-' }}</td>
                <td>{{ d.加密 || '-' }}</td>
                <td>{{ d.上传 || '-' }}</td>
                <td>{{ d.下载 || '-' }}</td>
                <td>{{ d.上线时间 || '-' }}</td>
              </tr>
            </tbody>
          </table>
        </div>
        <div class="pagination">
          <div>
            <button class="page-btn" @click="page--" :disabled="page <= 1">上一页</button>
            <span style="margin:0 12px">第 {{ page }} / {{ totalPages }} 页</span>
            <button class="page-btn" @click="page++" :disabled="page >= totalPages">下一页</button>
          </div>
          <div class="page-size-selector">
            <label>每页：</label>
            <select v-model.number="pageSize" @change="page=1">
              <option value="10">10</option>
              <option value="20">20</option>
              <option value="50">50</option>
              <option value="9999">全部</option>
            </select>
          </div>
        </div>
      </div>
    </div>
  </div>
  <script>
    const { createApp } = Vue;
    createApp({
      data() {
        return {
          allDevices: ${JSON.stringify(devices)},
          filter: 'all',
          page: 1,
          pageSize: 10,
          sortKey: '',
          sortOrder: 'asc'
        };
      },
      computed: {
        sorted() {
          const gateway = this.allDevices.filter(d => d.类型 === '网关');
          const clients = this.allDevices.filter(d => d.类型 !== '网关');
          if (!this.sortKey) return [...gateway, ...clients];
          const sorted = [...clients].sort((a, b) => {
            let av = a[this.sortKey], bv = b[this.sortKey];
            if (this.sortKey === '虚拟IP') { av = this.ipToNum(av); bv = this.ipToNum(bv); }
            if (this.sortKey === '上线时间') { av = new Date(av).getTime(); bv = new Date(bv).getTime(); }
            if (av < bv) return this.sortOrder === 'asc' ? -1 : 1;
            if (av > bv) return this.sortOrder === 'asc' ? 1 : -1;
            return 0;
          });
          return [...gateway, ...sorted];
        },
        filtered() {
          let list = this.sorted;
          if (this.filter === 'online') list = list.filter(d => d.状态 === '在线');
          if (this.filter === 'offline') list = list.filter(d => d.状态 === '离线');
          return list;
        },
        totalClients() { return this.allDevices.filter(d => d.类型 !== '网关').length; },
        onlineClients() { return this.allDevices.filter(d => d.状态 === '在线' && d.类型 !== '网关').length; },
        offlineClients() { return this.allDevices.filter(d => d.状态 === '离线').length; },
        totalPages() { return Math.ceil(this.filtered.length / this.pageSize) || 1; },
        pageDevices() {
          const end = this.page * this.pageSize;
          return this.filtered.slice(end - this.pageSize, end);
        }
      },
      methods: {
        sortBy(key) {
          if (this.sortKey === key) { this.sortOrder = this.sortOrder === 'asc' ? 'desc' : 'asc'; }
          else { this.sortKey = key; this.sortOrder = 'asc'; }
          this.page = 1;
        },
        ipToNum(ip) {
          if (!ip) return 0;
          return ip.split('.').reduce((a, p, i) => a + (parseInt(p) || 0) * Math.pow(256, 3 - i), 0);
        },
        logout() {
          document.cookie = 'network_code=; expires=Thu, 01 Jan 1970 00:00:00 UTC; path=/;';
          document.cookie = 'gateway_ip=; expires=Thu, 01 Jan 1970 00:00:00 UTC; path=/;';
          window.location.href = window.location.origin + '/room';
        }
      }
    }).mount('#app');
  </script>
</body>
</html>`;
}

function renderPeerLoginHtml(errorMessage) {
  return `<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>互联服务端 - VNT2</title>
  <script src="https://unpkg.com/vue@3/dist/vue.global.js"></script>
  <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body {
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
      background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
      min-height: 100vh; display: flex; align-items: center; justify-content: center;
    }
    .modal {
      background: rgba(255,255,255,0.96); width: calc(100vw - 48px); max-width: 420px;
      padding: 36px 32px; border-radius: 16px;
      box-shadow: 0 20px 60px rgba(0,0,0,0.28); backdrop-filter: blur(10px);
    }
    .modal h2 {
      margin-bottom: 30px; text-align: center;
      background: linear-gradient(45deg,#667eea,#764ba2);
      -webkit-background-clip: text; -webkit-text-fill-color: transparent;
      font-size: 24px;
    }
    .form-group { margin-bottom: 20px; }
    .form-group input {
      width: 100%; padding: 12px; border: 2px solid #e0e0e0;
      border-radius: 8px; font-size: 14px;
    }
    .form-group input:focus { outline: none; border-color: #667eea; }
    .submit-btn {
      display: block; margin: 0 auto; padding: 12px 40px;
      background: linear-gradient(45deg,#667eea,#764ba2); color: #fff;
      border: none; border-radius: 8px; font-size: 16px; font-weight: 500; cursor: pointer;
    }
    .submit-btn:hover { transform: translateY(-2px); box-shadow: 0 5px 15px rgba(102,126,234,.3); }
    .remember-row {
      display: flex; align-items: center; justify-content: center;
      margin: 16px 0 20px; gap: 8px;
    }
    .remember-row input[type="checkbox"] { width: 16px; height: 16px; cursor: pointer; }
    .remember-row label { font-size: 14px; color: #555; cursor: pointer; user-select: none; }
    .error-message {
      background: #ffebee; color: #c62828; padding: 12px; border-radius: 8px;
      margin-bottom: 20px; border: 1px solid #ef5350; font-size: 14px;
    }
  </style>
</head>
<body>
  <div id="app">
    <div class="modal">
      <h2>互联服务端验证</h2>
      <div v-if="showError" class="error-message">{{ errorMessage }}</div>
      <div class="form-group">
        <input v-model="token" placeholder="请输入 SERVER_TOKEN" @keyup.enter="login" @focus="inputType='text'" @blur="inputType='password'" :type="inputType" />
      </div>
      <div class="remember-row">
        <input type="checkbox" id="rememberPeerToken" v-model="rememberToken" />
        <label for="rememberPeerToken">记住令牌</label>
      </div>
      <button class="submit-btn" @click="login">进入</button>
    </div>
  </div>
  <script>
    const { createApp } = Vue;
    createApp({
      data() { return {
        token: localStorage.getItem('vnts2_peer_token') || '',
        inputType: 'password',
        rememberToken: !!localStorage.getItem('vnts2_peer_token'),
        showError: ${errorMessage ? "true" : "false"},
        errorMessage: ${errorMessage ? JSON.stringify(errorMessage) : "''"}
      }; },
      methods: {
        login() {
          if (!this.token) { this.showError = true; this.errorMessage = '请输入互联令牌'; return; }
          if (this.rememberToken) { localStorage.setItem('vnts2_peer_token', this.token); }
          else { localStorage.removeItem('vnts2_peer_token'); }
          document.cookie = 'peer_auth=' + encodeURIComponent(this.token) + '; path=/; max-age=86400; SameSite=Lax';
          location.href = '/peer';
        }
      }
    }).mount('#app');
  </script>
</body>
</html>`;
}

function renderPeerHtml(servers) {
  return `<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>互联服务端 - VNT2</title>
  <script src="https://unpkg.com/vue@3/dist/vue.global.js"></script>
  <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body {
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
      background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
      min-height: 100vh;
    }
    .container { max-width: 1200px; margin: 0 auto; padding: 20px; }
    .header {
      background: rgba(255,255,255,0.95); border-radius: 15px; padding: 20px;
      margin-bottom: 20px; box-shadow: 0 10px 30px rgba(0,0,0,0.1);
      backdrop-filter: blur(10px); display: flex; justify-content: space-between; align-items: center;
    }
    .title {
      font-size: 24px; font-weight: bold;
      background: linear-gradient(45deg,#667eea,#764ba2);
      -webkit-background-clip: text; -webkit-text-fill-color: transparent;
    }
    .logout-btn {
      background: linear-gradient(45deg,#f44336,#e91e63); color: #fff;
      border: none; padding: 10px 20px; border-radius: 25px; cursor: pointer;
      font-weight: 500; transition: all .3s ease;
    }
    .logout-btn:hover { transform: translateY(-2px); box-shadow: 0 5px 15px rgba(244,67,54,.3); }
    .main-content {
      background: rgba(255,255,255,0.95); border-radius: 15px; padding: 20px;
      box-shadow: 0 10px 30px rgba(0,0,0,0.1); backdrop-filter: blur(10px);
    }
    table { width: 100%; border-collapse: collapse; background: #fff; }
    th, td { padding: 14px; text-align: center; border-bottom: 1px solid #f0f0f0; border-right: 1px solid #e8e8e8; }
    th {
      background: linear-gradient(45deg,#1761ea,#066ce9); color: #fff;
      font-weight: 600; border-right: 1px solid rgba(255,255,255,0.3);
    }
    tr:nth-child(even) td { background: #c3c9ed; }
    tr:nth-child(odd) td { background: #b6aaf1; }
    tr:hover td { background: #ee6fdf !important; }
    .status-badge { padding: 4px 12px; border-radius: 15px; font-size: 12px; font-weight: 500; display: inline-block; }
    .status-online { background: linear-gradient(45deg,#4caf50,#45a049); color: #fff; }
    .status-offline { background: linear-gradient(45deg,#f44336,#e91e63); color: #fff; }
    .empty { text-align: center; padding: 60px 20px; color: #999; }
    @media (max-width: 768px) { th, td { padding: 8px 4px; font-size: 13px; } }
  </style>
</head>
<body>
  <div id="app">
    <div class="container">
      <div class="header">
        <h1 class="title">互联服务端 ({{ servers.length }})</h1>
        <button class="logout-btn" @click="logout">退出</button>
      </div>
      <div class="main-content">
        <table v-if="servers.length">
          <thead><tr>
            <th>地址</th>
            <th>状态</th>
          </tr></thead>
          <tbody>
            <tr v-for="s in servers" :key="s.addr">
              <td style="font-family:monospace;font-size:13px">{{ s.addr }}</td>
              <td><span :class="'status-badge ' + (s.online ? 'status-online' : 'status-offline')">{{ s.online ? '在线' : '离线' }}</span></td>
            </tr>
          </tbody>
        </table>
        <div v-else class="empty">未配置互联服务端</div>
      </div>
    </div>
  </div>
  <script>
    const { createApp } = Vue;
    createApp({
      data() { return { servers: ${JSON.stringify(servers)} }; },
      methods: {
        logout() {
          document.cookie = 'peer_auth=; expires=Thu, 01 Jan 1970 00:00:00 UTC; path=/;';
          window.location.href = '/peer';
        }
      }
    }).mount('#app');
  </script>
</body>
</html>`;
}

function renderLogLoginHtml(errorMessage) {
  return `<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>日志验证 - vnts2-cf</title>
  <script src="https://unpkg.com/vue@3/dist/vue.global.js"></script>
  <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body {
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
      background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
      min-height: 100vh;
      display: flex;
      align-items: center;
      justify-content: center;
    }
    .modal {
      background: rgba(255, 255, 255, 0.96);
      width: calc(100vw - 48px);
      max-width: 400px;
      min-width: 280px;
      padding: 36px 32px;
      border-radius: 16px;
      box-shadow: 0 20px 60px rgba(0, 0, 0, 0.28);
      backdrop-filter: blur(10px);
    }
    .modal h2 {
      margin-bottom: 30px;
      text-align: center;
      background: linear-gradient(45deg, #667eea, #764ba2);
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;
      font-size: 24px;
    }
    .form-group { margin-bottom: 20px; }
    .form-group input {
      width: 100%; padding: 12px;
      border: 2px solid #e0e0e0; border-radius: 8px;
      font-size: 14px; transition: border-color .3s ease;
    }
    .form-group input:focus { outline: none; border-color: #667eea; }
    .submit-btn {
      display: block; margin: 0 auto; padding: 12px 40px;
      background: linear-gradient(45deg, #667eea, #764ba2);
      color: #fff; border: none; border-radius: 8px;
      font-size: 16px; font-weight: 500; cursor: pointer;
      transition: all .3s ease;
    }
    .submit-btn:hover { transform: translateY(-2px); box-shadow: 0 5px 15px rgba(102,126,234,.3); }
    .remember-row {
      display: flex; align-items: center; justify-content: center;
      margin: 16px 0 20px; gap: 8px;
    }
    .remember-row input[type="checkbox"] { width: 16px; height: 16px; cursor: pointer; }
    .remember-row label { font-size: 14px; color: #555; cursor: pointer; user-select: none; }
    .error-message {
      background: #ffebee; color: #c62828; padding: 12px;
      border-radius: 8px; margin-bottom: 20px;
      border: 1px solid #ef5350; font-size: 14px;
    }
  </style>
</head>
<body>
  <div id="app">
    <div class="modal">
      <h2>日志验证</h2>
      <form method="POST" action="" @submit.prevent="login">
        <div v-if="showError" class="error-message">{{ errorMessage }}</div>
        <div class="form-group">
          <input v-model="loginForm.password" :type="inputType" placeholder="请输入日志密码" @keyup.enter="login" @focus="inputType='text'" @blur="inputType='password'" />
        </div>
        <div class="remember-row">
          <input type="checkbox" id="rememberPwd" v-model="rememberPwd" />
          <label for="rememberPwd">记住密码</label>
        </div>
        <button class="submit-btn">确认登录</button>
      </form>
    </div>
  </div>
  <script>
    const { createApp } = Vue;
    createApp({
      data() {
        const saved = localStorage.getItem('vnts2_log_pwd') || '';
        return {
          loginForm: { password: saved },
          inputType: 'password',
          rememberPwd: !!saved,
          showError: ${errorMessage ? "true" : "false"},
          errorMessage: ${errorMessage ? JSON.stringify(errorMessage) : "''"}
        };
      },
      methods: {
        login() {
          if (!this.loginForm.password) {
            this.showError = true;
            this.errorMessage = '请输入密码！';
            return;
          }
          if (this.rememberPwd) {
            localStorage.setItem('vnts2_log_pwd', this.loginForm.password);
          } else {
            localStorage.removeItem('vnts2_log_pwd');
          }
          document.cookie = 'log_auth=' + encodeURIComponent(this.loginForm.password) + '; path=/; max-age=86400; SameSite=Lax';
          event.target.submit();
        }
      }
    }).mount('#app');
  </script>
</body>
</html>`;
}

function renderLogHtml(data) {
  const logs = data.logs || [];
  return `<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>服务日志 - VNT2</title>
  <script src="https://unpkg.com/vue@3/dist/vue.global.js"></script>
  <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #f5f5f5; }
    .header {
      background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
      color: #fff; padding: 20px;
      display: flex; justify-content: space-between; align-items: center;
    }
    .title { font-size: 24px; font-weight: bold; }
    .logout-btn, .clear-btn {
      background: rgba(255,255,255,0.2); color: #fff;
      border: 1px solid rgba(255,255,255,0.3); padding: 8px 16px;
      border-radius: 20px; cursor: pointer; transition: all .3s ease; margin-left: 10px;
    }
    .logout-btn:hover, .clear-btn:hover { background: rgba(255,255,255,0.3); }
    .clear-btn { background: rgba(244,67,54,0.8); border-color: rgba(244,67,54,0.9); }
    .clear-btn:hover { background: rgba(244,67,54,0.9); }
    .container { max-width: 1200px; margin: 0 auto; padding: 20px; }
    .log-container {
      background: #fff; border-radius: 10px;
      box-shadow: 0 2px 10px rgba(0,0,0,0.1); overflow: hidden;
    }
    .log-item {
      padding: 12px 20px; border-bottom: 1px solid #f0f0f0;
      font-family: Consolas, Monaco, monospace; font-size: 13px; line-height: 1.5;
    }
    .log-item:last-child { border-bottom: none; }
    .log-time { color: #666; margin-right: 15px; }
    .log-level {
      padding: 2px 8px; border-radius: 4px; font-weight: bold;
      margin-right: 15px; min-width: 50px; text-align: center; display: inline-block;
    }
    .level-error { background: #ffebee; color: #c62828; }
    .level-warn { background: #fff3e0; color: #ef6c00; }
    .level-info { background: #e3f2fd; color: #1565c0; }
    .level-debug { background: #f3e5f5; color: #7b1fa2; }
    .log-message { color: #333; }
    .empty-logs { text-align: center; padding: 60px 20px; color: #999; }
    .success-message {
      background: #e8f5e8; color: #2e7d32; padding: 12px; border-radius: 8px;
      margin-bottom: 20px; border: 1px solid #4caf50; font-size: 14px;
    }
    .error-message {
      background: #ffebee; color: #c62828; padding: 12px; border-radius: 8px;
      margin-bottom: 20px; border: 1px solid #ef5350; font-size: 14px;
    }
    .back-to-top {
      position: fixed; bottom: 20px; right: 20px; width: 50px; height: 50px;
      border-radius: 50%; background: linear-gradient(135deg,#667eea,#764ba2);
      color: #fff; font-size: 24px; font-weight: bold; text-align: center;
      line-height: 50px; cursor: pointer; box-shadow: 0 2px 10px rgba(0,0,0,0.2);
      transition: all .3s ease; z-index: 9999;
      opacity: 0; pointer-events: none;
    }
    .back-to-top.show { opacity: 1; pointer-events: auto; }
    .back-to-top:hover { transform: translateY(-2px); box-shadow: 0 4px 15px rgba(0,0,0,0.3); }
  </style>
</head>
<body>
  <div id="app">
    <div class="header">
      <div class="title">服务运行日志 (${logs.length})</div>
      <div>
        <button class="clear-btn" @click="clearLogs">清空日志</button>
        <button class="logout-btn" @click="logout">退出</button>
      </div>
    </div>
    <div class="container">
      <div v-if="showNotification" :class="notificationType === 'success' ? 'success-message' : 'error-message'">
        {{ notificationMessage }}
      </div>
      <div class="log-container">
        <div v-if="logs.length === 0" class="empty-logs">暂无日志记录</div>
        <div v-for="log in logs" :key="log.timestamp" class="log-item" ref="logItems">
          <span class="log-time">{{ formatTime(log.timestamp) }}</span>
          <span :class="'log-level level-' + log.level">{{ log.level.toUpperCase() }}</span>
          <span class="log-message">{{ log.message }}</span>
        </div>
      </div>
    </div>
    <button class="back-to-top" @click="scrollToTop" :class="{ show: showBackToTop }">&#x1F51D;</button>
  </div>
  <script>
    const { createApp } = Vue;
    createApp({
      data() {
        return {
          logs: ${JSON.stringify(logs)},
          showBackToTop: false,
          showNotification: false,
          notificationType: '',
          notificationMessage: ''
        };
      },
      mounted() {
        window.addEventListener('scroll', this.handleScroll);
      },
      beforeUnmount() { window.removeEventListener('scroll', this.handleScroll); },
      methods: {
        handleScroll() { this.showBackToTop = window.scrollY > 200; },
        scrollToTop() { window.scrollTo({ top: 0, behavior: 'smooth' }); },
        formatTime(ts) {
          if (!ts) return '';
          const d = new Date(ts);
          return d.toLocaleString('zh-CN', { year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit' });
        },
        logout() {
          document.cookie = 'log_auth=; expires=Thu, 01 Jan 1970 00:00:00 UTC; path=/;';
          window.location.href = '/log';
        },
        async clearLogs() {
          if (!confirm('确定要清空所有日志吗？此操作不可恢复！')) return;
          try {
            const r = await fetch(window.location.href + '/clear', { method: 'POST', headers: { 'Content-Type': 'application/json' } });
            const result = await r.json();
            if (r.ok && result.status === 'ok') {
              this.logs = [];
              this.showNotification = true; this.notificationType = 'success'; this.notificationMessage = '日志已成功清空';
            } else {
              this.showNotification = true; this.notificationType = 'error'; this.notificationMessage = result.error || result.message || '清空日志失败';
            }
            setTimeout(() => { this.showNotification = false; }, 4000);
          } catch (e) {
            this.showNotification = true; this.notificationType = 'error'; this.notificationMessage = '网络错误，请稍后重试';
            setTimeout(() => { this.showNotification = false; }, 3000);
          }
        }
      }
    }).mount('#app');
  </script>
</body>
</html>`;
}
