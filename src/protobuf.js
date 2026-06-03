const MAX_PROTO_BYTES = 2 * 1024 * 1024;

export class ProtoReader {
  constructor(bytes) {
    this.bytes = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes);
    this.pos = 0;
  }

  eof() {
    return this.pos >= this.bytes.length;
  }

  readTag() {
    const tag = this.readVarint();
    return { field: Number(tag >> 3n), wire: Number(tag & 7n) };
  }

  readVarint() {
    let shift = 0n;
    let value = 0n;
    while (this.pos < this.bytes.length) {
      const b = this.bytes[this.pos++];
      value |= BigInt(b & 0x7f) << shift;
      if ((b & 0x80) === 0) return value;
      shift += 7n;
      if (shift > 70n) throw new Error("varint 过长");
    }
    throw new Error("读取 varint 时遇到意外 EOF");
  }

  readBool() {
    return this.readVarint() !== 0n;
  }

  readU32() {
    return Number(this.readVarint() & 0xffffffffn) >>> 0;
  }

  readI64() {
    const v = this.readVarint();
    return v > 0x7fffffffffffffffn ? Number(v - 0x10000000000000000n) : Number(v);
  }

  readFixed32() {
    if (this.pos + 4 > this.bytes.length) throw new Error("读取 fixed32 时遇到意外 EOF");
    const v =
      (this.bytes[this.pos]) |
      (this.bytes[this.pos + 1] << 8) |
      (this.bytes[this.pos + 2] << 16) |
      (this.bytes[this.pos + 3] << 24);
    this.pos += 4;
    return v >>> 0;
  }

  readBytes() {
    const len = Number(this.readVarint());
    if (this.pos + len > this.bytes.length) throw new Error("读取 bytes 时遇到意外 EOF");
    const out = this.bytes.slice(this.pos, this.pos + len);
    this.pos += len;
    return out;
  }

  readString() {
    return new TextDecoder().decode(this.readBytes());
  }

  skip(wire) {
    if (wire === 0) {
      this.readVarint();
    } else if (wire === 1) {
      this.pos += 8;
    } else if (wire === 2) {
      this.pos += Number(this.readVarint());
    } else if (wire === 5) {
      this.pos += 4;
    } else {
      throw new Error(`不支持的 protobuf wire type：${wire}`);
    }
    if (this.pos > this.bytes.length) throw new Error("跳过字段时遇到意外 EOF");
  }
}

export class ProtoWriter {
  constructor() {
    this.buf = [];
  }

  tag(field, wire) {
    this.varint(BigInt((field << 3) | wire));
  }

  varint(value) {
    let v = BigInt(value);
    if (v < 0n) v += 0x10000000000000000n;
    while (v >= 0x80n) {
      this.buf.push(Number((v & 0x7fn) | 0x80n));
      v >>= 7n;
    }
    this.buf.push(Number(v));
  }

  uint(field, value) {
    if (!value) return;
    this.tag(field, 0);
    this.varint(BigInt(value >>> 0));
  }

  uint64(field, value) {
    if (!value) return;
    this.tag(field, 0);
    this.varint(BigInt(value));
  }

  int64(field, value) {
    if (!value) return;
    this.tag(field, 0);
    this.varint(BigInt(value));
  }

  bool(field, value) {
    if (!value) return;
    this.tag(field, 0);
    this.varint(value ? 1n : 0n);
  }

  fixed32(field, value) {
    if (value === undefined || value === null || value === 0) return;
    this.tag(field, 5);
    const v = value >>> 0;
    this.buf.push(v & 0xff, (v >>> 8) & 0xff, (v >>> 16) & 0xff, (v >>> 24) & 0xff);
  }

  string(field, value) {
    if (!value) return;
    this.bytes(field, new TextEncoder().encode(value));
  }

  bytes(field, value) {
    if (!value || value.length === 0) return;
    this.tag(field, 2);
    this.varint(BigInt(value.length));
    this.append(value);
  }

  message(field, value) {
    if (!value) return;
    this.tag(field, 2);
    this.varint(BigInt(value.length));
    this.append(value);
  }

  append(value) {
    if (this.buf.length + value.length > MAX_PROTO_BYTES) throw new Error(`protobuf 消息超过大小限制：${MAX_PROTO_BYTES}`);
    for (const byte of value) this.buf.push(byte);
  }

  finish() {
    if (this.buf.length > MAX_PROTO_BYTES) throw new Error(`protobuf 消息超过大小限制：${MAX_PROTO_BYTES}`);
    return new Uint8Array(this.buf);
  }
}
