#!/usr/bin/env node
// Generates extension/icons/icon{16,32,48,128}.png from pure Node.js.
// Design: dark navy background (#0f172a) + white "IQ" lettermark.
"use strict";

const fs = require("fs");
const path = require("path");
const zlib = require("zlib");

// ---- PNG encoder ----

const CRC_TABLE = (() => {
  const t = new Uint32Array(256);
  for (let n = 0; n < 256; n++) {
    let c = n;
    for (let k = 0; k < 8; k++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
    t[n] = c;
  }
  return t;
})();

function crc32(buf) {
  let c = 0xffffffff;
  for (let i = 0; i < buf.length; i++) c = (c >>> 8) ^ CRC_TABLE[(c ^ buf[i]) & 0xff];
  return (c ^ 0xffffffff) >>> 0;
}

function pngChunk(type, data) {
  const t = Buffer.from(type, "ascii");
  const d = Buffer.isBuffer(data) ? data : Buffer.alloc(0);
  const len = Buffer.alloc(4);
  len.writeUInt32BE(d.length, 0);
  const crc = Buffer.alloc(4);
  crc.writeUInt32BE(crc32(Buffer.concat([t, d])), 0);
  return Buffer.concat([len, t, d, crc]);
}

function encodePNG(width, height, getRGB) {
  const stride = width * 3 + 1;
  const raw = Buffer.alloc(height * stride);
  for (let y = 0; y < height; y++) {
    raw[y * stride] = 0; // filter: None
    for (let x = 0; x < width; x++) {
      const [r, g, b] = getRGB(x, y);
      const off = y * stride + 1 + x * 3;
      raw[off] = r;
      raw[off + 1] = g;
      raw[off + 2] = b;
    }
  }
  const compressed = zlib.deflateSync(raw, { level: 9 });
  const ihdr = Buffer.alloc(13);
  ihdr.writeUInt32BE(width, 0);
  ihdr.writeUInt32BE(height, 4);
  ihdr[8] = 8; // bit depth
  ihdr[9] = 2; // RGB
  const sig = Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]);
  return Buffer.concat([
    sig,
    pngChunk("IHDR", ihdr),
    pngChunk("IDAT", compressed),
    pngChunk("IEND", Buffer.alloc(0)),
  ]);
}

// ---- Icon design (virtual 100×100 canvas) ----
// "I" letterform: x [10,38], y [14,86]
// Gap: x [38,46]
// "Q" letterform: x [46,90], y [14,86]

const BG = [15, 23, 42];    // #0f172a
const FG = [248, 250, 252]; // #f8fafc

function buildParams(size) {
  // Adaptive minimum stroke so every mark is >= 2px at render size.
  // Virtual units: minStroke = max(10, ceil(200 / size))
  const minStroke = Math.max(10, Math.ceil(200 / size));
  return {
    // "I" letter
    iLeft: 10, iRight: 38, iTop: 14, iBottom: 86,
    iStemL: 20, iStemR: 28,
    iSerifH: minStroke,
    // "Q" letter — ring centered in [46,90] × [14,86]
    qCX: 68, qCY: 50,
    qOuterR: 18,
    qInnerR: Math.max(4, 18 - minStroke),
    // "Q" tail: bottom-right notch
    qTailX1: 72, qTailX2: 90,
    qTailY1: 64, qTailY2: 84,
  };
}

function isLit(vx, vy, p) {
  // "I" — serifs + stem
  if (vx >= p.iLeft && vx <= p.iRight && vy >= p.iTop && vy <= p.iBottom) {
    if (vy <= p.iTop + p.iSerifH) return true;
    if (vy >= p.iBottom - p.iSerifH) return true;
    if (vx >= p.iStemL && vx <= p.iStemR) return true;
  }

  // "Q" — donut ring + tail
  const dx = vx - p.qCX;
  const dy = vy - p.qCY;
  const dist = Math.sqrt(dx * dx + dy * dy);
  if (dist >= p.qInnerR && dist <= p.qOuterR) {
    // Open the ring at the bottom-right to connect with the tail.
    // Suppress the ring in the ~30..80 deg sector (screen coords, y-down).
    const deg = Math.atan2(dy, dx) * (180 / Math.PI); // -180..180
    if (deg < 30 || deg > 80) return true;
  }
  if (vx >= p.qTailX1 && vx <= p.qTailX2 && vy >= p.qTailY1 && vy <= p.qTailY2) return true;

  return false;
}

function makeIcon(size) {
  // 2×2 supersampling for anti-aliased edges at every size.
  const p = buildParams(size);
  return encodePNG(size, size, (px, py) => {
    let r = 0, g = 0, b = 0;
    for (let sy = 0; sy < 2; sy++) {
      for (let sx = 0; sx < 2; sx++) {
        const vx = (px + (sx + 0.5) / 2) / size * 100;
        const vy = (py + (sy + 0.5) / 2) / size * 100;
        const [cr, cg, cb] = isLit(vx, vy, p) ? FG : BG;
        r += cr; g += cg; b += cb;
      }
    }
    return [Math.round(r / 4), Math.round(g / 4), Math.round(b / 4)];
  });
}

// ---- Generate ----

const SIZES = [16, 32, 48, 128];
const OUT_DIR = path.resolve(__dirname, "..", "extension", "icons");
fs.mkdirSync(OUT_DIR, { recursive: true });

for (const size of SIZES) {
  const buf = makeIcon(size);
  const outFile = path.join(OUT_DIR, `icon${size}.png`);
  fs.writeFileSync(outFile, buf);
  process.stdout.write(`  icon${size}.png  (${buf.length} B)\n`);
}
process.stdout.write("done.\n");
