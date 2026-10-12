# -*- coding: utf-8 -*-
"""生成 TaskConfirmMax 插件图标（stdlib-only，无 Pillow 依赖）。

与 taskconfirmbridge.png 同构（白色写字板 + 内容线条），
但底色换成紫罗兰渐变，并在右下角加一枚橙色「升级」三角标，便于区分。
"""
import struct
import zlib
import math

S = 2            # 超采样倍率
W = 256 * S      # 画布
OUT = 256


def blend(c1, c2, t):
    t = max(0.0, min(1.0, t))
    return tuple(round(a + (b - a) * t) for a, b in zip(c1, c2))


BG = (124, 58, 237)      # #7c3aed 紫
BG_DARK = (67, 56, 202)  # #4338ca 靛
WHITE = (255, 255, 255)
GRAY = (203, 213, 225)
LINE = (124, 58, 237)
LINE2 = (56, 189, 248)   # #38bdf8 浅蓝
BOOST = (251, 146, 60)   # #fb923c 橙


def rounded_rect_dist(x, y, cx0, cy0, cx1, cy1, r):
    qx = max(cx0 + r - x, 0, x - (cx1 - r))
    qy = max(cy0 + r - y, 0, y - (cy1 - r))
    return math.hypot(qx, qy) - r


def in_triangle(x, y, p1, p2, p3):
    def sign(a, b, c):
        return (a[0] - c[0]) * (b[1] - c[1]) - (b[0] - c[0]) * (a[1] - c[1])
    d1 = sign((x, y), p1, p2)
    d2 = sign((x, y), p2, p3)
    d3 = sign((x, y), p3, p1)
    neg = (d1 < 0) or (d2 < 0) or (d3 < 0)
    pos = (d1 > 0) or (d2 > 0) or (d3 > 0)
    return not (neg and pos)


def pixel(px, py):
    d = rounded_rect_dist(px, py, 4, 4, W - 4, W - 4, 44 * S)
    if d > 0:
        return None
    t = min(1.0, math.hypot(px - W / 2, py - W / 2) / (W * 0.75))
    c = blend(BG, BG_DARK, t * 0.65)
    # 白色写字板
    d = rounded_rect_dist(px, py, 52 * S, 44 * S, 204 * S, 212 * S, 18 * S)
    if d <= 0:
        c = WHITE
    # 顶部夹子
    d = rounded_rect_dist(px, py, 96 * S, 34 * S, 160 * S, 74 * S, 10 * S)
    if d <= 0:
        c = GRAY
    # 三根内容线条（紫→浅蓝渐变）
    for (x0, x1, y0, y1, col) in [
        (76, 180, 96, 108, LINE),
        (76, 180, 124, 136, LINE),
        (76, 148, 152, 164, LINE2),
    ]:
        d = rounded_rect_dist(px, py, x0 * S, y0 * S, x1 * S, y1 * S, (y1 - y0) / 2 * S)
        if d <= 0:
            c = col
    # 右下角「升级」三角标（白底 + 橙色箭头）
    board_r = rounded_rect_dist(px, py, 52 * S, 44 * S, 204 * S, 212 * S, 18 * S)
    tri = in_triangle(px, py, (150 * S, 196 * S), (198 * S, 196 * S), (174 * S, 158 * S))
    if board_r <= 0 and tri:
        c = BOOST
    return c


rows = []
for oy in range(OUT):
    row = bytearray()
    for ox in range(OUT):
        r = g = b = a = 0
        n = 0
        for sy in (0.25, 0.75):
            for sx in (0.25, 0.75):
                px = (ox + sx) * S
                py = (oy + sy) * S
                c = pixel(px, py)
                if c is not None:
                    r += c[0]; g += c[1]; b += c[2]; a += 255
                n += 1
        row += bytes((round(r / n), round(g / n), round(b / n), round(a / n)))
    rows.append(bytes(row))

raw = b"".join(b"\x00" + row for row in rows)


def chunk(tag, data):
    c = struct.pack(">I", len(data)) + tag + data
    return c + struct.pack(">I", zlib.crc32(tag + data) & 0xFFFFFFFF)


png = (b"\x89PNG\r\n\x1a\n"
       + chunk(b"IHDR", struct.pack(">IIBBBBB", OUT, OUT, 8, 6, 0, 0, 0))
       + chunk(b"IDAT", zlib.compress(raw, 9))
       + chunk(b"IEND", b""))

open("taskconfirmmax.png", "wb").write(png)
print("ICON_OK", len(png), "bytes")
