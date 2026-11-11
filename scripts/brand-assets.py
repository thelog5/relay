"""Builds web/static from the black source logo: lavender gradient mark,
near-white wordmark, and favicons.

    python3 scripts/brand-assets.py assets/brand/relay-logo.png web/static
"""
import sys, os
from PIL import Image, ImageDraw

src, out = sys.argv[1], sys.argv[2]
os.makedirs(out, exist_ok=True)
im = Image.open(src).convert("RGBA")
alpha = im.getchannel("A")
SPLIT = 820  # mark ends at x=786, wordmark starts at 853

def hexrgb(h): return tuple(int(h[i:i+2], 16) for i in (1, 3, 5))
TOP, BOTTOM, INK = hexrgb("#dcd5ff"), hexrgb("#8f7df7"), hexrgb("#f3f2f8")

def gradient(w, h):
    g = Image.new("RGB", (w, h))
    px = g.load()
    for y in range(h):
        for x in range(w):
            t = (x / w * 0.55 + y / h * 0.45)
            px[x, y] = tuple(round(TOP[i] + (BOTTOM[i] - TOP[i]) * t) for i in range(3))
    return g

def colored(box, fill):
    a = alpha.crop(box)
    rgb = fill(a.size)
    rgb.putalpha(a)
    return rgb

mark_box = alpha.crop((0, 0, SPLIT, im.height)).getbbox()
word_box = alpha.crop((SPLIT, 0, im.width, im.height)).getbbox()
word_box = (word_box[0] + SPLIT, word_box[1], word_box[2] + SPLIT, word_box[3])
mark = colored(mark_box, lambda s: gradient(*s))
word = colored(word_box, lambda s: Image.new("RGB", s, INK))

# Full lockup, keeping the original spacing between mark and wordmark.
x0, y0 = mark_box[0], min(mark_box[1], word_box[1])
x1, y1 = word_box[2], max(mark_box[3], word_box[3])
lock = Image.new("RGBA", (x1 - x0, y1 - y0), (0, 0, 0, 0))
lock.alpha_composite(mark, (mark_box[0] - x0, mark_box[1] - y0))
lock.alpha_composite(word, (word_box[0] - x0, word_box[1] - y0))

def fit_h(img, h):
    return img.resize((round(img.width * h / img.height), h), Image.LANCZOS)

fit_h(lock, 96).save(os.path.join(out, "logo.png"), optimize=True)
fit_h(mark, 96).save(os.path.join(out, "mark.png"), optimize=True)

def icon(size, pad=0.16, radius=0.22):
    big = size * 8  # draw large, downsample for smooth edges
    bg = Image.new("RGBA", (big, big), (0, 0, 0, 0))
    d = ImageDraw.Draw(bg)
    d.rounded_rectangle((0, 0, big - 1, big - 1), radius=round(big * radius), fill=hexrgb("#16141f") + (255,))
    inner = round(big * (1 - 2 * pad))
    m = mark.resize((inner, round(mark.height * inner / mark.width)), Image.LANCZOS)
    bg.alpha_composite(m, ((big - m.width) // 2, (big - m.height) // 2))
    return bg.resize((size, size), Image.LANCZOS)

icon(32, pad=0.07, radius=0.24).save(os.path.join(out, "favicon-32.png"), optimize=True)
icon(180, pad=0.18).save(os.path.join(out, "apple-touch-icon.png"), optimize=True)
icon(512, pad=0.18).save(os.path.join(out, "icon-512.png"), optimize=True)
print("mark", mark_box, "word", word_box)
for f in sorted(os.listdir(out)):
    p = os.path.join(out, f); print(f, Image.open(p).size, os.path.getsize(p), "bytes")
