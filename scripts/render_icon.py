"""Render the Duckle squircle logo to a high-res PNG for `tauri icon`.

Reproduces docs/assets/logo.svg (a rounded square with a diagonal
#FFF100 -> #FF6900 gradient, a soft top gloss, and a bold 'D') as a
1024x1024 source image. The animated shimmer in the SVG is decorative and
omitted from the static icon. Run from the repo root:

    python scripts/render_icon.py
"""

from PIL import Image, ImageDraw, ImageFont

S = 1024            # output size
VB = 256            # SVG viewBox units
K = S / VB          # scale factor


def lerp(a, b, t):
    return tuple(round(a[i] + (b[i] - a[i]) * t) for i in range(3))


TL = (0xFF, 0xF1, 0x00)   # top-left  #FFF100
BR = (0xFF, 0x69, 0x00)   # bottom-right #FF6900


def main():
    img = Image.new("RGBA", (S, S), (0, 0, 0, 0))

    # Diagonal gradient over the whole canvas (x1,y1=0,0 -> x2,y2=1,1).
    grad = Image.new("RGBA", (S, S))
    gpx = grad.load()
    for y in range(S):
        for x in range(S):
            t = (x + y) / (2 * (S - 1))
            r, g, b = lerp(TL, BR, t)
            gpx[x, y] = (r, g, b, 255)

    # Squircle mask: rect x=28 y=28 w=200 h=200 rx=54 (in viewBox units).
    mask = Image.new("L", (S, S), 0)
    md = ImageDraw.Draw(mask)
    x0, y0 = 28 * K, 28 * K
    x1, y1 = (28 + 200) * K, (28 + 200) * K
    md.rounded_rectangle([x0, y0, x1, y1], radius=54 * K, fill=255)

    img.paste(grad, (0, 0), mask)

    # Top gloss: white fade over the upper ~half, clipped to the squircle.
    gloss = Image.new("RGBA", (S, S), (0, 0, 0, 0))
    gpx = gloss.load()
    gloss_h = int(128 * K)  # rect h=100 from y=28 -> ~y=128
    top = int(28 * K)
    for y in range(top, top + gloss_h):
        f = (y - top) / gloss_h
        a = int(0.34 * 255 * max(0.0, 1.0 - f / 0.55))
        if a <= 0:
            continue
        for x in range(S):
            gpx[x, y] = (255, 255, 255, a)
    gloss_masked = Image.new("RGBA", (S, S), (0, 0, 0, 0))
    gloss_masked.paste(gloss, (0, 0), mask)
    img = Image.alpha_composite(img, gloss_masked)

    # The 'D' glyph: bold, centered, #141008.
    draw = ImageDraw.Draw(img)
    font = None
    for path in (
        "C:/Windows/Fonts/seguibl.ttf",   # Segoe UI Black (~weight 900)
        "C:/Windows/Fonts/segoeuib.ttf",  # Segoe UI Bold
        "C:/Windows/Fonts/arialbd.ttf",   # Arial Bold
    ):
        try:
            font = ImageFont.truetype(path, int(138 * K))
            break
        except OSError:
            continue
    if font is None:
        raise SystemExit("no bold font found")

    cx, cy = 128 * K, 129 * K
    bbox = draw.textbbox((0, 0), "D", font=font)
    tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
    draw.text((cx - tw / 2 - bbox[0], cy - th / 2 - bbox[1]), "D", font=font, fill=(0x14, 0x10, 0x08, 255))

    out = "apps/desktop/icons/icon-source.png"
    img.save(out)
    print("wrote", out, img.size)


if __name__ == "__main__":
    main()
