#!/usr/bin/env python3
"""Capture the TUI via a real pty, render every frame with pyte+Pillow, encode with ffmpeg.

Produces two high-quality GIFs at 1280x800, 30fps:
- docs/assets/demo.gif  — overview: Tasks -> Workers -> Resources -> AsyncOps -> sort/pause/help
- docs/assets/tui.gif   — detail: drill into a task, filter, exit

Why not vhs? `vhs` on this macOS host returns "no frames" even for a trivial
echo (likely tty/ffmpeg capture mismatch). This script drives the console
directly through a pty so we get pixel-identical output without that fragility.
"""
import os
import pty
import select
import subprocess
import sys
import time
from pathlib import Path

import pyte  # type: ignore[import-untyped]
from PIL import Image, ImageDraw, ImageFont  # type: ignore[import-untyped]

ROOT = Path(__file__).resolve().parents[2]
ASSETS = ROOT / "docs" / "assets"
BIN = ROOT / "target" / "debug" / "eddy-console"
REPLAY = ASSETS / "demo.bin"

COLS, ROWS = 160, 40  # matches the TUI's preferred size at 1280x800
WIDTH, HEIGHT = 1280, 800
FPS = 12  # lower than 30 to keep GIFs small while staying smooth

# --- Catppuccin Mocha palette (matches the vhs tape theme) ---
BG = (30, 30, 46)
FG = (205, 214, 244)
BORDER = (49, 50, 68)
DIM = (108, 112, 134)

COLORS = {
    0: (30, 30, 46),
    1: (243, 139, 168),
    2: (166, 227, 161),
    3: (249, 226, 175),
    4: (137, 180, 250),
    5: (245, 194, 231),
    6: (148, 226, 213),
    7: (205, 214, 244),
    8: (108, 112, 134),
    9: (243, 139, 168),
    10: (166, 227, 161),
    11: (249, 226, 175),
    12: (137, 180, 250),
    13: (245, 194, 231),
    14: (148, 226, 213),
    15: (205, 214, 244),
}


def font(size=16):
    for p in [
        "/System/Library/Fonts/Menlo.ttc",
        "/System/Library/Fonts/Courier.dfont",
        "/Library/Fonts/Courier New.ttf",
    ]:
        if Path(p).exists():
            try:
                return ImageFont.truetype(p, size)
            except Exception:
                continue
    return ImageFont.load_default()


def screen_to_image(screen):
    W, H = WIDTH, HEIGHT
    pad = 16
    cell_w = (W - 2 * pad) / COLS
    cell_h = (H - 2 * pad) / ROWS
    # pick a font size that fills the cell
    img = Image.new("RGB", (W, H), BG)
    draw = ImageDraw.Draw(img)
    fnt = font(size=int(cell_h * 0.86))
    # draw border
    draw.rounded_rectangle([(pad - 4, pad - 4), (W - pad + 4, H - pad + 4)], radius=10, outline=BORDER, width=2)
    # Try to measure a cell; fallback to estimate
    try:
        cw, ch = draw.textbbox((0, 0), "M", font=fnt)[2:4]
        cw = max(cw, 1)
        ch = max(ch, 1)
    except Exception:
        cw, ch = int(cell_w), int(cell_h)
    # Slight centering
    ox, oy = pad, pad
    for y in range(ROWS):
        for x in range(COLS):
            ch_ = screen.buffer[y][x]
            c = ch_.data or " "
            if c == " " and ch_.fg == "default" and ch_.bg == "default":
                continue
            bg = COLORS.get(_color_index(ch_.bg, default=0), BG)
            fg = COLORS.get(_color_index(ch_.fg, default=7), FG)
            # background block already BG; only draw non-default bg
            if bg != BG:
                draw.rectangle(
                    [(ox + x * cell_w, oy + y * cell_h), (ox + (x + 1) * cell_w, oy + (y + 1) * cell_h)],
                    fill=bg,
                )
            # style: bold -> brighter
            style = FG if ch_.bold else fg
            # reverse
            if ch_.reverse:
                style, bg2 = bg, style
                draw.rectangle(
                    [(ox + x * cell_w, oy + y * cell_h), (ox + (x + 1) * cell_w, oy + (y + 1) * cell_h)],
                    fill=bg2,
                )
            draw.text((ox + x * cell_w, oy + y * cell_h), c, fill=style, font=fnt)
    return img


def _color_index(c, default=7):
    # pyte uses "default" or color names/numbers
    if c == "default":
        return default
    try:
        return int(c)
    except Exception:
        return default


def drive(keys, duration=32):
    """Spawn eddy-console in a pty, feed keys on schedule, sample screens at FPS."""
    master, slave = pty.openpty()
    # Large pty so the console doesn't truncate — winsize must be set or the
    # console falls back to 80x24 and pyte mis-wraps every line.
    try:
        import fcntl
        import struct
        import termios

        winsz = struct.pack("HHHH", ROWS, COLS, 0, 0)
        fcntl.ioctl(slave, termios.TIOCSWINSZ, winsz)
    except Exception:
        pass
    env = os.environ.copy()
    env.update({"TERM": "xterm-256color", "COLORTERM": "truecolor", "COLUMNS": str(COLS), "LINES": str(ROWS)})
    cmd = [str(BIN), "--replay", str(REPLAY)]
    proc = subprocess.Popen(cmd, stdin=slave, stdout=slave, stderr=slave, env=env, close_fds=True)
    os.close(slave)
    # Non-blocking
    try:
        import fcntl

        fl = fcntl.fcntl(master, fcntl.F_GETFL)
        fcntl.fcntl(master, fcntl.F_SETFL, fl | os.O_NONBLOCK)
    except Exception:
        pass

    screen = pyte.Screen(COLS, ROWS)
    stream = pyte.ByteStream(screen)
    images = []
    start = time.monotonic()
    next_key = 0
    # keys: list of (at_seconds, bytes_to_write)
    sample_interval = 1.0 / FPS
    next_sample = start + sample_interval
    buf = b""
    # Give the console a moment to start
    time.sleep(0.6)
    try:
        while time.monotonic() - start < duration:
            now = time.monotonic()
            if next_key < len(keys) and now - start >= keys[next_key][0]:
                os.write(master, keys[next_key][1])
                next_key += 1
            # drain pty
            try:
                while True:
                    r, _, _ = select.select([master], [], [], 0)
                    if not r:
                        break
                    data = os.read(master, 65536)
                    if not data:
                        break
                    buf += data
                    stream.feed(data)
            except OSError:
                pass
            if now >= next_sample:
                images.append(screen_to_image(screen))
                next_sample = now + sample_interval
            time.sleep(0.02)
            if proc.poll() is not None:
                break
    finally:
        try:
            proc.terminate()
            proc.wait(timeout=2)
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass
        try:
            os.close(master)
        except Exception:
            pass
    return images


def encode_gif(images, out):
    if not images:
        print(f"no frames for {out}", file=sys.stderr)
        sys.exit(1)
    # Keep a white-ish background consistent
    tmp = Path("/tmp") / f"eddy_{out.stem}_%05d.png"
    # Use ffmpeg palettegen for best quality (small, crisp GIF)
    png_dir = Path("/tmp") / f"eddy_{out.stem}_pngs"
    png_dir.mkdir(exist_ok=True)
    for i, im in enumerate(images):
        im.save(png_dir / f"{i:05d}.png")
    palette = Path("/tmp") / f"eddy_{out.stem}_palette.png"
    subprocess.check_call(
        [
            "ffmpeg",
            "-y",
            "-framerate",
            str(FPS),
            "-i",
            str(png_dir / "%05d.png"),
            "-vf",
            "palettegen=max_colors=256",
            str(palette),
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    subprocess.check_call(
        [
            "ffmpeg",
            "-y",
            "-framerate",
            str(FPS),
            "-i",
            str(png_dir / "%05d.png"),
            "-i",
            str(palette),
            "-lavfi",
            "paletteuse=dither=bayer:bayer_scale=3",
            "-gifflags",
            "+transdiff",
            str(out),
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    print(f"wrote {out} ({out.stat().st_size/1024:.0f} KiB, {len(images)} frames @ {FPS}fps)")
    # cleanup (keep demo.bin)
    import shutil

    shutil.rmtree(png_dir, ignore_errors=True)
    try:
        palette.unlink()
    except Exception:
        pass


def main():
    if not BIN.exists():
        subprocess.check_call(["cargo", "build", "-p", "eddy-console"], cwd=str(ROOT))
    if not REPLAY.exists():
        subprocess.check_call([sys.executable, str(ASSETS / "generate_demo.py")])

    # demo.gif — overview loop
    # Keys are timed to match the tape's story beats but via the pty
    demo_keys = [
        (2.5, b"2"),  # Workers
        (6.0, b"3"),  # Resources
        (9.0, b"4"),  # AsyncOps
        (12.0, b"1"),  # Tasks
        (13.5, b"b"),  # sort busy
        (16.5, b" "),  # pause
        (19.0, b"?"),  # help
        (22.5, b"?"),  # close help
        (24.0, b" "),  # unpause
        (26.0, b"q"),  # quit
    ]
    tui_keys = [
        (2.5, b"\x1b[B"),  # Down
        (3.0, b"\x1b[B"),  # Down
        (3.8, b"\r"),  # Enter
        (7.5, b"\x1b"),  # Esc
        (9.0, b"/echo\r"),  # filter (type /echo then Enter via stream)
        (12.5, b"\x1b"),  # clear filter
        (14.0, b"q"),
    ]
    print("capturing demo.gif ...")
    demo_imgs = drive(demo_keys, duration=28)
    encode_gif(demo_imgs, ASSETS / "demo.gif")
    print("capturing tui.gif ...")
    tui_imgs = drive(tui_keys, duration=16)
    encode_gif(tui_imgs, ASSETS / "tui.gif")


if __name__ == "__main__":
    main()
