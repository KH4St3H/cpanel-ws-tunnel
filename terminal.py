#!/usr/bin/env python3
"""
cPanel WebSocket Terminal Client
Authenticates with cPanel and connects to its built-in WebSocket terminal.
"""

import asyncio
import json
import shutil
import signal
import sys
import termios
import tty

import requests
import websockets
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK

from cpanel_auth import CONFIG_FILE, load_config, cpanel_login, build_ws_url
from proxy_util import create_proxy_connection


# ---------------------------------------------------------------------------
# Async WebSocket terminal
# ---------------------------------------------------------------------------

def terminal_size():
    s = shutil.get_terminal_size((80, 24))
    return s.columns, s.lines


async def ws_terminal(host: str, port: int, token: str, cookie: str, verify_ssl: bool, proxy: str = None) -> None:
    ssl_ctx: bool | object = True
    if not verify_ssl:
        import ssl
        ssl_ctx = ssl.create_default_context()
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl.CERT_NONE

    cols, rows = terminal_size()
    ws_url = build_ws_url(host, port, token, cols, rows)
    origin = f"https://{host}:{port}"

    print(f"Connecting to terminal: {ws_url}\n")

    ws_kwargs = dict(
        ssl=ssl_ctx,
        additional_headers={"Origin": origin, "Cookie": cookie},
        ping_interval=20,
        ping_timeout=30,
    )
    sock = await create_proxy_connection(proxy, host, port)
    if sock:
        ws_kwargs["sock"] = sock
        ws_kwargs["server_hostname"] = host

    async with websockets.connect(ws_url, **ws_kwargs) as ws:
        loop = asyncio.get_event_loop()

        # Put stdin in raw mode
        old_tty = termios.tcgetattr(sys.stdin)
        tty.setraw(sys.stdin.fileno())

        def restore_tty():
            termios.tcsetattr(sys.stdin, termios.TCSADRAIN, old_tty)

        def on_resize(*_):
            c, r = terminal_size()
            # cPanel Shell websocket accepts resize as a JSON control message
            asyncio.ensure_future(
                ws.send(json.dumps({"resize": [c, r]}))
            )

        signal.signal(signal.SIGWINCH, on_resize)

        async def read_stdin():
            """Forward local keystrokes → server as raw text."""
            while True:
                data = await loop.run_in_executor(None, sys.stdin.buffer.read1, 256)
                if not data:
                    break
                await ws.send(data.decode("utf-8", errors="replace"))

        async def read_ws():
            """Forward server output → local stdout (raw PTY stream)."""
            async for frame in ws:
                if isinstance(frame, bytes):
                    sys.stdout.buffer.write(frame)
                else:
                    sys.stdout.write(frame)
                sys.stdout.flush()

        try:
            await asyncio.gather(read_stdin(), read_ws())
        except (ConnectionClosedOK, ConnectionClosedError):
            pass
        finally:
            restore_tty()
            signal.signal(signal.SIGWINCH, signal.SIG_DFL)
            print("\r\nConnection closed.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    config_path = sys.argv[1] if len(sys.argv) > 1 else CONFIG_FILE

    try:
        cfg = load_config(config_path)
    except (FileNotFoundError, ValueError) as exc:
        print(f"Config error: {exc}", file=sys.stderr)
        sys.exit(1)

    for field in ("host", "username", "password"):
        if not cfg[field]:
            print(f"Config error: '{field}' is required in [cpanel]", file=sys.stderr)
            sys.exit(1)

    if not cfg["verify_ssl"]:
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        print("Warning: SSL verification disabled.")

    print(f"Logging in to https://{cfg['host']}:{cfg['port']} as {cfg['username']} ...")
    try:
        token, cookie = cpanel_login(
            cfg["host"], cfg["port"], cfg["username"], cfg["password"], cfg["verify_ssl"],
            cfg.get("proxy")
        )
    except requests.exceptions.SSLError:
        print(
            "SSL error. If using a self-signed certificate, set verify_ssl = false in config.ini",
            file=sys.stderr,
        )
        sys.exit(1)
    except requests.exceptions.ConnectionError as exc:
        print(f"Connection error: {exc}", file=sys.stderr)
        sys.exit(1)
    except requests.exceptions.HTTPError as exc:
        print(f"HTTP error during login: {exc}", file=sys.stderr)
        sys.exit(1)
    except RuntimeError as exc:
        print(f"Login error: {exc}", file=sys.stderr)
        sys.exit(1)

    print(f"Authenticated. Session token: cpsess{token[:8]}...")

    try:
        asyncio.run(ws_terminal(cfg["host"], cfg["port"], token, cookie, cfg["verify_ssl"], cfg.get("proxy")))
    except KeyboardInterrupt:
        print("\r\nInterrupted.")


if __name__ == "__main__":
    main()
