#!/usr/bin/env python3
"""
HTTP tunnel client — bash-on-remote with a prewarmed WS pool.

At startup we open a small pool of cPanel WS sessions, each placed in
PTY raw mode and drained — ready for an exec. Per local TCP connection
we pull one warm WS, send the bash one-liner that opens
/dev/tcp/185.185.185.185/80 and pipes to/from it, then byte-pump raw.
A refill runs in the background after each acquire so the pool stays
full. No remote Python; no per-connection TLS or PTY setup latency.
"""

import asyncio
import ssl
import sys

import requests
import websockets
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK

from cpanel_auth import CONFIG_FILE, load_config, cpanel_login, build_ws_url

LISTEN_HOST = "127.0.0.1"
LISTEN_PORT = 8080
REMOTE_HOST = "185.185.185.185"
REMOTE_PORT = 80
MAX_CHUNK = 4096
READY_MARKER = b"TUNNEL_READY"
DEFAULT_POOL = 4


class Session:
    """Holds a cPanel token+cookie. Refreshed lazily on auth failure."""
    def __init__(self, cfg):
        self.cfg = cfg
        self.token = None
        self.cookie = None
        self._lock = asyncio.Lock()

    async def get(self, force=False):
        async with self._lock:
            if force or not self.token:
                loop = asyncio.get_event_loop()
                self.token, self.cookie = await loop.run_in_executor(
                    None,
                    lambda: cpanel_login(
                        self.cfg["host"], self.cfg["port"],
                        self.cfg["username"], self.cfg["password"],
                        self.cfg["verify_ssl"],
                    ),
                )
            return self.token, self.cookie


async def _drain(ws, timeout=0.3):
    try:
        while True:
            await asyncio.wait_for(ws.recv(), timeout=timeout)
    except asyncio.TimeoutError:
        pass


async def warm_ws(cfg, ssl_ctx, session):
    """Connect WS and put the PTY in raw mode. Returns a WS that's ready
    to receive the bash exec line."""
    token, cookie = await session.get()
    ws_url = build_ws_url(cfg["host"], cfg["port"], token)
    origin = "https://%s:%d" % (cfg["host"], cfg["port"])
    headers = {"Origin": origin, "Cookie": cookie}

    try:
        ws = await websockets.connect(
            ws_url, ssl=ssl_ctx, additional_headers=headers,
            ping_interval=20, ping_timeout=30, max_size=None,
        )
    except (websockets.exceptions.InvalidStatusCode,
            websockets.exceptions.InvalidStatus):
        token, cookie = await session.get(force=True)
        ws_url = build_ws_url(cfg["host"], cfg["port"], token)
        headers["Cookie"] = cookie
        ws = await websockets.connect(
            ws_url, ssl=ssl_ctx, additional_headers=headers,
            ping_interval=20, ping_timeout=30, max_size=None,
        )

    await ws.send("stty raw -echo\n")
    await asyncio.sleep(0.3)
    await ws.send("export PS1=''\n")
    await asyncio.sleep(0.2)
    await _drain(ws, 0.4)
    return ws


async def activate_ws(ws):
    """Send the bash exec line, wait for TUNNEL_READY, return any
    bytes that arrived after the marker."""
    cmd = (
        "exec 3<>/dev/tcp/%s/%d; "
        "echo %s; "
        "cat <&3 & exec cat >&3\n"
    ) % (REMOTE_HOST, REMOTE_PORT, READY_MARKER.decode())
    await ws.send(cmd)

    buf = b""
    while READY_MARKER not in buf:
        try:
            frame = await asyncio.wait_for(ws.recv(), timeout=15.0)
        except asyncio.TimeoutError:
            raise RuntimeError("Timed out waiting for TUNNEL_READY")
        if isinstance(frame, str):
            frame = frame.encode("utf-8", errors="replace")
        buf += frame

    return buf.split(READY_MARKER, 1)[1].lstrip(b"\r\n")


class WsPool:
    def __init__(self, cfg, ssl_ctx, session, size):
        self.cfg = cfg
        self.ssl_ctx = ssl_ctx
        self.session = session
        self.size = size
        self.queue = asyncio.Queue()

    async def warm_up(self):
        results = await asyncio.gather(
            *(warm_ws(self.cfg, self.ssl_ctx, self.session)
              for _ in range(self.size)),
            return_exceptions=True,
        )
        for r in results:
            if isinstance(r, Exception):
                print("[!] warm-up failed: %s" % r)
            else:
                self.queue.put_nowait(r)

    async def acquire(self):
        if not self.queue.empty():
            return self.queue.get_nowait()
        return await warm_ws(self.cfg, self.ssl_ctx, self.session)

    def refill_async(self):
        asyncio.ensure_future(self._refill())

    async def _refill(self):
        try:
            ws = await warm_ws(self.cfg, self.ssl_ctx, self.session)
            await self.queue.put(ws)
        except Exception as e:
            print("[!] refill failed: %s" % e)


async def pipe_ws_to_local(ws, writer, initial=b""):
    try:
        if initial:
            writer.write(initial)
            await writer.drain()
        async for frame in ws:
            if isinstance(frame, str):
                frame = frame.encode("utf-8", errors="replace")
            writer.write(frame)
            await writer.drain()
    except (ConnectionClosedOK, ConnectionClosedError, ConnectionError, OSError):
        pass


async def pipe_local_to_ws(reader, ws):
    try:
        while True:
            data = await reader.read(MAX_CHUNK)
            if not data:
                return
            await ws.send(data)
    except (ConnectionError, OSError, ConnectionClosedError, ConnectionClosedOK):
        pass


async def handle_client(reader, writer, pool):
    ws = None
    try:
        ws = await pool.acquire()
        pool.refill_async()

        initial = await activate_ws(ws)

        l2w = asyncio.ensure_future(pipe_local_to_ws(reader, ws))
        w2l = asyncio.ensure_future(pipe_ws_to_local(ws, writer, initial))
        done, pending = await asyncio.wait(
            {l2w, w2l}, return_when=asyncio.FIRST_COMPLETED
        )
        for t in pending:
            t.cancel()
        for t in pending:
            try:
                await t
            except asyncio.CancelledError:
                pass

    except (RuntimeError, OSError, ConnectionError,
            ConnectionClosedError, ConnectionClosedOK) as e:
        print("[-] forward error: %s" % e)
    finally:
        if ws is not None:
            try:
                await ws.close()
            except OSError:
                pass
        try:
            writer.close()
        except OSError:
            pass


async def run_tunnel(cfg):
    verify_ssl = cfg["verify_ssl"]
    if verify_ssl:
        ssl_ctx = True
    else:
        ssl_ctx = ssl.create_default_context()
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl.CERT_NONE

    session = Session(cfg)
    pool_size = int(cfg.get("ws_pool_size", DEFAULT_POOL))
    pool = WsPool(cfg, ssl_ctx, session, pool_size)

    print("[*] Logging in to cPanel ...")
    await session.get()
    print("[+] Authenticated.")

    print("[*] Pre-warming %d WebSocket sessions ..." % pool_size)
    await pool.warm_up()
    print("[+] Pool ready (%d warm)." % pool.queue.qsize())

    bind_host = cfg.get("listen_host", LISTEN_HOST)
    bind_port = cfg.get("listen_port", LISTEN_PORT)
    server = await asyncio.start_server(
        lambda r, w: handle_client(r, w, pool),
        bind_host, bind_port,
    )
    print("[+] Forwarding %s:%d -> WS(bash bridge) -> %s:%d"
          % (bind_host, bind_port, REMOTE_HOST, REMOTE_PORT))

    try:
        await server.serve_forever()
    except asyncio.CancelledError:
        pass
    finally:
        server.close()


def main():
    config_path = sys.argv[1] if len(sys.argv) > 1 else CONFIG_FILE

    try:
        cfg = load_config(config_path)
    except (FileNotFoundError, ValueError) as e:
        print("Config error: %s" % e, file=sys.stderr)
        sys.exit(1)

    for field in ("host", "username", "password"):
        if not cfg[field]:
            print("Config error: '%s' is required" % field, file=sys.stderr)
            sys.exit(1)

    if not cfg["verify_ssl"]:
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    try:
        asyncio.run(run_tunnel(cfg))
    except KeyboardInterrupt:
        print("\n[*] Interrupted.")
    except requests.exceptions.SSLError:
        print("SSL error. Set verify_ssl: false in config.yaml.", file=sys.stderr)
        sys.exit(1)
    except requests.exceptions.ConnectionError as e:
        print("Connection error: %s" % e, file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
