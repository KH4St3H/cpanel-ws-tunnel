#!/usr/bin/env python3
"""
HTTP tunnel client — bash-on-remote with a prewarmed WS pool.

At startup we open a small pool of cPanel WS sessions, each placed in
PTY raw mode and drained — ready for an exec. Per local TCP connection
we pull one warm WS, send the bash one-liner that opens
/dev/tcp/185.185.185.185/80 and pipes to/from it, then byte-pump raw.
A refill runs in the background after each acquire so the pool stays
full. No remote Python; no per-connection TLS or PTY setup latency.

Pass -v / --verbose for per-connection debug logging.
"""

import argparse
import asyncio
import itertools
import logging
import ssl
import sys
import time

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

log = logging.getLogger("http_tunnel")
_conn_seq = itertools.count(1)


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
                log.debug("cpanel login (force=%s)", force)
                loop = asyncio.get_event_loop()
                self.token, self.cookie = await loop.run_in_executor(
                    None,
                    lambda: cpanel_login(
                        self.cfg["host"], self.cfg["port"],
                        self.cfg["username"], self.cfg["password"],
                        self.cfg["verify_ssl"],
                    ),
                )
                log.debug("cpanel token cpsess%s...", self.token[:8])
            return self.token, self.cookie


async def _drain(ws, timeout=0.3):
    n = 0
    try:
        while True:
            frame = await asyncio.wait_for(ws.recv(), timeout=timeout)
            n += len(frame)
    except asyncio.TimeoutError:
        pass
    if n:
        log.debug("drained %d bytes from PTY", n)


async def warm_ws(cfg, ssl_ctx, session, tag=""):
    """Connect WS and put the PTY in raw mode. Returns a WS that's ready
    to receive the bash exec line."""
    t0 = time.monotonic()
    token, cookie = await session.get()
    ws_url = build_ws_url(cfg["host"], cfg["port"], token)
    origin = "https://%s:%d" % (cfg["host"], cfg["port"])
    headers = {"Origin": origin, "Cookie": cookie}

    log.debug("%sopening ws", tag)
    try:
        ws = await websockets.connect(
            ws_url, ssl=ssl_ctx, additional_headers=headers,
            ping_interval=20, ping_timeout=30, max_size=None,
        )
    except (websockets.exceptions.InvalidStatusCode,
            websockets.exceptions.InvalidStatus) as e:
        log.warning("%sws auth rejected (%s); refreshing token", tag, e)
        token, cookie = await session.get(force=True)
        ws_url = build_ws_url(cfg["host"], cfg["port"], token)
        headers["Cookie"] = cookie
        ws = await websockets.connect(
            ws_url, ssl=ssl_ctx, additional_headers=headers,
            ping_interval=20, ping_timeout=30, max_size=None,
        )
    log.debug("%sws connected in %.0fms", tag, (time.monotonic() - t0) * 1000)

    await ws.send("stty raw -echo\n")
    await asyncio.sleep(0.3)
    await ws.send("export PS1=''\n")
    await asyncio.sleep(0.2)
    await _drain(ws, 0.4)
    log.debug("%sws warm in %.0fms", tag, (time.monotonic() - t0) * 1000)
    return ws


async def activate_ws(ws, tag=""):
    """Send the bash exec line, wait for TUNNEL_READY, return any
    bytes that arrived after the marker."""
    t0 = time.monotonic()
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
            log.error("%stimeout waiting for TUNNEL_READY (got %d bytes: %r)",
                      tag, len(buf), buf[:120])
            raise RuntimeError("Timed out waiting for TUNNEL_READY")
        if isinstance(frame, str):
            frame = frame.encode("utf-8", errors="replace")
        buf += frame

    after = buf.split(READY_MARKER, 1)[1].lstrip(b"\r\n")
    log.debug("%sactivated in %.0fms (initial=%d bytes)",
              tag, (time.monotonic() - t0) * 1000, len(after))
    return after


class WsPool:
    def __init__(self, cfg, ssl_ctx, session, size):
        self.cfg = cfg
        self.ssl_ctx = ssl_ctx
        self.session = session
        self.size = size
        self.queue = asyncio.Queue()
        self._refills_in_flight = 0

    async def warm_up(self):
        results = await asyncio.gather(
            *(warm_ws(self.cfg, self.ssl_ctx, self.session,
                      tag="[warm %d/%d] " % (i + 1, self.size))
              for i in range(self.size)),
            return_exceptions=True,
        )
        for r in results:
            if isinstance(r, Exception):
                log.error("warm-up failed: %s", r)
            else:
                self.queue.put_nowait(r)

    async def acquire(self, tag=""):
        if not self.queue.empty():
            ws = self.queue.get_nowait()
            log.debug("%sacquired warm ws (pool=%d)", tag, self.queue.qsize())
            return ws
        log.debug("%spool empty; warming on demand", tag)
        return await warm_ws(self.cfg, self.ssl_ctx, self.session, tag=tag)

    def refill_async(self):
        self._refills_in_flight += 1
        asyncio.ensure_future(self._refill())

    async def _refill(self):
        try:
            ws = await warm_ws(self.cfg, self.ssl_ctx, self.session,
                               tag="[refill] ")
            await self.queue.put(ws)
            log.debug("[refill] pool=%d", self.queue.qsize())
        except Exception as e:
            log.error("refill failed: %s", e)
        finally:
            self._refills_in_flight -= 1


async def pipe_ws_to_local(ws, writer, initial=b"", tag=""):
    n = 0
    try:
        if initial:
            writer.write(initial)
            await writer.drain()
            n += len(initial)
        async for frame in ws:
            if isinstance(frame, str):
                frame = frame.encode("utf-8", errors="replace")
            writer.write(frame)
            await writer.drain()
            n += len(frame)
    except (ConnectionClosedOK, ConnectionClosedError, ConnectionError, OSError) as e:
        log.debug("%sws->local ended (%s) after %d bytes", tag, type(e).__name__, n)
    return n


async def pipe_local_to_ws(reader, ws, tag=""):
    n = 0
    try:
        while True:
            data = await reader.read(MAX_CHUNK)
            if not data:
                return n
            await ws.send(data)
            n += len(data)
    except (ConnectionError, OSError, ConnectionClosedError, ConnectionClosedOK) as e:
        log.debug("%slocal->ws ended (%s) after %d bytes", tag, type(e).__name__, n)
    return n


async def handle_client(reader, writer, pool):
    cid = next(_conn_seq)
    peer = writer.get_extra_info("peername") or ("?", 0)
    tag = "[c%d] " % cid
    log.info("%saccept from %s:%s", tag, peer[0], peer[1])
    t0 = time.monotonic()
    ws = None
    try:
        ws = await pool.acquire(tag=tag)
        pool.refill_async()

        initial = await activate_ws(ws, tag=tag)

        l2w = asyncio.ensure_future(pipe_local_to_ws(reader, ws, tag=tag))
        w2l = asyncio.ensure_future(pipe_ws_to_local(ws, writer, initial, tag=tag))
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

        sent = l2w.result() if l2w in done and not l2w.cancelled() else 0
        recv = w2l.result() if w2l in done and not w2l.cancelled() else 0
        log.info("%sclose after %.0fms  out=%d  in=%d",
                 tag, (time.monotonic() - t0) * 1000, sent, recv)

    except (RuntimeError, OSError, ConnectionError,
            ConnectionClosedError, ConnectionClosedOK) as e:
        log.error("%sforward error: %s", tag, e)
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

    log.info("logging in to cpanel %s:%d as %s",
             cfg["host"], cfg["port"], cfg["username"])
    await session.get()
    log.info("authenticated")

    log.info("pre-warming %d ws sessions", pool_size)
    t0 = time.monotonic()
    await pool.warm_up()
    log.info("pool ready (%d warm, %.0fms)",
             pool.queue.qsize(), (time.monotonic() - t0) * 1000)

    bind_host = cfg.get("listen_host", LISTEN_HOST)
    bind_port = cfg.get("listen_port", LISTEN_PORT)
    server = await asyncio.start_server(
        lambda r, w: handle_client(r, w, pool),
        bind_host, bind_port,
    )
    log.info("forwarding %s:%d -> ws(bash bridge) -> %s:%d",
             bind_host, bind_port, REMOTE_HOST, REMOTE_PORT)

    try:
        await server.serve_forever()
    except asyncio.CancelledError:
        pass
    finally:
        server.close()


def setup_logging(verbose):
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname).1s %(message)s",
        datefmt="%H:%M:%S",
    )
    if not verbose:
        # quiet noisy libraries unless explicitly requested
        logging.getLogger("websockets").setLevel(logging.WARNING)


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    ap.add_argument("config", nargs="?", default=CONFIG_FILE,
                    help="path to config.yaml (default: %(default)s)")
    ap.add_argument("-v", "--verbose", action="store_true",
                    help="enable per-connection debug logging")
    args = ap.parse_args()

    setup_logging(args.verbose)

    try:
        cfg = load_config(args.config)
    except (FileNotFoundError, ValueError) as e:
        log.error("config error: %s", e)
        sys.exit(1)

    for field in ("host", "username", "password"):
        if not cfg[field]:
            log.error("config error: '%s' is required", field)
            sys.exit(1)

    if not cfg["verify_ssl"]:
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    try:
        asyncio.run(run_tunnel(cfg))
    except KeyboardInterrupt:
        log.info("interrupted")
    except requests.exceptions.SSLError:
        log.error("ssl error. Set verify_ssl: false in config.yaml.")
        sys.exit(1)
    except requests.exceptions.ConnectionError as e:
        log.error("connection error: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
