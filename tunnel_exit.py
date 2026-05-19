#!/usr/bin/env python3
"""
Tunnel Exit (Client B) — TCP forwarder exit side.
Connects to cPanel (where the relay FIFOs are set up by tunnel_server.py),
then for every N frame opens a TCP connection to the configured upstream
host and relays bytes.
"""

import asyncio
import base64
import ssl
import sys

import requests
import websockets
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK

from cpanel_auth import CONFIG_FILE, load_config, cpanel_login, build_ws_url
from proxy_util import create_proxy_connection

MAX_CHUNK = 2048
DEBUG = False


def dbg(msg):
    if DEBUG:
        print("[dbg] %s" % msg)


def encode_frame(ftype, conn_id, payload=b""):
    return "%s:%d:%s\n" % (ftype, conn_id, base64.b64encode(payload).decode("ascii"))


def decode_frame(line):
    parts = line.strip().split(":", 2)
    if len(parts) < 3:
        return None, 0, b""
    ftype = parts[0]
    try:
        conn_id = int(parts[1])
    except ValueError:
        return None, 0, b""
    payload = base64.b64decode(parts[2]) if parts[2] else b""
    return ftype, conn_id, payload


# ---------------------------------------------------------------------------
# Deployer
# ---------------------------------------------------------------------------

async def deploy_bridge(ws):
    async def send_cmd(cmd):
        await ws.send(cmd + "\n")

    async def drain(timeout=0.5, silent=True):
        collected = ""
        try:
            while True:
                frame = await asyncio.wait_for(ws.recv(), timeout=timeout)
                if isinstance(frame, bytes):
                    frame = frame.decode("utf-8", errors="replace")
                collected += frame
        except asyncio.TimeoutError:
            pass
        if not silent and collected.strip():
            print("[debug] %s" % collected.strip()[:200])
        return collected

    await send_cmd("HISTFILE=/dev/null; history -c")
    await asyncio.sleep(0.2)
    await send_cmd("stty -echo")
    await asyncio.sleep(0.3)
    await send_cmd("stty -onlcr")
    await asyncio.sleep(0.2)
    await send_cmd("export PS1=''")
    await asyncio.sleep(0.2)
    await drain(0.5)

    await send_cmd("mkdir -p $HOME/.cache/.fontconfig")
    await asyncio.sleep(0.2)
    await send_cmd("mkfifo $HOME/.cache/.fontconfig/.uuid-r 2>/dev/null; mkfifo $HOME/.cache/.fontconfig/.uuid-w 2>/dev/null; true")
    await asyncio.sleep(0.3)
    await drain(0.3, silent=False)

    await send_cmd("exec -a 'bash' cat $HOME/.cache/.fontconfig/.uuid-w & exec -a 'bash' cat > $HOME/.cache/.fontconfig/.uuid-r")

    buf = ""
    while True:
        try:
            frame = await asyncio.wait_for(ws.recv(), timeout=60.0)
        except asyncio.TimeoutError:
            raise RuntimeError("Timed out waiting for TUNNEL_READY from Client A")
        if isinstance(frame, bytes):
            frame = frame.decode("utf-8", errors="replace")
        buf += frame
        if "TUNNEL_READY" in buf:
            await ws.send("TUNNEL_READY\n")
            print("[+] Tunnel established.")
            return


# ---------------------------------------------------------------------------
# TCP connection manager
# ---------------------------------------------------------------------------

class ExitNode:
    def __init__(self, ws, upstream_host, upstream_port):
        self.ws = ws
        self.upstream_host = upstream_host
        self.upstream_port = upstream_port
        self._send_lock = asyncio.Lock()
        self._conns = {}        # conn_id -> (reader, writer)
        self._read_tasks = {}   # conn_id -> task

    async def send_frame(self, ftype, conn_id, payload=b""):
        data = encode_frame(ftype, conn_id, payload)
        async with self._send_lock:
            await self.ws.send(data)

    async def handle_new(self, conn_id, payload):
        dbg("N conn_id=%d -> %s:%d" % (conn_id, self.upstream_host, self.upstream_port))
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(self.upstream_host, self.upstream_port),
                timeout=10.0,
            )
        except Exception as e:
            msg = "ERR:%s" % str(e)[:100]
            dbg("conn_id=%d connect failed: %s" % (conn_id, msg))
            await self.send_frame("A", conn_id, msg.encode("utf-8"))
            return

        self._conns[conn_id] = (reader, writer)
        await self.send_frame("A", conn_id, b"OK")

        task = asyncio.ensure_future(self._tcp_reader(conn_id, reader))
        self._read_tasks[conn_id] = task

    async def _tcp_reader(self, conn_id, reader):
        try:
            while True:
                data = await reader.read(MAX_CHUNK)
                if not data:
                    await self.send_frame("C", conn_id)
                    break
                for i in range(0, len(data), MAX_CHUNK):
                    await self.send_frame("D", conn_id, data[i:i + MAX_CHUNK])
        except (ConnectionError, OSError):
            try:
                await self.send_frame("C", conn_id)
            except Exception:
                pass
        finally:
            self._close_conn(conn_id)

    async def handle_data(self, conn_id, payload):
        pair = self._conns.get(conn_id)
        if pair is None:
            return
        _, writer = pair
        try:
            writer.write(payload)
            await writer.drain()
        except (ConnectionError, OSError):
            try:
                await self.send_frame("C", conn_id)
            except Exception:
                pass
            self._close_conn(conn_id)

    def handle_close(self, conn_id):
        self._close_conn(conn_id)

    def _close_conn(self, conn_id):
        pair = self._conns.pop(conn_id, None)
        if pair:
            _, writer = pair
            try:
                writer.close()
            except OSError:
                pass
        task = self._read_tasks.pop(conn_id, None)
        if task and not task.done():
            task.cancel()

    async def cleanup(self):
        for cid in list(self._conns):
            self._close_conn(cid)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

async def run_exit_once(cfg, ssl_ctx, on_established=None):
    verify_ssl = cfg["verify_ssl"]
    proxy = cfg.get("proxy")

    print("[*] Logging in to cPanel ...")
    token, cookie = cpanel_login(
        cfg["host"], cfg["port"], cfg["username"], cfg["password"], verify_ssl, proxy
    )
    print("[+] Authenticated. Token: cpsess%s..." % token[:8])

    ws_url = build_ws_url(cfg["host"], cfg["port"], token)
    origin = "https://%s:%d" % (cfg["host"], cfg["port"])

    print("[*] Connecting WebSocket ...")
    ws_kwargs = dict(
        ssl=ssl_ctx,
        additional_headers={"Origin": origin, "Cookie": cookie},
        ping_interval=20,
        ping_timeout=30,
    )
    sock = await create_proxy_connection(proxy, cfg["host"], cfg["port"])
    if sock:
        ws_kwargs["sock"] = sock
        ws_kwargs["server_hostname"] = cfg["host"]

    async with websockets.connect(ws_url, **ws_kwargs) as ws:
        print("[*] Setting up FIFOs ...")
        await deploy_bridge(ws)

        node = ExitNode(ws, cfg["upstream_host"], cfg["upstream_port"])

        async def keepalive():
            try:
                while True:
                    await asyncio.sleep(15)
                    await node.send_frame("P", 0)
            except (asyncio.CancelledError, ConnectionClosedError, ConnectionClosedOK):
                pass

        ka_task = asyncio.ensure_future(keepalive())

        print("[+] Exit node active. Forwarding to %s:%d"
              % (cfg["upstream_host"], cfg["upstream_port"]))
        if on_established is not None:
            on_established()

        buf = ""
        try:
            async for raw in ws:
                if isinstance(raw, bytes):
                    raw = raw.decode("utf-8", errors="replace")
                buf += raw
                while "\n" in buf:
                    line, buf = buf.split("\n", 1)
                    if not line.strip():
                        continue
                    ftype, cid, payload = decode_frame(line)
                    if ftype is None:
                        continue

                    if ftype == "N":
                        await node.handle_new(cid, payload)
                    elif ftype == "D":
                        await node.handle_data(cid, payload)
                    elif ftype == "C":
                        node.handle_close(cid)
                    elif ftype == "P":
                        pass
        finally:
            ka_task.cancel()
            await node.cleanup()


async def run_exit(cfg):
    verify_ssl = cfg["verify_ssl"]
    ssl_ctx = True
    if not verify_ssl:
        ssl_ctx = ssl.create_default_context()
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl.CERT_NONE

    retry = 0

    def session_established():
        nonlocal retry
        retry = 0

    while True:
        try:
            await run_exit_once(cfg, ssl_ctx, on_established=session_established)
            break
        except KeyboardInterrupt:
            raise
        except (ConnectionClosedError, ConnectionClosedOK, websockets.exceptions.InvalidStatusCode,
                ConnectionRefusedError, OSError, RuntimeError) as e:
            retry += 1
            wait = min(retry * 2, 30)
            print("[!] Disconnected: %s" % e)
            print("[*] Reconnecting in %ds (attempt %d) ..." % (wait, retry))
            await asyncio.sleep(wait)
        except requests.exceptions.RequestException as e:
            retry += 1
            wait = min(retry * 2, 30)
            print("[!] Auth error: %s" % e)
            print("[*] Retrying in %ds (attempt %d) ..." % (wait, retry))
            await asyncio.sleep(wait)


def main():
    global DEBUG
    args = [a for a in sys.argv[1:] if not a.startswith("-")]
    config_path = args[0] if args else CONFIG_FILE
    DEBUG = "--debug" in sys.argv or "-d" in sys.argv

    try:
        cfg = load_config(config_path)
    except (FileNotFoundError, ValueError) as e:
        print("Config error: %s" % e, file=sys.stderr)
        sys.exit(1)

    for field in ("host", "username", "password"):
        if not cfg[field]:
            print("Config error: '%s' is required" % field, file=sys.stderr)
            sys.exit(1)
    if not cfg["upstream_host"] or not cfg["upstream_port"]:
        print("Config error: 'upstream_host' and 'upstream_port' are required",
              file=sys.stderr)
        sys.exit(1)

    if not cfg["verify_ssl"]:
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    try:
        asyncio.run(run_exit(cfg))
    except KeyboardInterrupt:
        print("\n[*] Interrupted.")


if __name__ == "__main__":
    main()
