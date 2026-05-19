#!/usr/bin/env python3
"""
Tunnel Server (Client A) — TCP forwarder side.
Connects to cPanel, establishes the FIFO bridge, then accepts local TCP
connections on listen_host:listen_port and pipes each one through the
WebSocket tunnel. The exit node (tunnel_exit.py) forwards to its configured
upstream host.
"""

import asyncio
import base64
import ssl
import sys
import traceback

import requests
import websockets
from websockets.exceptions import (
    ConnectionClosed,
    ConnectionClosedError,
    ConnectionClosedOK,
)

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
# Deployer — sets up the FIFO bridge on cPanel
# ---------------------------------------------------------------------------

async def deploy_remote(ws):
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

    await send_cmd("exec -a 'bash' cat $HOME/.cache/.fontconfig/.uuid-r & exec -a 'bash' cat > $HOME/.cache/.fontconfig/.uuid-w")
    print("[*] Waiting for exit node (Client B) to connect ...")

    buf = ""
    while True:
        try:
            await ws.send("TUNNEL_READY\n")
        except (ConnectionClosedError, ConnectionClosedOK):
            raise RuntimeError("WebSocket closed while waiting for Client B")

        try:
            frame = await asyncio.wait_for(ws.recv(), timeout=3.0)
            if isinstance(frame, bytes):
                frame = frame.decode("utf-8", errors="replace")
            buf += frame
            if "TUNNEL_READY" in buf:
                print("[+] Tunnel established.")
                return
        except asyncio.TimeoutError:
            continue


# ---------------------------------------------------------------------------
# WebSocket multiplexer
# ---------------------------------------------------------------------------

class WsMux:
    def __init__(self, ws):
        self.ws = ws
        self._queues = {}
        self._send_lock = asyncio.Lock()
        self._reader_task = None

    async def start(self):
        self._reader_task = asyncio.ensure_future(self._reader_loop())

    async def stop(self):
        if self._reader_task:
            self._reader_task.cancel()
            try:
                await self._reader_task
            except asyncio.CancelledError:
                pass

    def register(self, conn_id):
        q = asyncio.Queue()
        self._queues[conn_id] = q
        return q

    def unregister(self, conn_id):
        self._queues.pop(conn_id, None)

    async def send_frame(self, ftype, conn_id, payload=b""):
        data = encode_frame(ftype, conn_id, payload)
        async with self._send_lock:
            await self.ws.send(data)

    async def _reader_loop(self):
        buf = ""
        try:
            try:
                async for raw in self.ws:
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
                        q = self._queues.get(cid)
                        if q is not None:
                            await q.put((ftype, payload))
            except (ConnectionClosedOK, ConnectionClosedError, ConnectionClosed):
                pass
            except asyncio.CancelledError:
                raise
            except Exception:
                if DEBUG:
                    traceback.print_exc()
        finally:
            for q in list(self._queues.values()):
                try:
                    await q.put((None, b""))
                except Exception:
                    pass


# ---------------------------------------------------------------------------
# Per-connection relay
# ---------------------------------------------------------------------------

_next_conn_id = 0
_conn_id_lock = asyncio.Lock()


async def get_conn_id():
    global _next_conn_id
    async with _conn_id_lock:
        _next_conn_id += 1
        if _next_conn_id > 65535:
            _next_conn_id = 1
        return _next_conn_id


async def _relay_data(reader, writer, mux, q, conn_id):
    async def local_to_remote():
        try:
            while True:
                data = await reader.read(MAX_CHUNK)
                if not data:
                    try:
                        await mux.send_frame("C", conn_id)
                    except Exception:
                        pass
                    break
                for i in range(0, len(data), MAX_CHUNK):
                    try:
                        await mux.send_frame("D", conn_id, data[i:i + MAX_CHUNK])
                    except ConnectionClosed:
                        break
        except asyncio.CancelledError:
            raise
        except (ConnectionError, OSError):
            try:
                await mux.send_frame("C", conn_id)
            except Exception:
                pass

    async def remote_to_local():
        try:
            while True:
                ft, pl = await q.get()
                if ft == "D":
                    writer.write(pl)
                    await writer.drain()
                elif ft == "C" or ft is None:
                    break
        except asyncio.CancelledError:
            raise
        except (ConnectionError, OSError):
            pass

    tasks = [asyncio.ensure_future(local_to_remote()),
             asyncio.ensure_future(remote_to_local())]
    try:
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for t in pending:
            t.cancel()
        for t in done:
            exc = t.exception()
            if exc and not isinstance(exc, asyncio.CancelledError):
                dbg("relay exception conn_id=%d: %s" % (conn_id, exc))
    except asyncio.CancelledError:
        for t in tasks:
            t.cancel()
        raise


async def handle_client(reader, writer, mux):
    conn_id = None
    peer = writer.get_extra_info("peername")
    dbg("new connection from %s" % (peer,))
    try:
        conn_id = await get_conn_id()
        q = mux.register(conn_id)

        # Empty target — exit node uses its configured upstream.
        await mux.send_frame("N", conn_id, b"")

        ftype, payload = await asyncio.wait_for(q.get(), timeout=15.0)
        if ftype != "A" or not payload.startswith(b"OK"):
            dbg("conn_id=%s ack failed: %s" % (conn_id, payload[:80]))
            return

        await _relay_data(reader, writer, mux, q, conn_id)

    except asyncio.CancelledError:
        dbg("conn_id=%s cancelled" % conn_id)
    except (asyncio.TimeoutError, ConnectionError,
            asyncio.IncompleteReadError, OSError) as e:
        dbg("forward error (conn_id=%s): %s" % (conn_id, e))
    except ConnectionClosed as e:
        print("[-] tunnel WebSocket closed (conn_id=%s): %s" % (conn_id, e))
    except Exception as e:
        print("[-] unexpected error (conn_id=%s): %s" % (conn_id, e))
        if DEBUG:
            traceback.print_exc()
    finally:
        if conn_id is not None:
            mux.unregister(conn_id)
            dbg("conn_id=%d closed" % conn_id)
        try:
            writer.close()
        except OSError:
            pass


# ---------------------------------------------------------------------------
# Keepalive
# ---------------------------------------------------------------------------

async def keepalive(mux):
    try:
        while True:
            await asyncio.sleep(15)
            await mux.send_frame("P", 0)
    except (asyncio.CancelledError, ConnectionClosedError, ConnectionClosedOK):
        pass


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def run_tunnel_once(cfg, ssl_ctx, on_established=None):
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
        print("[*] Using pre-connected proxy socket: %s" % sock)

    try:
        ws = await websockets.connect(ws_url, **ws_kwargs)
    except Exception as e:
        print("[!] WebSocket connect failed: %s" % e)
        raise

    async with ws:
        print("[*] Setting up FIFOs ...")
        await deploy_remote(ws)

        mux = WsMux(ws)
        await mux.start()

        ka_task = asyncio.ensure_future(keepalive(mux))

        listen_host = cfg["listen_host"]
        listen_port = cfg["listen_port"]
        server = await asyncio.start_server(
            lambda r, w: handle_client(r, w, mux),
            listen_host, listen_port,
        )
        print("[+] Forwarding %s:%d -> tunnel -> (exit upstream)"
              % (listen_host, listen_port))
        if on_established is not None:
            on_established()

        serve_task = asyncio.create_task(server.serve_forever())
        reader_task = mux._reader_task
        tunnel_ws_dead = False
        try:
            done, _pending = await asyncio.wait(
                {serve_task, reader_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            finished = next(iter(done))
            if finished is reader_task:
                if not reader_task.cancelled():
                    tunnel_ws_dead = True
                    print("[!] Tunnel WebSocket reader ended; will reconnect.")
                serve_task.cancel()
            else:
                reader_task.cancel()
            await asyncio.gather(serve_task, reader_task, return_exceptions=True)
        except asyncio.CancelledError:
            serve_task.cancel()
            reader_task.cancel()
            await asyncio.gather(serve_task, reader_task, return_exceptions=True)
            raise
        finally:
            ka_task.cancel()
            try:
                await ka_task
            except asyncio.CancelledError:
                pass
            await mux.stop()
            server.close()
            await server.wait_closed()

        if tunnel_ws_dead:
            raise RuntimeError("WebSocket tunnel closed")


async def run_tunnel(cfg):
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
            await run_tunnel_once(cfg, ssl_ctx, on_established=session_established)
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

    if not cfg["verify_ssl"]:
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    try:
        asyncio.run(run_tunnel(cfg))
    except KeyboardInterrupt:
        print("\n[*] Interrupted.")


if __name__ == "__main__":
    main()
