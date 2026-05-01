#!/usr/bin/env python3
"""
Tunnel Server (Client A) — SOCKS5 proxy side.
Connects to cPanel, deploys the relay + bridge scripts,
starts SOCKS5 server locally, and forwards traffic through the tunnel.
Client B (tunnel_exit.py) must connect to the same cPanel host afterwards.
"""

import asyncio
import base64
import os
import socket
import ssl
import struct
import sys
import traceback

import requests
import websockets
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK

from cpanel_auth import CONFIG_FILE, load_config, cpanel_login, build_ws_url
from proxy_util import create_proxy_connection

SOCKS5_PORT = 10808
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
# Deployer — uploads relay + bridge, starts relay, execs bridge
# ---------------------------------------------------------------------------

async def deploy_remote(ws):
    """Deploy relay.py and client.py (bridge) to cPanel, start them,
    and wait for TUNNEL_READY."""

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

    # Set up hidden working dir and FIFOs with innocuous names
    await send_cmd("mkdir -p $HOME/.cache/.fontconfig")
    await asyncio.sleep(0.2)
    await send_cmd("rm -f $HOME/.cache/.fontconfig/.uuid-r $HOME/.cache/.fontconfig/.uuid-w; mkfifo $HOME/.cache/.fontconfig/.uuid-r $HOME/.cache/.fontconfig/.uuid-w")
    await asyncio.sleep(0.3)
    await drain(0.3, silent=False)

    # Open pipes with disguised process names
    await send_cmd("exec -a 'bash' cat $HOME/.cache/.fontconfig/.uuid-r & exec -a 'bash' cat > $HOME/.cache/.fontconfig/.uuid-w")
    print("[*] Waiting for exit node (Client B) to connect ...")

    # The exec blocks until Client B opens the other end of the FIFO.
    # We repeatedly send TUNNEL_READY until Client B echoes it back,
    # confirming the round-trip pipe is working.
    buf = ""
    while True:
        # Send a probe — if pipes aren't connected yet it gets buffered
        # or dropped, which is fine. Once connected, Client B will see it.
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
            # No response yet — Client B hasn't connected. Retry.
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
        except (ConnectionClosedOK, ConnectionClosedError):
            pass
        for q in self._queues.values():
            await q.put((None, b""))


# ---------------------------------------------------------------------------
# SOCKS5 server
# ---------------------------------------------------------------------------

SOCKS5_CMD_CONNECT = 0x01
SOCKS5_CMD_BIND = 0x02
SOCKS5_CMD_UDP = 0x03


async def socks5_read_addr(reader):
    """Read SOCKS5 address (atyp + addr + port). Returns (host, port, raw_addr_bytes)."""
    data = await reader.readexactly(1)
    atyp = data[0]
    if atyp == 0x01:
        raw = await reader.readexactly(4)
        host = "%d.%d.%d.%d" % struct.unpack("!BBBB", raw)
        addr_bytes = bytes([atyp]) + raw
    elif atyp == 0x03:
        dlen_b = await reader.readexactly(1)
        dlen = dlen_b[0]
        name = await reader.readexactly(dlen)
        host = name.decode("ascii")
        addr_bytes = bytes([atyp]) + dlen_b + name
    elif atyp == 0x04:
        raw = await reader.readexactly(16)
        import ipaddress
        host = str(ipaddress.IPv6Address(raw))
        addr_bytes = bytes([atyp]) + raw
    else:
        raise ValueError("Unsupported address type %d" % atyp)
    port_b = await reader.readexactly(2)
    port = struct.unpack("!H", port_b)[0]
    return host, port, addr_bytes + port_b


async def socks5_handshake(reader, writer):
    """Returns (cmd, host, port)."""
    data = await reader.readexactly(2)
    ver, nmethods = struct.unpack("!BB", data)
    dbg("greeting: ver=%d nmethods=%d" % (ver, nmethods))
    if ver != 0x05:
        raise ValueError("Not SOCKS5 (ver=%d)" % ver)
    methods = await reader.readexactly(nmethods)
    dbg("methods: %s" % [m for m in methods])
    writer.write(struct.pack("!BB", 0x05, 0x00))
    await writer.drain()

    data = await reader.readexactly(3)
    ver, cmd, _ = struct.unpack("!BBB", data)
    cmd_names = {1: "CONNECT", 2: "BIND", 3: "UDP_ASSOCIATE"}
    dbg("request: ver=%d cmd=%d(%s)" % (ver, cmd, cmd_names.get(cmd, "?")))
    if ver != 0x05:
        raise ValueError("Not SOCKS5 (ver=%d)" % ver)
    if cmd not in (SOCKS5_CMD_CONNECT, SOCKS5_CMD_BIND, SOCKS5_CMD_UDP):
        writer.write(struct.pack("!BBBBIH", 0x05, 0x07, 0x00, 0x01, 0, 0))
        await writer.drain()
        raise ValueError("Unsupported command %d" % cmd)

    host, port, _ = await socks5_read_addr(reader)
    dbg("target: %s:%d" % (host, port))
    return cmd, host, port


def socks5_reply(rep_code, writer, bind_addr="0.0.0.0", bind_port=0):
    """Send SOCKS5 reply. rep_code: 0x00=success, 0x01=general failure, 0x05=refused, etc."""
    parts = bind_addr.split(".")
    if len(parts) == 4:
        addr_bytes = struct.pack("!BBBB", *[int(p) for p in parts])
        writer.write(struct.pack("!BBB", 0x05, rep_code, 0x00) +
                      b'\x01' + addr_bytes + struct.pack("!H", bind_port))
    else:
        writer.write(struct.pack("!BBBBIH", 0x05, rep_code, 0x00, 0x01, 0, bind_port))


def parse_udp_header(data):
    """Parse SOCKS5 UDP header. Returns (frag, atyp, host, port, payload, header_len)."""
    if len(data) < 4:
        return None
    frag = data[2]
    atyp = data[3]
    offset = 4
    if atyp == 0x01:
        if len(data) < offset + 6:
            return None
        host = "%d.%d.%d.%d" % struct.unpack("!BBBB", data[offset:offset + 4])
        offset += 4
    elif atyp == 0x03:
        if len(data) < offset + 1:
            return None
        dlen = data[offset]
        offset += 1
        if len(data) < offset + dlen + 2:
            return None
        host = data[offset:offset + dlen].decode("ascii")
        offset += dlen
    elif atyp == 0x04:
        if len(data) < offset + 18:
            return None
        import ipaddress
        host = str(ipaddress.IPv6Address(data[offset:offset + 16]))
        offset += 16
    else:
        return None
    port = struct.unpack("!H", data[offset:offset + 2])[0]
    offset += 2
    return frag, atyp, host, port, data[offset:], offset


def build_udp_header(host, port):
    """Build SOCKS5 UDP response header (RSV + FRAG + ATYP + ADDR + PORT)."""
    # Try IPv4 first
    parts = host.split(".")
    if len(parts) == 4:
        try:
            addr = struct.pack("!BBBB", *[int(p) for p in parts])
            return struct.pack("!HBB", 0, 0, 0x01) + addr + struct.pack("!H", port)
        except (ValueError, struct.error):
            pass
    # Domain name
    encoded = host.encode("ascii")
    return struct.pack("!HBB", 0, 0, 0x03) + bytes([len(encoded)]) + encoded + struct.pack("!H", port)


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
    """Bidirectional relay between a local TCP socket and the tunnel mux."""

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
                    await mux.send_frame("D", conn_id, data[i:i + MAX_CHUNK])
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


async def handle_connect(reader, writer, mux, conn_id, host, port):
    """Handle SOCKS5 CONNECT command."""
    q = mux.register(conn_id)
    target = "%s:%d" % (host, port)
    dbg("connect conn_id=%d → %s" % (conn_id, target))
    await mux.send_frame("N", conn_id, target.encode("utf-8"))

    dbg("connect conn_id=%d waiting for ack" % conn_id)
    ftype, payload = await asyncio.wait_for(q.get(), timeout=15.0)
    dbg("connect conn_id=%d ack: type=%s payload=%s" % (conn_id, ftype, payload[:80]))
    if ftype != "A" or not payload.startswith(b"OK"):
        dbg("connect conn_id=%d FAILED: %s" % (conn_id, payload))
        socks5_reply(0x05, writer)
        await writer.drain()
        return

    dbg("connect conn_id=%d established" % conn_id)
    socks5_reply(0x00, writer)
    await writer.drain()

    await _relay_data(reader, writer, mux, q, conn_id)


async def handle_bind(reader, writer, mux, conn_id, host, port):
    """Handle SOCKS5 BIND command."""
    q = mux.register(conn_id)
    target = "%s:%d" % (host, port)
    await mux.send_frame("B", conn_id, target.encode("utf-8"))

    # First reply: bound address from exit node
    ftype, payload = await asyncio.wait_for(q.get(), timeout=15.0)
    if ftype != "A" or not payload.startswith(b"OK:"):
        socks5_reply(0x05, writer)
        await writer.drain()
        return

    # payload = OK:bind_host:bind_port
    parts = payload.decode("utf-8").split(":", 2)
    bind_host = parts[1] if len(parts) > 1 else "0.0.0.0"
    bind_port = int(parts[2]) if len(parts) > 2 else 0
    socks5_reply(0x00, writer, bind_host, bind_port)
    await writer.drain()

    # Second reply: incoming connection
    ftype, payload = await asyncio.wait_for(q.get(), timeout=120.0)
    if ftype != "I":
        socks5_reply(0x05, writer)
        await writer.drain()
        return

    # payload = remote_host:remote_port
    rparts = payload.decode("utf-8").rsplit(":", 1)
    rhost = rparts[0] if len(rparts) > 0 else "0.0.0.0"
    rport = int(rparts[1]) if len(rparts) > 1 else 0
    socks5_reply(0x00, writer, rhost, rport)
    await writer.drain()

    await _relay_data(reader, writer, mux, q, conn_id)


async def handle_udp_associate(reader, writer, mux, conn_id):
    """Handle SOCKS5 UDP ASSOCIATE command."""
    q = mux.register(conn_id)
    loop = asyncio.get_event_loop()

    # Create local UDP relay socket
    udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp_sock.bind(("127.0.0.1", 0))
    udp_sock.setblocking(False)
    relay_addr, relay_port = udp_sock.getsockname()

    # Tell the SOCKS5 client where to send UDP
    socks5_reply(0x00, writer, relay_addr, relay_port)
    await writer.drain()

    client_addr = None  # Will be set on first UDP packet from client

    async def udp_to_tunnel():
        """Read UDP from local client, parse SOCKS5 header, send U frame to tunnel."""
        nonlocal client_addr
        while True:
            try:
                data, addr = await loop.run_in_executor(None, lambda: udp_sock.recvfrom(65535))
            except OSError:
                break
            if not data:
                break
            client_addr = addr
            parsed = parse_udp_header(data)
            if parsed is None:
                continue
            frag, atyp, dhost, dport, payload, _ = parsed
            if frag != 0:
                continue  # fragmentation not supported
            # Encode: target_host:target_port + \x00 + udp_payload
            target_info = ("%s:%d" % (dhost, dport)).encode("utf-8") + b"\x00" + payload
            await mux.send_frame("U", conn_id, target_info)

    async def tunnel_to_udp():
        """Read U frames from tunnel, wrap with SOCKS5 header, send UDP to client."""
        while True:
            ftype, payload = await q.get()
            if ftype == "U":
                # payload = source_host:source_port\x00udp_data
                sep = payload.find(b"\x00")
                if sep < 0:
                    continue
                src_info = payload[:sep].decode("utf-8", errors="replace")
                udp_data = payload[sep + 1:]
                src_parts = src_info.rsplit(":", 1)
                if len(src_parts) != 2:
                    continue
                src_host = src_parts[0]
                src_port = int(src_parts[1])
                header = build_udp_header(src_host, src_port)
                if client_addr:
                    try:
                        udp_sock.sendto(header + udp_data, client_addr)
                    except OSError:
                        pass
            elif ftype == "C" or ftype is None:
                break

    async def watch_tcp():
        """The TCP connection stays open during the association.
        If the client closes it, the UDP association ends."""
        try:
            while True:
                data = await reader.read(1)
                if not data:
                    break
        except (ConnectionError, OSError):
            pass

    try:
        done, pending = await asyncio.wait(
            [asyncio.ensure_future(t) for t in [udp_to_tunnel(), tunnel_to_udp(), watch_tcp()]],
            return_when=asyncio.FIRST_COMPLETED,
        )
        for t in pending:
            t.cancel()
    finally:
        await mux.send_frame("C", conn_id)
        udp_sock.close()


async def handle_socks_client(reader, writer, mux):
    conn_id = None
    peer = writer.get_extra_info("peername")
    dbg("new connection from %s" % (peer,))
    try:
        cmd, host, port = await socks5_handshake(reader, writer)
        conn_id = await get_conn_id()
        dbg("conn_id=%d cmd=%d target=%s:%d" % (conn_id, cmd, host, port))

        if cmd == SOCKS5_CMD_CONNECT:
            await handle_connect(reader, writer, mux, conn_id, host, port)
        elif cmd == SOCKS5_CMD_BIND:
            await handle_bind(reader, writer, mux, conn_id, host, port)
        elif cmd == SOCKS5_CMD_UDP:
            await handle_udp_associate(reader, writer, mux, conn_id)

    except asyncio.CancelledError:
        dbg("conn_id=%s cancelled" % conn_id)
    except (asyncio.TimeoutError, ValueError, asyncio.IncompleteReadError,
            ConnectionError, OSError) as e:
        dbg("SOCKS5 error (conn_id=%s): %s" % (conn_id, e))
        if DEBUG:
            traceback.print_exc()
    except Exception as e:
        print("[-] SOCKS5 unexpected error (conn_id=%s): %s" % (conn_id, e))
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

async def run_tunnel_once(cfg, ssl_ctx):
    """Single tunnel session. Raises on disconnect."""
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
    print("[*] WS URL: %s" % ws_url)
    print("[*] Origin: %s" % origin)
    print("[*] Cookie: %s" % cookie)
    print("[*] Proxy: %s" % proxy)
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
        ws_conn = websockets.connect(ws_url, **ws_kwargs)
        ws = await ws_conn
    except Exception as e:
        print("[!] WebSocket connect failed: %s" % e)
        if hasattr(e, 'status_code'):
            print("[!] HTTP status: %s" % e.status_code)
        if hasattr(e, 'headers'):
            print("[!] Response headers: %s" % e.headers)
        raise

    async with ws:
        print("[*] Setting up FIFOs ...")
        await deploy_remote(ws)

        mux = WsMux(ws)
        await mux.start()

        ka_task = asyncio.ensure_future(keepalive(mux))

        server = await asyncio.start_server(
            lambda r, w: handle_socks_client(r, w, mux),
            "127.0.0.1", SOCKS5_PORT,
        )
        print("[+] SOCKS5 proxy listening on 127.0.0.1:%d" % SOCKS5_PORT)

        try:
            await server.serve_forever()
        except asyncio.CancelledError:
            pass
        finally:
            ka_task.cancel()
            await mux.stop()
            server.close()


async def run_tunnel(cfg):
    verify_ssl = cfg["verify_ssl"]
    ssl_ctx = True
    if not verify_ssl:
        ssl_ctx = ssl.create_default_context()
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl.CERT_NONE

    retry = 0
    while True:
        try:
            await run_tunnel_once(cfg, ssl_ctx)
            break  # clean exit
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
