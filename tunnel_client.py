#!/usr/bin/env python3
"""
SOCKS5 tunnel client — authenticates with cPanel, deploys remote scripts
via the WebSocket terminal, then runs a local SOCKS5 proxy on :1080
that forwards traffic through the tunnel.
"""

import asyncio
import base64
import os
import ssl
import struct
import sys

import requests
import websockets
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK

from cpanel_auth import CONFIG_FILE, load_config, cpanel_login, build_ws_url

SOCKS5_PORT = 10808
MAX_CHUNK = 2048
REMOTE_DIR = os.path.join(os.path.dirname(__file__), "remote")

# These are the cPanel Python app folders used by the "safe patch/restore"
# deploy strategy (see test.py).
SERVER_PROJ = "monkey"
CLIENT_PROJ = "monkeytexts"


# ---------------------------------------------------------------------------
# Protocol helpers (mirrors remote/server.py)
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
# Deployer — uploads and starts remote scripts via the terminal
# ---------------------------------------------------------------------------

async def deploy_remote(ws):
    """Send commands through the WebSocket terminal to deploy and start
    the remote server and client scripts. Returns when TUNNEL_READY is seen."""

    async def remote_write(dst, content):
        b64 = base64.b64encode(content).decode("ascii")
        cmd = (
            "python3 -c \""
            "import base64 as b,sys;"
            "open('%s','wb').write(b.b64decode(sys.argv[1]))"
            "\" '%s'" % (dst, b64)
        )
        await ws.send(cmd + "\n")

    async def send_cmd(cmd):
        await ws.send(cmd + "\n")

    async def read_output(timeout=0.5):
        """Read any pending output for up to `timeout` seconds."""
        buff = b""
        try:
            while True:
                chunk = await asyncio.wait_for(ws.recv(), timeout=timeout)
                if isinstance(chunk, str):
                    chunk = chunk.encode("utf-8", errors="replace")
                buff += chunk
        except asyncio.TimeoutError:
            pass
        return buff

    async def drain(timeout=0.5):
        """Read and discard any pending output for *timeout* seconds."""
        try:
            while True:
                d = await asyncio.wait_for(ws.recv(), timeout=timeout)
                print(d)
        except asyncio.TimeoutError:
            pass

    # Suppress echo, newline translation, and prompt
    await send_cmd("HISTFILE=/dev/null; history -c")
    await asyncio.sleep(0.2)
    await send_cmd("stty -echo")
    await asyncio.sleep(0.3)
    await send_cmd("stty -onlcr")
    await asyncio.sleep(0.2)
    await send_cmd("export PS1=''")
    await asyncio.sleep(0.2)
    await drain(0.5)

    # Deploy strategy (same as test.py):
    # - read current ~/SERVER_PROJ/main.py
    # - write combined (old + remote/server.py) to main.py
    # - start it backgrounded
    # - restore original main.py

    await send_cmd(f"cp ~/{SERVER_PROJ}/main.py ~/{SERVER_PROJ}/main.py.bak")
    await asyncio.sleep(0.2)
    await drain(0.3)

    with open('remote/server.py', 'rb') as f:
        await remote_write(f'{SERVER_PROJ}/main.py', f.read())
    await asyncio.sleep(0.5)
    await drain(0.3)

    await send_cmd(f'python3 ~/{SERVER_PROJ}/main.py &')
    await asyncio.sleep(0.5)
    await drain(0.3)
    
    await send_cmd(f"mv ~/{SERVER_PROJ}/main.py.bak ~/{SERVER_PROJ}/main.py")
    await asyncio.sleep(0.5)
    await drain(0.3)

    await send_cmd(f'cp ~/{CLIENT_PROJ}/main.py ~/{CLIENT_PROJ}/main.py.bak')
    await asyncio.sleep(0.5)
    await drain(0.3)
    
    with open('remote/client.py', 'rb') as f:
        client_entry = f.read()
    await remote_write(f'{CLIENT_PROJ}/main.py', client_entry)
    await asyncio.sleep(0.5)
    await drain(0.3)
    await send_cmd(f"bash -c 'sleep 10 && mv ~/{CLIENT_PROJ}/main.py.bak ~/{CLIENT_PROJ}/main.py' &")


    # Replace the shell with the already-deployed client app.
    await send_cmd(f"exec python3 ~/{CLIENT_PROJ}/main.py")

    # Wait for TUNNEL_READY
    buf = ""
    while True:
        try:
            frame = await asyncio.wait_for(ws.recv(), timeout=15.0)
        except asyncio.TimeoutError:
            raise RuntimeError("Timed out waiting for TUNNEL_READY from remote")
        if isinstance(frame, bytes):
            frame = frame.decode("utf-8", errors="replace")
        buf += frame
        if "TUNNEL_READY" in buf:
            print("[+] Tunnel established.")
            return


# ---------------------------------------------------------------------------
# WebSocket multiplexer
# ---------------------------------------------------------------------------

class WsMux:
    def __init__(self, ws):
        self.ws = ws
        self._queues = {}         # conn_id -> asyncio.Queue
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
                    elif ftype == "P":
                        pass  # pong, ignore
        except (ConnectionClosedOK, ConnectionClosedError):
            pass
        # Signal all waiting queues
        for q in self._queues.values():
            await q.put((None, b""))


# ---------------------------------------------------------------------------
# SOCKS5 server
# ---------------------------------------------------------------------------

async def socks5_handshake(reader, writer):
    """Perform SOCKS5 handshake. Returns (host, port) on success or raises."""
    # Greeting
    data = await reader.readexactly(2)
    ver, nmethods = struct.unpack("!BB", data)
    if ver != 0x05:
        raise ValueError("Not SOCKS5")
    methods = await reader.readexactly(nmethods)
    # Reply: no auth
    writer.write(struct.pack("!BB", 0x05, 0x00))
    await writer.drain()

    # Request
    data = await reader.readexactly(4)
    ver, cmd, _, atyp = struct.unpack("!BBBB", data)
    if ver != 0x05 or cmd != 0x01:
        # Only CONNECT supported
        writer.write(struct.pack("!BBBBIH", 0x05, 0x07, 0x00, 0x01, 0, 0))
        await writer.drain()
        raise ValueError("Unsupported command %d" % cmd)

    if atyp == 0x01:  # IPv4
        raw = await reader.readexactly(4)
        host = "%d.%d.%d.%d" % struct.unpack("!BBBB", raw)
    elif atyp == 0x03:  # Domain
        dlen = (await reader.readexactly(1))[0]
        host = (await reader.readexactly(dlen)).decode("ascii")
    elif atyp == 0x04:  # IPv6
        raw = await reader.readexactly(16)
        # Format as compressed IPv6
        import ipaddress
        host = str(ipaddress.IPv6Address(raw))
    else:
        raise ValueError("Unsupported address type %d" % atyp)

    port = struct.unpack("!H", await reader.readexactly(2))[0]
    return host, port


def socks5_reply(success, writer):
    if success:
        writer.write(struct.pack("!BBBBIH", 0x05, 0x00, 0x00, 0x01, 0, 0))
    else:
        writer.write(struct.pack("!BBBBIH", 0x05, 0x05, 0x00, 0x01, 0, 0))


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


async def handle_socks_client(reader, writer, mux):
    conn_id = None
    try:
        host, port = await socks5_handshake(reader, writer)
        conn_id = await get_conn_id()
        q = mux.register(conn_id)

        # Request connection through tunnel
        target = "%s:%d" % (host, port)
        await mux.send_frame("N", conn_id, target.encode("utf-8"))

        # Wait for ack
        ftype, payload = await asyncio.wait_for(q.get(), timeout=15.0)
        if ftype != "A" or not payload.startswith(b"OK"):
            socks5_reply(False, writer)
            await writer.drain()
            return

        socks5_reply(True, writer)
        await writer.drain()

        # Relay data bidirectionally
        async def local_to_remote():
            try:
                while True:
                    data = await reader.read(MAX_CHUNK)
                    if not data:
                        await mux.send_frame("C", conn_id)
                        break
                    for i in range(0, len(data), MAX_CHUNK):
                        chunk = data[i:i + MAX_CHUNK]
                        await mux.send_frame("D", conn_id, chunk)
            except (ConnectionError, OSError):
                await mux.send_frame("C", conn_id)

        async def remote_to_local():
            try:
                while True:
                    ftype, payload = await q.get()
                    if ftype == "D":
                        writer.write(payload)
                        await writer.drain()
                    elif ftype == "C" or ftype is None:
                        break
            except (ConnectionError, OSError):
                pass

        await asyncio.gather(local_to_remote(), remote_to_local())

    except (asyncio.TimeoutError, ValueError, asyncio.IncompleteReadError, OSError) as e:
        print("[-] SOCKS5 error: %s" % e)
    finally:
        if conn_id is not None:
            mux.unregister(conn_id)
        writer.close()


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

async def run_tunnel(cfg):
    verify_ssl = cfg["verify_ssl"]
    ssl_ctx = True
    if not verify_ssl:
        ssl_ctx = ssl.create_default_context()
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl.CERT_NONE

    print("[*] Logging in to cPanel ...")
    token, cookie = cpanel_login(
        cfg["host"], cfg["port"], cfg["username"], cfg["password"], verify_ssl
    )
    print("[+] Authenticated. Token: cpsess%s..." % token[:8])

    ws_url = build_ws_url(cfg["host"], cfg["port"], token)
    origin = "https://%s:%d" % (cfg["host"], cfg["port"])

    print("[*] Connecting WebSocket ...")
    async with websockets.connect(
        ws_url,
        ssl=ssl_ctx,
        additional_headers={"Origin": origin, "Cookie": cookie},
        ping_interval=20,
        ping_timeout=30,
    ) as ws:
        print("[*] Deploying remote scripts ...")
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
            print("[*] Tunnel closed.")


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
        print("SSL error. Set verify_ssl: false in config.yaml for self-signed certs.", file=sys.stderr)
        sys.exit(1)
    except requests.exceptions.ConnectionError as e:
        print("Connection error: %s" % e, file=sys.stderr)
        sys.exit(1)
    except RuntimeError as e:
        print("Error: %s" % e, file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
