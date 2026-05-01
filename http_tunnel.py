#!/usr/bin/env python3
"""
HTTP tunnel client — authenticates with cPanel, deploys remote scripts via
the WebSocket terminal, then runs a local TCP forwarder. Every connection
to the local listener is tunnelled to 185.185.185.185:80 on the cPanel
host, where an HTTP server routes by Host header. Plain HTTP and VLESS
xhttp transport both ride this raw byte pipe unchanged.
"""

import asyncio
import ssl
import sys

import requests
import websockets

from cpanel_auth import CONFIG_FILE, load_config, cpanel_login, build_ws_url
from tunnel_client import WsMux, deploy_remote, get_conn_id

LISTEN_HOST = "127.0.0.1"
LISTEN_PORT = 8080
REMOTE_TARGET = "185.185.185.185:80"
MAX_CHUNK = 2048


async def handle_client(reader, writer, mux):
    conn_id = None
    try:
        conn_id = await get_conn_id()
        q = mux.register(conn_id)

        await mux.send_frame("N", conn_id, REMOTE_TARGET.encode("utf-8"))
        ftype, payload = await asyncio.wait_for(q.get(), timeout=15.0)
        if ftype != "A" or not payload.startswith(b"OK"):
            return

        async def local_to_remote():
            try:
                while True:
                    data = await reader.read(MAX_CHUNK)
                    if not data:
                        await mux.send_frame("C", conn_id)
                        return
                    for i in range(0, len(data), MAX_CHUNK):
                        await mux.send_frame("D", conn_id, data[i:i + MAX_CHUNK])
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
                        return
            except (ConnectionError, OSError):
                pass

        await asyncio.gather(local_to_remote(), remote_to_local())

    except (asyncio.TimeoutError, ConnectionError,
            asyncio.IncompleteReadError, OSError) as e:
        print("[-] forward error: %s" % e)
    finally:
        if conn_id is not None:
            mux.unregister(conn_id)
        try:
            writer.close()
        except OSError:
            pass


async def keepalive(mux):
    try:
        while True:
            await asyncio.sleep(15)
            await mux.send_frame("P", 0)
    except asyncio.CancelledError:
        pass


async def run_tunnel(cfg):
    verify_ssl = cfg["verify_ssl"]
    if verify_ssl:
        ssl_ctx = True
    else:
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

        bind_host = cfg.get("listen_host", LISTEN_HOST)
        bind_port = cfg.get("listen_port", LISTEN_PORT)
        server = await asyncio.start_server(
            lambda r, w: handle_client(r, w, mux),
            bind_host, bind_port,
        )
        print("[+] Forwarding %s:%d -> tunnel -> %s"
              % (bind_host, bind_port, REMOTE_TARGET))

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
        print("SSL error. Set verify_ssl: false in config.yaml.", file=sys.stderr)
        sys.exit(1)
    except requests.exceptions.ConnectionError as e:
        print("Connection error: %s" % e, file=sys.stderr)
        sys.exit(1)
    except RuntimeError as e:
        print("Error: %s" % e, file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
