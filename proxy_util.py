#!/usr/bin/env python3
"""Shared utility for routing WebSocket connections through a SOCKS proxy."""

from urllib.parse import urlparse


async def create_proxy_connection(proxy_url, dest_host, dest_port):
    """Create a raw TCP socket to dest_host:dest_port through a SOCKS proxy.

    Returns a connected socket for websockets.connect(sock=...),
    or None if no proxy is configured.
    """
    if not proxy_url:
        return None

    import asyncio
    import socks

    parsed = urlparse(proxy_url)
    scheme = parsed.scheme.lower()
    proxy_type_map = {
        "socks5": socks.SOCKS5,
        "socks5h": socks.SOCKS5,
        "socks4": socks.SOCKS4,
        "socks4a": socks.SOCKS4,
    }
    ptype = proxy_type_map.get(scheme, socks.SOCKS5)
    proxy_host = parsed.hostname
    proxy_port = parsed.port or 1080
    proxy_user = parsed.username
    proxy_pass = parsed.password

    loop = asyncio.get_event_loop()

    def _connect():
        s = socks.socksocket()
        s.set_proxy(ptype, proxy_host, proxy_port,
                    username=proxy_user, password=proxy_pass)
        s.settimeout(15)
        s.connect((dest_host, dest_port))
        s.setblocking(False)
        return s

    sock = await loop.run_in_executor(None, _connect)
    print("[*] Proxy: connected to %s:%d via %s" % (dest_host, dest_port, proxy_url))
    return sock
