#!/usr/bin/env python3
"""
Remote relay — runs backgrounded on the cPanel host.
Accepts exactly 2 bridge connections on a Unix socket and
forwards all data between them (dumb pipe).
Sends TUNNEL_READY to each bridge once both are connected.
"""

import atexit
import os
import select
import signal
import socket
import sys

SOCK_PATH = "/tmp/.wst.sock"
MAX_RECV = 4096


def cleanup():
    try:
        os.unlink(SOCK_PATH)
    except OSError:
        pass


def main():
    cleanup()
    atexit.register(cleanup)
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))

    srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    srv.bind(SOCK_PATH)
    srv.listen(2)

    # Accept first bridge (Client A — the SOCKS5 side)
    conn_a, _ = srv.accept()
    # Accept second bridge (Client B — the exit node)
    conn_b, _ = srv.accept()

    # Signal both that the tunnel is ready
    conn_a.sendall(b"TUNNEL_READY\n")
    conn_b.sendall(b"TUNNEL_READY\n")

    conn_a.setblocking(False)
    conn_b.setblocking(False)

    # Bidirectional relay: conn_a <-> conn_b
    buf_a2b = b""  # data from A destined for B
    buf_b2a = b""  # data from B destined for A

    while True:
        rlist = []
        wlist = []

        # Only read from a side if its outgoing buffer isn't too large
        if len(buf_a2b) < 65536:
            rlist.append(conn_a)
        if len(buf_b2a) < 65536:
            rlist.append(conn_b)
        if buf_a2b:
            wlist.append(conn_b)
        if buf_b2a:
            wlist.append(conn_a)

        if not rlist and not wlist:
            break

        try:
            readable, writable, _ = select.select(rlist, wlist, [], 1.0)
        except (ValueError, OSError):
            break

        # Write pending data
        if conn_b in writable and buf_a2b:
            try:
                sent = conn_b.send(buf_a2b)
                buf_a2b = buf_a2b[sent:]
            except OSError:
                break

        if conn_a in writable and buf_b2a:
            try:
                sent = conn_a.send(buf_b2a)
                buf_b2a = buf_b2a[sent:]
            except OSError:
                break

        # Read incoming data
        if conn_a in readable:
            try:
                data = conn_a.recv(MAX_RECV)
            except (BlockingIOError, OSError):
                data = b""
            if not data:
                break
            buf_a2b += data

        if conn_b in readable:
            try:
                data = conn_b.recv(MAX_RECV)
            except (BlockingIOError, OSError):
                data = b""
            if not data:
                break
            buf_b2a += data

    conn_a.close()
    conn_b.close()
    srv.close()


if __name__ == "__main__":
    main()
