#!/usr/bin/env python3
"""
Remote tunnel client — runs in foreground via exec, replacing the shell.
Bridges stdin/stdout (the WebSocket terminal) to the Unix socket
where server.py is listening.
"""

import os
import select
import socket
import sys

SOCK_PATH = "/tmp/.wst.sock"
MAX_RECV = 4096


def main():
    # Unbuffered binary stdin/stdout
    stdin = os.fdopen(0, "rb", 0)
    stdout = os.fdopen(1, "wb", 0)

    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.connect(SOCK_PATH)
    sock.setblocking(False)

    # Wait for TUNNEL_READY from server
    ready_buf = b""
    while True:
        r, _, _ = select.select([sock], [], [], 5.0)
        if sock in r:
            data = sock.recv(MAX_RECV)
            if not data:
                sys.exit(1)
            ready_buf += data
            if b"\n" in ready_buf:
                line, ready_buf = ready_buf.split(b"\n", 1)
                stdout.write(line + b"\n")
                break

    # Bidirectional relay: stdin <-> unix socket
    sock_buf = ready_buf  # leftover after TUNNEL_READY
    stdin_buf = b""

    while True:
        rlist = [stdin, sock]
        wlist = []
        if stdin_buf:
            wlist.append(sock)
        if sock_buf:
            wlist.append(stdout)

        try:
            readable, writable, _ = select.select(rlist, wlist, [], 1.0)
        except (ValueError, OSError):
            break

        if stdout in writable and sock_buf:
            try:
                written = stdout.write(sock_buf)
                if written:
                    sock_buf = sock_buf[written:]
            except OSError:
                break

        if sock in writable and stdin_buf:
            try:
                sent = sock.send(stdin_buf)
                stdin_buf = stdin_buf[sent:]
            except OSError:
                break

        if stdin in readable:
            try:
                data = stdin.read(MAX_RECV)
            except OSError:
                break
            if not data:
                break
            stdin_buf += data

        if sock in readable:
            try:
                data = sock.recv(MAX_RECV)
            except BlockingIOError:
                continue
            except OSError:
                break
            if not data:
                break
            sock_buf += data

    sock.close()


if __name__ == "__main__":
    main()
