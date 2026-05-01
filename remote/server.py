#!/usr/bin/env python3
"""
Remote tunnel server — runs backgrounded on the cPanel host.
Listens on a Unix socket, accepts frames from the client script,
opens TCP connections to targets, and relays data back.
Protocol: newline-delimited  TYPE:CONN_ID:PAYLOAD_B64
"""

import atexit
import base64
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


def main():
    cleanup()
    atexit.register(cleanup)
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))

    srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    srv.bind(SOCK_PATH)
    srv.listen(1)

    conn, _ = srv.accept()
    conn.setblocking(False)

    # Signal readiness
    conn.sendall(b"TUNNEL_READY\n")

    # conn_id -> tcp socket
    tcp_socks = {}
    # tcp socket -> conn_id  (reverse map)
    tcp_to_id = {}
    # read buffer for the unix socket
    buf = b""
    # write buffer for the unix socket
    out_buf = b""

    running = True
    while running:
        rlist = [conn]
        wlist = []
        for s in list(tcp_socks.values()):
            rlist.append(s)
        if out_buf:
            wlist.append(conn)

        try:
            readable, writable, _ = select.select(rlist, wlist, [], 1.0)
        except (ValueError, OSError):
            break

        # Write pending data to unix socket
        if conn in writable and out_buf:
            try:
                sent = conn.send(out_buf)
                out_buf = out_buf[sent:]
            except (BrokenPipeError, OSError):
                break

        # Read from unix socket (frames from client script)
        if conn in readable:
            try:
                data = conn.recv(MAX_RECV)
            except (BlockingIOError, OSError):
                data = b""
            if not data:
                break
            buf += data
            while b"\n" in buf:
                line, buf = buf.split(b"\n", 1)
                line = line.decode("ascii", errors="replace")
                ftype, cid, payload = decode_frame(line)
                if ftype is None:
                    continue

                if ftype == "N":
                    # New connection request: payload = host:port
                    target = payload.decode("utf-8", errors="replace")
                    host_port = target.rsplit(":", 1)
                    if len(host_port) != 2:
                        out_buf += encode_frame("A", cid, b"ERR:bad_target").encode("ascii")
                        continue
                    thost, tport_s = host_port
                    try:
                        tport = int(tport_s)
                    except ValueError:
                        out_buf += encode_frame("A", cid, b"ERR:bad_port").encode("ascii")
                        continue
                    try:
                        ts = socket.create_connection((thost, tport), timeout=10)
                        ts.setblocking(False)
                        tcp_socks[cid] = ts
                        tcp_to_id[ts] = cid
                        out_buf += encode_frame("A", cid, b"OK").encode("ascii")
                    except Exception as e:
                        msg = "ERR:%s" % str(e)[:100]
                        out_buf += encode_frame("A", cid, msg.encode("utf-8")).encode("ascii")

                elif ftype == "D":
                    ts = tcp_socks.get(cid)
                    if ts:
                        try:
                            ts.sendall(payload)
                        except OSError:
                            out_buf += encode_frame("C", cid).encode("ascii")
                            tcp_to_id.pop(ts, None)
                            tcp_socks.pop(cid, None)
                            ts.close()

                elif ftype == "C":
                    ts = tcp_socks.pop(cid, None)
                    if ts:
                        tcp_to_id.pop(ts, None)
                        ts.close()

                elif ftype == "P":
                    out_buf += encode_frame("P", 0).encode("ascii")

        # Read from TCP sockets
        for s in readable:
            if s is conn:
                continue
            cid = tcp_to_id.get(s)
            if cid is None:
                continue
            try:
                data = s.recv(MAX_RECV)
            except (BlockingIOError, OSError):
                continue
            if not data:
                # Connection closed by remote
                out_buf += encode_frame("C", cid).encode("ascii")
                tcp_to_id.pop(s, None)
                tcp_socks.pop(cid, None)
                s.close()
            else:
                # Chunk into 2048-byte pieces for PTY safety
                for i in range(0, len(data), 2048):
                    chunk = data[i:i + 2048]
                    out_buf += encode_frame("D", cid, chunk).encode("ascii")

    # Cleanup
    for ts in tcp_socks.values():
        ts.close()
    conn.close()
    srv.close()


if __name__ == "__main__":
    main()
