#!/bin/bash
# Remote tunnel client — bash port of remote/client.py.
# Bridges stdin/stdout (the WebSocket terminal) to the Unix socket where
# server.py is listening. Uses nc -U; falls back to socat.
SOCK=/tmp/.wst.sock
nc -U "$SOCK" 2>/dev/null || exec socat - "UNIX-CONNECT:$SOCK"
