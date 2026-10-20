# cPanel WebSocket Tunnel (`cpanel-ws-tunnel`)

A stealthy, high-performance tunneling and proxy suite leveraging cPanel's built-in WebSocket terminal (`/cpsess{token}/websocket/Shell`).

By tunneling traffic through standard cPanel HTTPS (port 2083) WebSocket sessions, this tool can bypass restrictive firewalls, stateful Deep Packet Inspection (DPI), and strict network filters where SSH, VPNs, or arbitrary TCP connections are blocked.

---

## Table of Contents

- [Features](#features)
- [How It Works & Architecture](#how-it-works--architecture)
  - [1. Interactive Shell (`terminal.py`)](#1-interactive-shell-terminalpy)
  - [2. Direct SOCKS5 Proxy (`tunnel_client.py`)](#2-direct-socks5-proxy-tunnel_clientpy)
  - [3. Dual-Client Relay Tunnel (`tunnel_server.py` + `tunnel_exit.py`)](#3-dual-client-relay-tunnel-tunnel_serverpy--tunnel_exitpy)
  - [4. High-Performance TCP Forwarder (`http_tunnel.py`)](#4-high-performance-tcp-forwarder-http_tunnelpy)
- [Wire Protocol](#wire-protocol)
- [Requirements & Installation](#requirements--installation)
- [Configuration](#configuration)
- [Usage Guide](#usage-guide)
  - [Interactive Terminal](#interactive-terminal)
  - [Direct SOCKS5 Tunnel](#direct-socks5-tunnel)
  - [Dual-Client Rendezvous Relay](#dual-client-rendezvous-relay)
  - [High-Speed HTTP / TCP Port Forwarder](#high-speed-http--tcp-port-forwarder)
- [Remote Components (`remote/`)](#remote-components-remote)
- [OpSec & Stealth Characteristics](#opsec--stealth-characteristics)
- [Troubleshooting](#troubleshooting)
- [License & Disclaimer](#license--disclaimer)

---

## Features

- **Standard HTTPS Transport**: Encapsulates all data inside genuine cPanel WebSocket frames over TLS (port 2083).
- **Multiple Tunneling Modes**:
  - **Interactive Terminal**: Full PTY terminal in your local CLI with live resize (`SIGWINCH`) handling.
  - **Direct SOCKS5 Proxy**: Turns the cPanel server into your direct outbound proxy.
  - **Rendezvous Relay**: Connects two isolated nodes (Client A and Client B) through cPanel named pipes without either needing public IPs or open inbound ports.
  - **Pre-warmed WebSocket Pool Forwarder**: Zero-remote-dependency TCP forwarder utilizing Bash `/dev/tcp` with pre-warmed sessions for low-latency HTTP/TCP forwarding.
- **Full SOCKS5 Protocol Support**:
  - `CONNECT` (TCP streams)
  - `BIND` (Incoming port binding)
  - `UDP_ASSOCIATE` (Full UDP forwarding)
- **Session Cache**: Automatically reuses and verifies `cpsess` session tokens via cPanel ping checks, avoiding redundant logins.
- **Upstream SOCKS Proxy Chaining**: Route the WebSocket connection itself through an upstream SOCKS4/SOCKS5 proxy.
- **Multi-Runtime Remote Fallbacks**: Ready-to-use remote bridge implementations in Python, Bash, Perl, and Go.

---

## How It Works & Architecture

Modern cPanel interfaces provide a browser-based web terminal using WebSockets. `cpanel-ws-tunnel` logs into cPanel via HTTP basic authentication or form login, extracts the session token (`cpsessXXXXXXXXXX`) and cookie headers, and initiates a persistent WebSocket connection to:

```text
wss://<cpanel-host>:2083/cpsess<token>/websocket/Shell?rows=24&cols=80
```

### 1. Interactive Shell (`terminal.py`)

Streams local `stdin`/`stdout` directly into the cPanel PTY shell session in raw mode. Dynamic window resizes (`SIGWINCH`) are captured and forwarded to cPanel as JSON control packets (`{"resize": [cols, rows]}`).

```
[Local Terminal] <=== WebSocket (TLS :2083) ===> [cPanel Web PTY]
```

---

### 2. Direct SOCKS5 Proxy (`tunnel_client.py`)

1. Connects to the cPanel WebSocket terminal.
2. Injects a lightweight remote server (`remote/server.py` or `remote/server.go`) and bridge script into `/tmp`.
3. Binds a local SOCKS5 proxy server (default: `127.0.0.1:10808`).
4. Traffic is multiplexed across the single WebSocket connection; the cPanel server opens outbound connections to destination targets directly.

```
[Local Apps]
    │ (SOCKS5 :10808)
    ▼
[tunnel_client.py]
    │ (Muxed WebSocket over TLS :2083)
    ▼
[cPanel Terminal Shell]
    │ (Unix Domain Socket /tmp/.wst.sock)
    ▼
[remote/server.py]
    │ (Direct Outbound TCP)
    ▼
[Target Internet Hosts]
```

---

### 3. Dual-Client Relay Tunnel (`tunnel_server.py` + `tunnel_exit.py`)

Acts as a rendezvous relay when neither party can accept incoming connections or when traffic must exit from a separate node:

- **Client A (`tunnel_server.py`)**: Runs on your client machine. Spawns local SOCKS5 proxy, connects to cPanel WebSocket, creates hidden FIFO pipes (`.uuid-r`, `.uuid-w`), and waits.
- **Client B (`tunnel_exit.py`)**: Runs on an exit machine. Connects to the same cPanel host, bridges to the opposite end of the FIFOs, and acts as the exit node.
- **cPanel**: Acts purely as a blind data pipe between Client A and Client B.

```
[Client A (Local)]                           [cPanel Relay Host]                           [Client B (Exit Node)]
  SOCKS5 Proxy (:10808)                     Hidden FIFO Pipes                               Outbound Connections
  (CONNECT / BIND / UDP)                   ($HOME/.cache/.fontconfig)                       (TCP / UDP)
           │                                          │                                             │
           │────── wss://cpanel:2083/Shell ──────────►│                                             │
           │                                          │◄────── wss://cpanel:2083/Shell ─────────────│
           │       [pipe A2B] ───────────────────────►│                                             │
           │◄─────────────────────── [pipe B2A] ──────│                                             │
                                                                                                    ▼
                                                                                            [Target Hosts]
```

---

### 4. High-Performance TCP Forwarder (`http_tunnel.py`)

Optimized for dedicated TCP port forwarding (e.g., exposing a remote web server on `127.0.0.1:8080`):
- Requires **no remote Python or Go binaries** installed on the cPanel host.
- Uses Bash's built-in `/dev/tcp` socket redirection:
  ```bash
  exec 3<>/dev/tcp/<target_host>/<target_port>; echo TUNNEL_READY; cat <&3 & exec cat >&3
  ```
- **Connection Pre-warming**: Maintains an idle pool of warm WebSocket connections in raw PTY mode to eliminate handshake and PTY initialization latency on new TCP connections.

---

## Wire Protocol

Multiplexed modes (`tunnel_client.py`, `tunnel_server.py`, `tunnel_exit.py`) exchange newline-delimited ASCII frames:

$$\text{TYPE}:\text{CONN\_ID}:\text{BASE64\_PAYLOAD}\backslash\text{n}$$

| Type | Name | Direction | Description |
|:---:|:---|:---:|:---|
| `N` | **New Connection** | Client $\to$ Exit | Requests outbound TCP connection. Payload: `host:port` |
| `A` | **Ack / Response** | Exit $\to$ Client | Status of request. Payload: `OK`, `OK:bind_host:bind_port`, or `ERR:...` |
| `D` | **Data** | Bidirectional | Stream data chunks (max 2048 bytes raw) |
| `C` | **Close** | Bidirectional | Closes stream for specified `CONN_ID` |
| `B` | **Bind** | Client $\to$ Exit | Requests SOCKS5 BIND listener on Exit node |
| `I` | **Incoming** | Exit $\to$ Client | Notifies incoming connection on bound port. Payload: `remote_host:remote_port` |
| `U` | **UDP Datagram** | Bidirectional | Transports UDP packets. Payload: `host:port\x00<binary-payload>` |
| `P` | **Ping** | Bidirectional | Keepalive probe (`CONN_ID=0`) sent every 15s |

---

## Requirements & Installation

- **Python**: 3.10 or newer
- **cPanel Account**: Must have Web Terminal privilege enabled (cPanel $\to$ Advanced $\to$ Terminal).

### Setup

1. Clone this repository:
   ```bash
   git clone https://github.com/KH4St3H/cpanel-ws-tunnel.git
   cd cpanel-ws-tunnel
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

*(Dependencies include: `requests`, `websockets`, `pyyaml`, `pysocks`, `python-socks[asyncio]`)*

---

## Configuration

Copy the sample configuration file to `config.yaml`:

```bash
cp config.yaml.example config.yaml
```

Edit `config.yaml`:

```yaml
cpanel:
  host: "cpanel.yourdomain.com"     # cPanel hostname or IP
  port: 2083                        # cPanel SSL port (default: 2083)
  username: "your_username"         # cPanel username
  password: "your_password"         # cPanel password
  verify_ssl: true                  # Set to false for self-signed certificates
  proxy: ""                         # Optional upstream SOCKS proxy: socks5://127.0.0.1:1080
```

### Configuration Options

| Option | Type | Default | Description |
|:---|:---:|:---:|:---|
| `host` | `string` | *(required)* | cPanel domain or IP address. |
| `port` | `integer` | `2083` | Port of the cPanel service. |
| `username` | `string` | *(required)* | Your cPanel login username. |
| `password` | `string` | *(required)* | Your cPanel login password. |
| `verify_ssl` | `boolean` | `true` | Validates TLS certificate. Disable only if testing with self-signed certs. |
| `proxy` | `string` | `null` | Optional upstream proxy (e.g. `socks5://user:pass@127.0.0.1:1080`). |
| `ws_pool_size` | `integer` | `4` | Number of pre-warmed connections for `http_tunnel.py`. |
| `listen_host` | `string` | `127.0.0.1` | Local listen interface for `http_tunnel.py`. |
| `listen_port` | `integer` | `8080` | Local listen port for `http_tunnel.py`. |

---

## Usage Guide

### Interactive Terminal

Launch an interactive remote shell session inside cPanel directly from your terminal:

```bash
python3 terminal.py
# Or specify a custom config file:
python3 terminal.py path/to/custom_config.yaml
```

---

### Direct SOCKS5 Tunnel

Turns your cPanel server into an immediate outbound SOCKS5 proxy:

```bash
python3 tunnel_client.py
```

- Local SOCKS5 proxy will listen on: **`127.0.0.1:10808`**
- Test with curl:
  ```bash
  curl --socks5-hostname 127.0.0.1:10808 https://ifconfig.me
  ```

---

### Dual-Client Rendezvous Relay

Use cPanel as a rendezvous point between two hosts without direct connectivity:

#### Step 1: Start Client A (Proxy Node)
On your client workstation:
```bash
python3 tunnel_server.py
```
Client A creates the FIFOs on cPanel and prints:
```text
[*] Waiting for exit node (Client B) to connect ...
```

#### Step 2: Start Client B (Exit Node)
On the remote exit machine (which also has `config.yaml` configured for the same cPanel account):
```bash
python3 tunnel_exit.py
```

Once connected, both nodes print:
```text
[+] Tunnel established.
[+] SOCKS5 proxy listening on 127.0.0.1:10808
```

You can now use `127.0.0.1:10808` on Client A; traffic will route through cPanel and exit from Client B!

---

### High-Speed HTTP / TCP Port Forwarder

Forward local connections through pre-warmed bash `/dev/tcp` sessions:

```bash
python3 http_tunnel.py -v
```

- Listens on `127.0.0.1:8080` and forwards to the target defined in `http_tunnel.py`.
- Features an automated background connection refill pool.

---

## Remote Components (`remote/`)

The `remote/` directory contains auxiliary components that can run directly on the cPanel host when custom bridging or alternative runtimes are desired:

| File | Language | Role | Description |
|:---|:---:|:---:|:---|
| `server.py` | Python 3 | Direct Exit Server | Listens on Unix socket `/tmp/.wst.sock` and handles outbound TCP connections. |
| `server.go` | Go | Direct Exit Server | High-performance Go port of `server.py`. |
| `client.py` | Python 3 | PTY Bridge | Bridges standard I/O (WebSocket PTY) to `/tmp/.wst.sock`. |
| `client.sh` | Bash | PTY Bridge | Lightweight bridge using `nc -U` or `socat`. |
| `client.pl` | Perl | PTY Bridge | Pure-Perl bridge for minimal environments without `nc`/`socat`. |
| `relay.py` | Python 3 | Dual-Client Relay | Multiplexes two Unix socket connections for the rendezvous pattern. |
| `relay.pl` | Perl | Dual-Client Relay | Pure-Perl port of `relay.py`. |

---

## OpSec & Stealth Characteristics

When deploying and operating tunnels in restricted or monitored environments:

- **Traffic Disguise**: All network traffic from the client is an encrypted HTTPS connection on standard port 2083 with valid TLS handshakes.
- **Terminal Cleanliness**:
  - Automatically disables command history (`HISTFILE=/dev/null; history -c`).
  - Suppresses shell echo and formatting (`stty -echo`, `stty -onlcr`, `PS1=''`).
- **Process Camouflage**: Remote processes are launched using `exec -a 'bash'` to appear as normal shell instances in process listings (`ps`).
- **Innocuous Storage**: Temporary files and FIFOs are stored under disguised paths (such as `$HOME/.cache/.fontconfig/.uuid-*`).

---

## Troubleshooting

### `Config error: ...`
Ensure `config.yaml` exists in the working directory (or provide its path as the first argument) and includes `host`, `username`, and `password`.

### `SSL error` / `CERTIFICATE_VERIFY_FAILED`
If your cPanel provider uses a self-signed SSL certificate, set `verify_ssl: false` in `config.yaml`.

### `Timed out waiting for TUNNEL_READY`
1. Check if the cPanel account has Terminal access enabled.
2. In direct mode, verify that Python 3 or `nc`/`socat` can execute in the user's shell environment.
3. In relay mode, ensure `tunnel_server.py` is started before `tunnel_exit.py`.

### Upstream Proxy Issues
If connecting through corporate proxies or Tor, specify `proxy: "socks5://127.0.0.1:9050"` in `config.yaml`.

---

## License & Disclaimer

This project is created for authorized network administration, auditing, and educational security research only. Ensure you have proper authorization from the server owner before using this tool on any infrastructure.
