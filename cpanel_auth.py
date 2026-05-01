#!/usr/bin/env python3
"""Shared cPanel authentication and config utilities."""

import json
import os

import yaml
import requests


CONFIG_FILE = os.path.join(os.path.dirname(__file__), "config.yaml")
CACHE_FILE = os.path.join(os.path.dirname(__file__), ".session_cache.json")


def load_config(path=None):
    path = path or CONFIG_FILE
    if not os.path.exists(path):
        raise FileNotFoundError("Config file not found: %s" % path)
    with open(path) as f:
        raw = yaml.safe_load(f)
    if not isinstance(raw, dict) or "cpanel" not in raw:
        raise ValueError("Config file must contain a 'cpanel' key")
    s = raw["cpanel"]
    return {
        "host":       str(s.get("host", "")).strip(),
        "port":       int(s.get("port", 2083)),
        "username":   str(s.get("username", "")).strip(),
        "password":   str(s.get("password", "")).strip(),
        "verify_ssl": bool(s.get("verify_ssl", True)),
        "proxy":      str(s.get("proxy", "")).strip() or None,
    }


def _save_cache(host, port, username, token, cookie):
    try:
        data = {"host": host, "port": port, "username": username,
                "token": token, "cookie": cookie}
        with open(CACHE_FILE, "w") as f:
            json.dump(data, f)
    except OSError:
        pass


def _load_cache(host, port, username):
    try:
        with open(CACHE_FILE) as f:
            data = json.load(f)
        if (data.get("host") == host and data.get("port") == port
                and data.get("username") == username):
            return data.get("token", ""), data.get("cookie", "")
    except (OSError, json.JSONDecodeError, TypeError):
        pass
    return None, None


def _proxy_dict(proxy_url):
    """Build requests-compatible proxy dict from a SOCKS proxy URL."""
    if not proxy_url:
        return None
    return {"http": proxy_url, "https": proxy_url}


def _session_valid(host, port, token, cookie, verify_ssl, proxy=None):
    """Check if a cached session is still alive by hitting a lightweight cPanel endpoint."""
    url = "https://%s:%d/cpsess%s/execute/Session/ping" % (host, port, token)
    try:
        resp = requests.get(url, headers={"Cookie": cookie},
                            verify=verify_ssl, timeout=10,
                            proxies=_proxy_dict(proxy))
        return resp.status_code == 200
    except (requests.RequestException, OSError):
        return False


def _do_login(host, port, username, password, verify_ssl, proxy=None):
    """Perform a fresh login. Returns (token, cookie_header)."""
    base = "https://%s:%d" % (host, port)
    session = requests.Session()
    session.verify = verify_ssl
    if proxy:
        session.proxies = _proxy_dict(proxy)

    resp = session.post(
        base + "/login/",
        data={"user": username, "pass": password},
        allow_redirects=True,
        timeout=15,
    )
    resp.raise_for_status()

    token = ""
    for r in [resp] + list(resp.history):
        url = r.url
        if "/cpsess" in url:
            part = url.split("/cpsess")[1].split("/")[0]
            if part:
                token = part
                break

    if not token:
        raise RuntimeError(
            "Login succeeded but could not extract session token. "
            "Check your credentials or cPanel version."
        )

    cookie_header = "; ".join(
        "%s=%s" % (c.name, c.value) for c in session.cookies
    )
    return token, cookie_header


def cpanel_login(host, port, username, password, verify_ssl=True, proxy=None):
    """Authenticate with cPanel. Uses cached session if still valid.
    Returns (cpsess_token, cookie_header_string)."""
    token, cookie = _load_cache(host, port, username)
    if token and cookie:
        print("[auth] Checking cached session (proxy=%s) ..." % proxy)
        if _session_valid(host, port, token, cookie, verify_ssl, proxy):
            print("[auth] Cached session valid.")
            return token, cookie
        print("[auth] Cached session invalid, re-logging in.")

    token, cookie = _do_login(host, port, username, password, verify_ssl, proxy)
    _save_cache(host, port, username, token, cookie)
    return token, cookie


def build_ws_url(host, port, token, cols=80, rows=24):
    return "wss://%s:%d/cpsess%s/websocket/Shell?rows=%d&cols=%d" % (
        host, port, token, rows, cols
    )
