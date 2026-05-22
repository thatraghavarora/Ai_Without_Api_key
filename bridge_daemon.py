#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
bridge_daemon.py — Persistent bridge (Windows safe)
Reads JSON from stdin, writes RESULT::<reqId>::<json> to stdout.
stderr is redirected to NUL to avoid g4f internal warnings crash.
"""
import sys, os, json, warnings

# Suppress ALL warnings before importing g4f
warnings.filterwarnings("ignore")
os.environ["PYTHONIOENCODING"] = "utf-8"
os.environ["PYTHONWARNINGS"]   = "ignore"

# Redirect stderr to NUL (Windows) so g4f internal errors don't crash us
try:
    _nul = open(os.devnull, 'w')
    sys.stderr = _nul
except Exception:
    pass

# ── Pre-load g4f once ─────────────────────────────────────────────
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from cli_browser import CliBrowser
browser = CliBrowser()

# Signal ready (use sys.__stdout__ to bypass any redirection)
sys.__stdout__.write("DAEMON_READY\n")
sys.__stdout__.flush()

# ── Request loop ──────────────────────────────────────────────────
for raw in sys.__stdin__:
    raw = raw.strip()
    if not raw:
        continue
    req_id = ""
    try:
        req      = json.loads(raw)
        req_id   = req.get("reqId", "")
        prompt   = req.get("prompt", "")
        provider = req.get("provider", "chatgpt")

        result = browser.ask(prompt, provider=provider)
        out = f"RESULT::{req_id}::" + json.dumps(result, ensure_ascii=False) + "\n"
        sys.__stdout__.write(out)
        sys.__stdout__.flush()

    except Exception as e:
        err = {"success": False, "response": "", "error": str(e), "provider": None}
        out = f"RESULT::{req_id}::" + json.dumps(err) + "\n"
        sys.__stdout__.write(out)
        sys.__stdout__.flush()
