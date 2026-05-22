#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, sys
os.environ.setdefault("PYTHONIOENCODING", "utf-8")
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

"""
chatgpt_bridge.py  v5 — no artificial delays
─────────────────────────────────────────────────────────
Calls CliBrowser.ask() directly — all delays removed.
CliBrowser now runs all providers in parallel (fastest wins).
─────────────────────────────────────────────────────────
"""

import argparse
import json
import os
import sys
import time

def log(msg: str):
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)

def _output_result(data: dict):
    print(f"RESULT::{json.dumps(data, ensure_ascii=False)}", flush=True)


def parse_args():
    p = argparse.ArgumentParser(description="ChatGPT CLI bridge")
    p.add_argument("--prompt",   required=True)
    p.add_argument("--job-id",   required=True)
    p.add_argument("--headless", default="0")
    p.add_argument("--provider", default="chatgpt", help="chatgpt | auto")
    return p.parse_args()


def main():
    args   = parse_args()
    prompt = args.prompt

    log(f"Bridge started  job={args.job_id}")
    log(f"Prompt: {prompt[:80]}{'...' if len(prompt) > 80 else ''}")
    log(f"[MODE] Provider: {args.provider}")

    # Import the parallel CLI browser
    script_dir = os.path.dirname(os.path.abspath(__file__))
    sys.path.insert(0, script_dir)

    try:
        from cli_browser import CliBrowser
    except ImportError as e:
        error = f"cli_browser import failed: {e}"
        log(f"[ERROR] {error}")
        _output_result({"success": False, "response": "", "error": error, "logs": []})
        return

    log("[BROWSER] Running parallel provider race ...")
    browser = CliBrowser()
    result  = browser.ask(prompt, provider=args.provider)

    _output_result(result)
    log(f"[BRIDGE] Done. success={result.get('success')} provider={result.get('provider','?')}")


if __name__ == "__main__":
    main()
