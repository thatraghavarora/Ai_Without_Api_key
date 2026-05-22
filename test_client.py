#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
test_client.py  -  Quick Python test client for the local API
--------------------------------------------------------------
Usage:
    python test_client.py "what is AI?"
    python test_client.py "what is AI?" --sync
    python test_client.py --status
"""

import io, sys
# Force UTF-8 on Windows (avoids cp1252 UnicodeEncodeError)
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

import argparse
import json
import time
import urllib.request
import urllib.error

BASE = "http://localhost:3000"


def _request(method: str, path: str, body=None):
    url  = BASE + path
    data = json.dumps(body).encode() if body else None
    req  = urllib.request.Request(
        url, data=data, method=method,
        headers={"Content-Type": "application/json"} if data else {},
    )
    try:
        with urllib.request.urlopen(req, timeout=360) as r:
            return json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        return json.loads(e.read().decode())
    except Exception as e:
        print(f"[ERROR] {e}")
        sys.exit(1)


def print_json(data):
    print(json.dumps(data, indent=2, ensure_ascii=False))


def sep(label=""):
    width = 50
    if label:
        print(f"--- {label} " + "-" * (width - len(label) - 5))
    else:
        print("-" * width)


def cmd_status():
    sep("API Status")
    print_json(_request("GET", "/api/status"))


def cmd_jobs():
    sep("All Jobs")
    print_json(_request("GET", "/api/jobs"))


def cmd_ask_sync(prompt: str):
    sep(f"Sync Ask: '{prompt[:40]}'")
    print("[*] Sending request (this may take 60-120 seconds)...")
    result = _request("POST", "/api/ask/sync", {"prompt": prompt})
    print_json(result)
    if result.get("result"):
        sep("ChatGPT Response")
        print(result["result"])


def cmd_ask_async(prompt: str):
    sep(f"Async Ask: '{prompt[:40]}'")
    job    = _request("POST", "/api/ask", {"prompt": prompt})
    job_id = job.get("jobId")
    print(f"[*] Job created: {job_id}")
    poll_url = f"/api/logs/{job_id}"
    print(f"[*] Polling {poll_url} ...\n")

    while True:
        time.sleep(4)
        state     = _request("GET", poll_url)
        status    = state.get("status")
        log_count = len(state.get("logs", []))
        print(f"    status={status:<10} logs={log_count}")

        if status not in ("running",):
            sep("Final Result")
            print_json(state)
            if state.get("result"):
                sep("ChatGPT Response")
                print(state["result"])
            break


def main():
    p = argparse.ArgumentParser(description="ChatGPT API test client")
    p.add_argument("prompt",   nargs="?", default=None, help="Prompt to send")
    p.add_argument("--sync",   action="store_true",     help="Use sync endpoint")
    p.add_argument("--status", action="store_true",     help="Show API status")
    p.add_argument("--jobs",   action="store_true",     help="List all jobs")
    args = p.parse_args()

    if args.status:
        cmd_status()
    elif args.jobs:
        cmd_jobs()
    elif args.prompt:
        if args.sync:
            cmd_ask_sync(args.prompt)
        else:
            cmd_ask_async(args.prompt)
    else:
        p.print_help()


if __name__ == "__main__":
    main()
