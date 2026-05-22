#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import io, sys
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

"""
cli_browser.py  v4 — FAST parallel edition
══════════════════════════════════════════════════════════════════
Same g4f approach but ALL providers fire at once in threads.
First valid response wins — no waiting for failed ones.
Human-like jitter removed (it was adding 1-2s of pure sleep).
══════════════════════════════════════════════════════════════════
"""

import argparse
import json
import time
import warnings
import concurrent.futures
warnings.filterwarnings("ignore")

def log(msg):
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)

# Phrases that indicate a login wall or error — reject these
LOGIN_WALL_PHRASES = [
    "log in to", "please log", "sign in to", "create an account",
    "subscribe to", "free trial", "pricing", "unauthorized",
    "you.com/signin", "you.com/pricing",
]

def _is_login_wall(text: str) -> bool:
    t = text.lower()
    return any(p in t for p in LOGIN_WALL_PHRASES)

def _is_valid(text: str) -> bool:
    return bool(text and text.strip() and len(text.strip()) > 20 and not _is_login_wall(text))


# Full catalogue — used in 'auto' mode
PROVIDER_CATALOGUE_ALL = [
    ("PollinationsAI",       "PollinationsAI",        "openai"),
    ("Yqcloud",              "Yqcloud",               None),
    ("GizAI",                "GizAI",                 None),
    ("Blackbox",             "Blackbox",              None),
    ("DeepInfra",            "DeepInfra",             "meta-llama/Meta-Llama-3.1-8B-Instruct"),
    ("HuggingFaceInference", "HuggingFaceInference",  "mistralai/Mistral-7B-Instruct-v0.2"),
]

# ChatGPT only — PollinationsAI with openai model
PROVIDER_CATALOGUE_CHATGPT = [
    ("PollinationsAI", "PollinationsAI", "openai"),
]

def get_catalogue(provider: str):
    if provider == "auto":
        return PROVIDER_CATALOGUE_ALL
    return PROVIDER_CATALOGUE_CHATGPT  # default: chatgpt



def _try_provider(name: str, provider_attr: str, model, prompt: str, timeout: int = 30) -> dict:
    """Try ONE g4f provider — returns result dict. Called from thread."""
    try:
        import g4f.Provider as Providers
        from g4f.client import Client

        prov = getattr(Providers, provider_attr, None)
        if prov is None:
            return {"success": False, "error": f"Provider {provider_attr} not found"}

        client = Client(provider=prov)
        kwargs = {
            "messages": [{"role": "user", "content": prompt}],
            "timeout":  timeout,
        }
        if model:
            kwargs["model"] = model

        response = client.chat.completions.create(**kwargs)
        text = (response.choices[0].message.content or "").strip()

        if _is_valid(text):
            log(f"[{name}] ✓ {len(text)} chars")
            return {"success": True, "response": text, "provider": f"g4f/{name}"}
        elif _is_login_wall(text):
            return {"success": False, "error": "login wall"}
        else:
            return {"success": False, "error": "empty response"}

    except Exception as e:
        err = str(e)[:100]
        log(f"[{name}] ✗ {err}")
        return {"success": False, "error": err}


# ══════════════════════════════════════════════════════════════════
#  CliBrowser — parallel version with auto-fallback
# ══════════════════════════════════════════════════════════════════
class CliBrowser:
    def ask(self, prompt: str, timeout: int = 35, provider: str = 'chatgpt') -> dict:
        result = self._try_catalogue(get_catalogue(provider), prompt, timeout, provider)
        # If primary failed and wasn't already 'auto', try all providers
        if not result.get("success") and provider != 'auto':
            log("[CLI] Primary failed — falling back to all providers")
            result = self._try_catalogue(PROVIDER_CATALOGUE_ALL, prompt, timeout, 'auto-fallback')
        return result

    def _try_catalogue(self, catalogue, prompt: str, timeout: int, label: str) -> dict:
        log(f"[CLI] Mode={label} | Firing {len(catalogue)} provider(s) in parallel ...")
        t0 = time.time()

        with concurrent.futures.ThreadPoolExecutor(max_workers=max(len(catalogue), 1)) as ex:
            futures = {
                ex.submit(_try_provider, name, attr, model, prompt, timeout): name
                for name, attr, model in catalogue
            }
            for future in concurrent.futures.as_completed(futures, timeout=timeout + 5):
                name = futures[future]
                try:
                    result = future.result()
                    if result.get("success"):
                        log(f"[CLI] Winner: {name} in {time.time()-t0:.2f}s")
                        return result
                except Exception as e:
                    log(f"[{name}] thread error: {e}")

        return {
            "success":  False,
            "response": "",
            "error":    "All providers failed. Check internet or try again.",

        }


# ══════════════════════════════════════════════════════════════════
#  CLI entry point
# ══════════════════════════════════════════════════════════════════
def main():
    p = argparse.ArgumentParser(description="CLI Browser — parallel g4f")
    p.add_argument("prompt",     help="Question to ask")
    p.add_argument("--json",     action="store_true", help="Output as RESULT::JSON")
    p.add_argument("--job-id",   default="cli",       help="Job ID (bridge mode)")
    p.add_argument("--provider", default="chatgpt",   help="chatgpt | auto")
    args = p.parse_args()

    browser = CliBrowser()
    result  = browser.ask(args.prompt, provider=args.provider)

    if args.json:
        print(f"RESULT::{json.dumps(result, ensure_ascii=False)}", flush=True)
    else:
        if result["success"]:
            print("\n" + "="*60)
            print(f"[{result.get('provider','?')}] Response:")
            print("="*60)
            print(result["response"])
            print("="*60)
        else:
            print(f"\n[FAILED] {result.get('error', 'Unknown error')}")


if __name__ == "__main__":
    main()
