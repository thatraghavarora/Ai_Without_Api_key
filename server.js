/**
 * server.js  –  Local REST API
 * ─────────────────────────────────────────────────────────────────
 *  POST /api/ask       { "prompt": "what is ai" }
 *  GET  /api/status    health-check
 *  GET  /api/logs/:id  fetch logs for a job id
 * ─────────────────────────────────────────────────────────────────
 *  The server spawns chatgpt_bridge.py (Python) which in turn
 *  drives the Playwright automation with human-like behaviour.
 */

import express  from 'express';
import { spawn } from 'child_process';
import path     from 'path';
import { fileURLToPath } from 'url';
import fs       from 'fs';
import crypto   from 'crypto';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const app  = express();
const PORT = process.env.PORT || 3000;

// ── Serve index.html with NO cache (always fresh JS) ───────────
app.get('/', (_req, res) => {
    res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate');
    res.setHeader('Pragma', 'no-cache');
    res.sendFile(path.join(__dirname, 'index.html'));
});
// Static assets (CSS/JS files if any)
app.use(express.static(__dirname));

// ── Store in-progress / completed jobs ──────────────────────────
const jobs = new Map();   // jobId → { status, result, logs, startedAt }

app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// ── CORS (useful if you call from a browser client) ─────────────
app.use((req, res, next) => {
    res.setHeader('Access-Control-Allow-Origin',  '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
    if (req.method === 'OPTIONS') return res.sendStatus(204);
    next();
});

// ── In-memory stores ────────────────────────────────────────────
const sessions = new Map();  // sessionId → { messages, summary, ... }

// ── Status ───────────────────────────────────────────────────────
app.get('/api/status', (_req, res) => {
    res.json({ status: 'online', uptime: process.uptime(), jobs: jobs.size, sessions: sessions.size });
});

// ── Create new session ────────────────────────────────────────────
app.post('/api/session', (_req, res) => {
    const sessionId = crypto.randomUUID();
    sessions.set(sessionId, {
        messages:   [],
        summary:    '',
        createdAt:  new Date().toISOString(),
        lastActive: new Date().toISOString(),
    });
    console.log(`[SESSION] Created ${sessionId}`);
    res.json({ sessionId, createdAt: sessions.get(sessionId).createdAt });
});

// ── Get session info ──────────────────────────────────────────────
app.get('/api/session/:id', (req, res) => {
    const s = sessions.get(req.params.id);
    if (!s) return res.status(404).json({ error: 'Session not found' });
    res.json({
        sessionId:  req.params.id,
        turns:      Math.floor(s.messages.length / 2),
        hasSummary: s.summary.length > 0,
        createdAt:  s.createdAt,
        lastActive: s.lastActive,
    });
});

// ════════════════════════════════════════════════════════════════
//  POST /api/fast  — fast direct call + session context injection
//  Body: { prompt, sessionId? }
// ════════════════════════════════════════════════════════════════
//  POST /api/fast  — parallel race + rolling session
//  Body: { prompt, prevSessionId? }
//  Returns: { success, result, provider, duration, newSessionId }
// ════════════════════════════════════════════════════════════════
app.post('/api/fast', async (req, res) => {
    const { prompt, prevSessionId } = req.body;
    if (!prompt?.trim()) return res.status(400).json({ error: 'prompt required' });

    const t0 = Date.now();
    const p  = prompt.trim();

    // ── Build rolling session context ─────────────────────────────
    const newSessionId = crypto.randomUUID();
    sessions.set(newSessionId, {
        messages: [], summary: '',
        createdAt: new Date().toISOString(), lastActive: new Date().toISOString(),
    });

    let carriedSummary = '';
    if (prevSessionId && sessions.has(prevSessionId)) {
        const prev = sessions.get(prevSessionId);
        carriedSummary = prev.summary ||
            (prev.messages.length >= 2
                ? `User asked: "${prev.messages.at(-2)?.content?.slice(0,120)}". AI said: "${prev.messages.at(-1)?.content?.slice(0,120)}".`
                : '');
        sessions.get(newSessionId).summary = carriedSummary;
        console.log(`[SESSION] Carried summary → ${newSessionId.slice(0,8)}`);
    }

    // Build full prompt with context injected
    const fullPrompt = carriedSummary
        ? `[Context from previous chat, do not mention to user: ${carriedSummary}]\n\n${p}`
        : p;

    console.log(`[FAST] session=${newSessionId.slice(0,8)} | "${p.slice(0,60)}"`);

    try {
        // Use daemon if ready, otherwise fall back to direct HTTP immediately
        const result = _daemonReady
            ? await _callBridge(fullPrompt, 'chatgpt')
            : await _fastQuery(p + (carriedSummary ? `\n[Context: ${carriedSummary}]` : ''));
        const duration = ((Date.now() - t0) / 1000).toFixed(2) + 's';

        if (result.success) {
            const s = sessions.get(newSessionId);
            s.messages.push({ role: 'user',      content: p });
            s.messages.push({ role: 'assistant', content: result.result });
            s.lastActive = new Date().toISOString();
            _microSummarise(newSessionId, p, result.result);
        }

        console.log(`[FAST] Done in ${duration} via ${result.provider || 'unknown'}`);
        res.json({ ...result, duration, newSessionId });
    } catch (e) {
        console.log('[FAST] Error:', e.message);
        res.json({ success: false, result: '', error: 'Request failed: ' + e.message, duration: '0s', newSessionId });
    }
});



// ════════════════════════════════════════════════════════════════
//  POST /api/stream  — SSE streaming, tokens appear as generated
//  Body: { prompt, prevSessionId? }
//  Client receives: text/event-stream  data: {"token":"..."}
//                                      data: {"done":true,"newSessionId":"..."}
// ════════════════════════════════════════════════════════════════
app.post('/api/stream', async (req, res) => {
    const { prompt, prevSessionId } = req.body;
    if (!prompt?.trim()) return res.status(400).json({ error: 'prompt required' });

    const p = prompt.trim();

    // SSE headers
    res.setHeader('Content-Type',  'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection',    'keep-alive');
    res.setHeader('X-Accel-Buffering', 'no');
    res.flushHeaders();

    const send = (obj) => res.write(`data: ${JSON.stringify(obj)}\n\n`);

    // Build session + context (same as /api/fast)
    const newSessionId = crypto.randomUUID();
    sessions.set(newSessionId, { messages:[], summary:'', createdAt: new Date().toISOString(), lastActive: new Date().toISOString() });

    let carriedSummary = '';
    if (prevSessionId && sessions.has(prevSessionId)) {
        const prev = sessions.get(prevSessionId);
        carriedSummary = prev.summary ||
            (prev.messages.length >= 2
                ? `User asked: "${prev.messages.at(-2)?.content?.slice(0,120)}". AI said: "${prev.messages.at(-1)?.content?.slice(0,120)}".`
                : '');
        sessions.get(newSessionId).summary = carriedSummary;
    }

    const messages = [];
    if (carriedSummary) messages.push({ role:'system', content:`Context (do NOT mention to user): ${carriedSummary}` });
    messages.push({ role:'user', content: p });

    console.log(`[STREAM] session=${newSessionId.slice(0,8)} | "${p.slice(0,60)}"`);

    let fullText = '';
    let streamed = false;
    const t0 = Date.now();

    // ── Try PollinationsAI streaming first ────────────────────────
    try {
        const resp = await fetch('https://text.pollinations.ai/openai', {
            method:  'POST',
            headers: { 'Content-Type': 'application/json' },
            body:    JSON.stringify({ model:'openai', messages, stream:true }),
            signal:  AbortSignal.timeout(30000),
        });

        if (resp.ok && resp.body) {
            const reader  = resp.body.getReader();
            const decoder = new TextDecoder();
            let   buffer  = '';

            while (true) {
                const { done, value } = await reader.read();
                if (done) break;
                buffer += decoder.decode(value, { stream: true });

                const lines = buffer.split('\n');
                buffer = lines.pop(); // keep incomplete last line

                for (const line of lines) {
                    if (!line.startsWith('data: ')) continue;
                    const raw = line.slice(6).trim();
                    if (raw === '[DONE]') continue;
                    try {
                        const chunk = JSON.parse(raw);
                        const token = chunk?.choices?.[0]?.delta?.content;
                        if (token) {
                            fullText += token;
                            send({ token });
                            streamed = true;
                        }
                    } catch {}
                }
            }
        }
    } catch (e) { console.log('[STREAM] Pollinations stream failed:', e.message); }

    // ── Fallback: if streaming failed, use parallel non-stream ────
    if (!streamed || fullText.length < 3) {
        console.log('[STREAM] Falling back to parallel non-stream');
        try {
            const r = await _fastQuery(messages);
            if (r.success) {
                // Send full text in chunks so client still animates it
                const words = r.result.split(' ');
                for (const word of words) {
                    send({ token: word + ' ' });
                    fullText += word + ' ';
                    await new Promise(resolve => setTimeout(resolve, 12)); // tiny delay per word
                }
            }
        } catch (e) { console.log('[STREAM] Fallback failed:', e.message); }
    }

    const dur = ((Date.now() - t0) / 1000).toFixed(2) + 's';

    // ── Save session + micro-summary ──────────────────────────────
    if (fullText.trim()) {
        const s = sessions.get(newSessionId);
        s.messages.push({ role:'user', content:p });
        s.messages.push({ role:'assistant', content:fullText.trim() });
        s.lastActive = new Date().toISOString();
        _microSummarise(newSessionId, p, fullText.trim());
    }

    // ── Signal done ───────────────────────────────────────────────
    send({ done: true, newSessionId, duration: dur, provider: 'PollinationsAI-stream' });
    console.log(`[STREAM] Done in ${dur} — ${fullText.length} chars`);
    res.end();
});

// ── Build summary locally — no network call, never fails ─────────
function _microSummarise(sessionId, userMsg, aiMsg) {
    if (!sessions.has(sessionId)) return;
    // Simple local summary: first 80 chars of Q + first 80 chars of A
    const q = userMsg.slice(0, 80).replace(/\n/g, ' ');
    const a = aiMsg.slice(0, 120).replace(/\n/g, ' ');
    const summary = `User asked about "${q}". AI explained: "${a}".`;
    sessions.get(sessionId).summary = summary;
    console.log(`[SESSION] ${sessionId.slice(0,8)} summary saved (local)`);
}

// ── Persistent Python daemon (started once, reused for all requests) ──
let   _daemon      = null;
let   _daemonReady = false;
const _pending     = new Map();   // reqId → { resolve, reject, timer }
let   _daemonBuf   = '';

function _startDaemon() {
    const pythonCmd = process.platform === 'win32' ? 'python' : 'python3';
    _daemon = spawn(pythonCmd, [
        path.join(__dirname, 'bridge_daemon.py'),
    ], { cwd: __dirname, env: { ...process.env, PYTHONIOENCODING: 'utf-8' } });

    _daemon.stdout.on('data', chunk => {
        _daemonBuf += chunk.toString();
        const lines = _daemonBuf.split('\n');
        _daemonBuf  = lines.pop();

        for (const rawLine of lines) {
            const line = rawLine.trimEnd();   // strip \r on Windows
            if (line.trim() === 'DAEMON_READY') {
                _daemonReady = true;
                console.log('[DAEMON] Ready — g4f pre-loaded');
                continue;
            }
            if (line.startsWith('RESULT::')) {
                // Format: RESULT::<reqId>::<json>
                const rest    = line.slice(8);
                const sepIdx  = rest.indexOf('::');
                if (sepIdx === -1) { console.log('[DAEMON] Bad RESULT line:', line.slice(0,60)); continue; }
                const reqId   = rest.slice(0, sepIdx);
                const jsonStr = rest.slice(sepIdx + 2).trim();
                const entry   = _pending.get(reqId);
                if (entry) {
                    clearTimeout(entry.timer);
                    _pending.delete(reqId);
                    try {
                        const d = JSON.parse(jsonStr);
                        entry.resolve({
                            success:  d.success,
                            result:   d.response || '',
                            provider: d.provider || 'g4f',
                            error:    d.error || null,
                        });
                    } catch(e) { entry.reject(new Error('Parse error: ' + e.message + ' | json: ' + jsonStr.slice(0,50))); }
                } else {
                    console.log('[DAEMON] No pending entry for reqId:', reqId.slice(0,8));
                }
            }
        }
    });

    _daemon.stderr.on('data', () => {});  // discard daemon stderr (g4f logs)

    _daemon.on('close', code => {
        console.log(`[DAEMON] Exited (code ${code}) — restarting in 2s`);
        _daemonReady = false;
        _daemon      = null;
        setTimeout(_startDaemon, 2000);
    });

    _daemon.on('error', e => console.error('[DAEMON] Spawn error:', e.message));
}

// Send a request to the daemon; returns Promise<result>
function _callBridge(prompt, provider = 'chatgpt') {
    return new Promise((resolve, reject) => {
        if (!_daemon || !_daemonReady) {
            return reject(new Error('Daemon not ready yet — try again in a moment'));
        }
        const reqId = crypto.randomUUID();
        const req   = JSON.stringify({ reqId, prompt, provider }) + '\n';

        const timer = setTimeout(() => {
            _pending.delete(reqId);
            reject(new Error('Request timeout (60s)'));
        }, 60000);

        _pending.set(reqId, { resolve, reject, timer });
        _daemon.stdin.write(req);
    });
}


// ── Extract last user message (for GET-based providers) ───────────
function _lastUserMsg(messages) {
    for (let i = messages.length - 1; i >= 0; i--) {
        if (messages[i].role === 'user') return messages[i].content;
    }
    return messages.at(-1)?.content || '';
}

// ══════════════════════════════════════════════════════════════════
//  _fastQuery — direct PollinationsAI (used when daemon not available)
//  No retries, no delays — Railway has fresh IP, just race endpoints
// ══════════════════════════════════════════════════════════════════
async function _fastQuery(userText) {
    const enc = encodeURIComponent(userText);

    function tryPost(model) {
        return fetch('https://text.pollinations.ai/openai', {
            method:  'POST',
            headers: { 'Content-Type': 'application/json' },
            body:    JSON.stringify({ model, messages: [{ role: 'user', content: userText }], stream: false }),
            signal:  AbortSignal.timeout(20000),
        }).then(async r => {
            if (!r.ok) throw new Error(`status ${r.status}`);
            const d = await r.json();
            const t = d?.choices?.[0]?.message?.content?.trim();
            if (!t || t.length < 3) throw new Error('empty');
            return { success: true, result: t, provider: 'ChatGPT' };
        });
    }

    function tryGet(seed) {
        return fetch(
            `https://text.pollinations.ai/${enc}?model=openai&seed=${seed}&nologo=true`,
            { signal: AbortSignal.timeout(20000) }
        ).then(async r => {
            if (!r.ok) throw new Error(`status ${r.status}`);
            const t = (await r.text()).trim();
            if (!t || t.length < 3 || t.startsWith('<') || t.startsWith('data:')) throw new Error('invalid');
            return { success: true, result: t, provider: 'ChatGPT' };
        });
    }

    try {
        return await Promise.any([
            tryPost('openai'),
            tryPost('openai-large'),
            tryGet(Date.now()),
            tryGet(Math.floor(Math.random() * 99999)),
        ]);
    } catch {
        return { success: false, result: '', error: 'ChatGPT unavailable. Please try again.' };
    }
}


// ────────────────────────────────────────────────────────────────
//  POST /api/ask
//  Body: { prompt: string, headless?: boolean }
// ────────────────────────────────────────────────────────────────
app.post('/api/ask', (req, res) => {
    const { prompt, headless = false } = req.body;

    if (!prompt || typeof prompt !== 'string' || prompt.trim().length === 0) {
        return res.status(400).json({ error: 'prompt is required and must be a non-empty string' });
    }

    // Create a unique job id
    const jobId = crypto.randomUUID();
    const startedAt = new Date().toISOString();

    jobs.set(jobId, {
        status:    'running',
        prompt:    prompt.trim(),
        result:    null,
        logs:      [],
        error:     null,
        startedAt,
        finishedAt: null,
    });

    console.log(`[API] Job ${jobId} created – prompt: "${prompt.slice(0, 60)}…"`);

    // ── Respond immediately with jobId (non-blocking) ───────────
    res.status(202).json({
        jobId,
        status:    'running',
        startedAt,
        pollUrl:   `/api/logs/${jobId}`,
        message:   'Job queued. Poll pollUrl for result.',
    });

    // ── Spawn Python bridge in background ───────────────────────
    _runBridge(jobId, prompt.trim(), headless);
});

// ────────────────────────────────────────────────────────────────
//  GET /api/logs/:jobId  –  poll for result
// ────────────────────────────────────────────────────────────────
app.get('/api/logs/:jobId', (req, res) => {
    const job = jobs.get(req.params.jobId);
    if (!job) return res.status(404).json({ error: 'Job not found' });
    res.json(job);
});

// ────────────────────────────────────────────────────────────────
//  GET /api/jobs  –  list all jobs
// ────────────────────────────────────────────────────────────────
app.get('/api/jobs', (_req, res) => {
    const list = [];
    for (const [id, job] of jobs.entries()) {
        list.push({ jobId: id, status: job.status, prompt: job.prompt, startedAt: job.startedAt });
    }
    res.json(list);
});

// ────────────────────────────────────────────────────────────────
//  POST /api/ask/sync  –  wait and return result directly
//  (blocks until automation completes – handy for simple clients)
// ────────────────────────────────────────────────────────────────
app.post('/api/ask/sync', async (req, res) => {
    const { prompt, headless = false, timeout = 300000 } = req.body;

    if (!prompt || typeof prompt !== 'string' || prompt.trim().length === 0) {
        return res.status(400).json({ error: 'prompt is required' });
    }

    const jobId = crypto.randomUUID();
    jobs.set(jobId, {
        status: 'running', prompt: prompt.trim(),
        result: null, logs: [], error: null,
        startedAt: new Date().toISOString(), finishedAt: null,
    });

    console.log(`[API] Sync job ${jobId} – prompt: "${prompt.slice(0, 60)}…"`);

    _runBridge(jobId, prompt.trim(), headless);

    // Poll until done or timeout
    const deadline = Date.now() + timeout;
    while (Date.now() < deadline) {
        await _sleep(3000);
        const job = jobs.get(jobId);
        if (job.status !== 'running') {
            return res.json(job);
        }
    }

    // Timeout
    const job = jobs.get(jobId);
    job.status = 'timeout';
    return res.status(408).json({ ...job, error: 'Automation timed out' });
});

// ────────────────────────────────────────────────────────────────
//  Internal: spawn Python bridge
// ────────────────────────────────────────────────────────────────
function _runBridge(jobId, prompt, headless) {
    const pythonScript = path.join(__dirname, 'chatgpt_bridge.py');
    const args = [
        pythonScript,
        '--prompt',   prompt,
        '--job-id',   jobId,
        '--headless', headless ? '1' : '0',
    ];

    // Try python3 first, fall back to python
    const pythonCmd = process.platform === 'win32' ? 'python' : 'python3';
    const child     = spawn(pythonCmd, args, {
        cwd: __dirname,
        env: { ...process.env, PYTHONIOENCODING: 'utf-8' },
    });

    let stdout = '';
    let stderr = '';

    child.stdout.on('data', chunk => {
        const txt = chunk.toString();
        stdout += txt;
        // Stream logs in real-time to job store
        txt.split('\n').filter(Boolean).forEach(line => {
            jobs.get(jobId)?.logs.push(line);
        });
    });

    child.stderr.on('data', chunk => {
        stderr += chunk.toString();
    });

    child.on('close', (code) => {
        const job = jobs.get(jobId);
        if (!job) return;

        job.finishedAt = new Date().toISOString();

        if (code === 0) {
            // Python prints a JSON result line prefixed with RESULT::
            // The error message inside may contain newlines, so grab the LAST occurrence
            const resultIdx = stdout.lastIndexOf('RESULT::');
            if (resultIdx !== -1) {
                // Take everything from RESULT:: to the next real newline that closes valid JSON
                const rawPayload = stdout.slice(resultIdx + 8).split('\n')[0].trim();
                try {
                    const parsed = JSON.parse(rawPayload);
                    job.result   = parsed.response  ?? null;
                    job.success  = parsed.success   ?? false;
                    job.status   = job.success ? 'done' : 'failed';
                    job.error    = parsed.error ?? null;
                    if (parsed.logs) job.logs = parsed.logs;
                } catch (e) {
                    job.status = 'failed';
                    job.error  = `JSON parse error: ${e.message}`;
                }
            } else {
                job.status = 'done';
                job.result = stdout.trim();
            }
        } else {
            job.status = 'failed';
            job.error  = stderr.trim() || `Process exited with code ${code}`;
        }

        console.log(`[API] Job ${jobId} finished → ${job.status}`);
    });

    child.on('error', (err) => {
        const job = jobs.get(jobId);
        if (job) { job.status = 'failed'; job.error = err.message; }
        console.error(`[API] Spawn error for job ${jobId}:`, err.message);
    });
}

function _sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ── Start ────────────────────────────────────────────────────────
if (!process.env.VERCEL) {
app.listen(PORT, () => {
    console.log(`
=======================================================
  AI Chat API  |  http://localhost:${PORT}
=======================================================
  POST /api/fast   <- Web UI chat (Python g4f bridge)
  GET  /api/status <- health check
  GET  /           <- Web UI
=======================================================
`);
    // Start persistent daemon (pre-loads g4f once for all requests)
    _startDaemon();
});
}

export default app;
