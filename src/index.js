// memelli-claude-team-shell — RAIL-SIDE INSTANCE (2026-05-13)
// Mirror of the operator's local locked dashboard at 127.0.0.1:7777.
// Operator: Mel (osmemelli@gmail.com)  GUC: 1604
//
// Endpoints:
//   GET  /                 → dashboard HTML
//   GET  /api/health       → liveness
//   GET  /api/status       → live probe of Groq + Anthropic + env presence
//   POST /api/auth         → exchange GUC token for op_session cookie
//   GET  /api/whoami       → identity + auth state
//   POST /api/ping         → master-loop simulation (auth'd; fans Groq slaves)
//   POST /api/groq         → Groq proxy (auth'd)
//   POST /api/claude       → Claude proxy (auth'd; requires ANTHROPIC_API_KEY env)
//   GET  /api/ledger       → ping ledger tail
//   GET  /api/events       → SSE stream
//
// Env vars expected (set at Railway project level, propagate to all services):
//   PORT                   → Railway-injected
//   GROQ_API_KEY           → operator's Groq key
//   ANTHROPIC_API_KEY      → operator's Claude API key (optional; Groq works without)
//   OPERATOR_EMAIL         → defaults to osmemelli@gmail.com
//   GUC_TOKEN              → defaults to 1604

import http from 'node:http';

const HOST = '0.0.0.0';
const PORT = parseInt(process.env.PORT || '3000');
const GROQ_KEY = process.env.GROQ_API_KEY || '';
const ANTHROPIC_KEY = process.env.ANTHROPIC_API_KEY || '';
const OPERATOR_EMAIL = process.env.OPERATOR_EMAIL || 'osmemelli@gmail.com';
const OP_GUC = process.env.GUC_TOKEN || '1604';
const SESSION_COOKIE = 'op_session';
const VERSION = '2.0.0-rail';
const SECRET = 'mel_1604_rail_2026';

const sessions = new Map();
function issueSession() {
  const t = SECRET + '_' + Date.now().toString(36) + '_' + Math.random().toString(36).slice(2, 10);
  sessions.set(t, { issued_at: Date.now(), last_seen: Date.now() });
  return t;
}
function validSession(token) {
  if (!token) return false;
  const s = sessions.get(token);
  if (!s) return false;
  s.last_seen = Date.now();
  return true;
}
function readSession(req) {
  const c = req.headers.cookie || '';
  const m = c.match(new RegExp(`${SESSION_COOKIE}=([^;]+)`));
  if (m) return m[1];
  if (req.headers['x-op-token'] === OP_GUC) return 'GUC_DIRECT';
  return null;
}
function authed(req) {
  const t = readSession(req);
  if (t === 'GUC_DIRECT') return true;
  return validSession(t);
}

// In-memory ledger (Railway containers are ephemeral — DB persistence comes when creds return)
const ledger = [];
function ledgerWrite(row) { ledger.push(row); if (ledger.length > 5000) ledger.shift(); }
const sseClients = new Set();
function sseBroadcast(event, data) {
  const payload = `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
  for (const r of sseClients) { try { r.write(payload); } catch {} }
}
function tid() { return 'pr_' + Date.now().toString(36) + '_' + Math.random().toString(36).slice(2, 8); }

function send(res, status, type, body, extra = {}) {
  res.writeHead(status, { 'Content-Type': type, 'Cache-Control': 'no-store', ...extra });
  res.end(body);
}
function sendJson(res, status, obj, extra = {}) { send(res, status, 'application/json', JSON.stringify(obj), extra); }
function bodyOf(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    req.on('data', c => chunks.push(c));
    req.on('end', () => {
      try { resolve(JSON.parse(Buffer.concat(chunks).toString('utf8') || '{}')); }
      catch (e) { reject(e); }
    });
    req.on('error', reject);
  });
}

async function groqChat(messages, opts = {}) {
  if (!GROQ_KEY) return { ok: false, error: 'GROQ_API_KEY env var not set' };
  const t0 = Date.now();
  try {
    const r = await fetch('https://api.groq.com/openai/v1/chat/completions', {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${GROQ_KEY}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        model: opts.model || 'llama-3.1-8b-instant',
        max_tokens: opts.max_tokens || 600,
        temperature: opts.temperature ?? 0.2,
        messages,
      }),
    });
    const text = await r.text();
    let body; try { body = JSON.parse(text); } catch { body = { raw: text.slice(0, 400) }; }
    return { ok: r.ok, status: r.status, ms: Date.now() - t0, body };
  } catch (e) { return { ok: false, ms: Date.now() - t0, error: e.message }; }
}

async function claudeChat(messages, opts = {}) {
  if (!ANTHROPIC_KEY) return { ok: false, error: 'ANTHROPIC_API_KEY env var not set' };
  const t0 = Date.now();
  try {
    const r = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: { 'x-api-key': ANTHROPIC_KEY, 'anthropic-version': '2023-06-01', 'Content-Type': 'application/json' },
      body: JSON.stringify({
        model: opts.model || 'claude-haiku-4-5-20251001',
        max_tokens: opts.max_tokens || 800,
        messages,
      }),
    });
    const text = await r.text();
    let body; try { body = JSON.parse(text); } catch { body = { raw: text.slice(0, 400) }; }
    return { ok: r.ok, status: r.status, ms: Date.now() - t0, body };
  } catch (e) { return { ok: false, ms: Date.now() - t0, error: e.message }; }
}

const DASHBOARD = `<!doctype html>
<html><head><meta charset="utf-8"><title>Infinity Rail Instance — Claude Team Shell</title>
<style>
:root{--bg:#0a0a0a;--panel:#121211;--line:#1f1f1c;--gold:#D4AF37;--green:#9bc479;--red:#e07a5f;--blue:#7fb0e8;--dim:#888;--fg:#e8e2d0}
*{box-sizing:border-box}body{background:var(--bg);color:var(--fg);font:13px ui-monospace,Menlo,Consolas,monospace;margin:0}
header{display:flex;justify-content:space-between;align-items:center;padding:12px 20px;border-bottom:1px solid var(--line)}
header h1{color:var(--gold);margin:0;font-size:14px}
.panel{background:var(--panel);border:1px solid var(--line);padding:14px;margin:12px}
.panel h2{color:var(--gold);margin:0 0 10px;font-size:12px;text-transform:uppercase}
.row{display:flex;justify-content:space-between;padding:4px 0;border-bottom:1px dashed var(--line)}
.k{color:var(--dim)}.v{color:var(--fg)}.ok{color:var(--green)}.bad{color:var(--red)}
.convo{background:#000;border:1px solid var(--line);padding:10px;max-height:280px;overflow:auto}
.turn{padding:6px 0;border-bottom:1px dashed var(--line)}
.turn .who{color:var(--gold);font-size:11px}.turn.op .who{color:var(--blue)}
.turn .body{color:var(--fg);white-space:pre-wrap}
.chatbox{display:flex;gap:8px;padding-top:10px}
.chatbox input{flex:1;background:#000;color:var(--fg);border:1px solid var(--line);padding:8px;font:inherit}
.chatbox button{background:var(--gold);color:#000;border:0;padding:8px 14px;font:inherit;cursor:pointer}
</style></head><body>
<header><h1>INFINITY RAIL INSTANCE — claude-team-shell · v${VERSION}</h1>
<div><span id="authstate" style="color:var(--red)">UNAUTHED</span> · <button id="authbtn" style="background:var(--gold);color:#000;border:0;padding:4px 10px">UNLOCK</button></div></header>
<section class="panel"><h2>Live Status</h2>
<div class="row"><span class="k">Groq</span><span id="s-groq">…</span></div>
<div class="row"><span class="k">Anthropic Claude</span><span id="s-claude">…</span></div>
<div class="row"><span class="k">Operator</span><span class="v">${OPERATOR_EMAIL}</span></div>
<div class="row"><span class="k">Version</span><span class="v">${VERSION}</span></div>
<div class="row"><span class="k">Sister instance</span><span class="v">local 127.0.0.1:7777 (mel's laptop)</span></div>
</section>
<section class="panel"><h2>PING ENGINE (rail-side; mirrors local cascade)</h2>
<div class="convo" id="pings"></div>
<div class="chatbox"><input id="pingq" placeholder="Ping the rail…"><button id="pingbtn">PING</button></div></section>
<section class="panel"><h2>Groq / Claude Chat</h2>
<div class="convo" id="convo"></div>
<div class="chatbox"><input id="q" placeholder="Ask…"><button id="send">Send</button></div></section>
<script>
const $=s=>document.querySelector(s);
async function whoami(){const j=await fetch('/api/whoami').then(r=>r.json());const e=$('#authstate');if(j.authed){e.textContent='AUTHED '+j.operator;e.style.color='var(--green)';$('#authbtn').style.display='none'}else{e.textContent='UNAUTHED';e.style.color='var(--red)';$('#authbtn').style.display='inline-block'}}
async function status(){const s=await fetch('/api/status').then(r=>r.json());$('#s-groq').textContent=s.groq.ok?'✓ '+s.groq.reply+' · '+s.groq.ms+'ms':'✗ '+(s.groq.error||'down');$('#s-groq').className='v '+(s.groq.ok?'ok':'bad');$('#s-claude').textContent=s.claude.ok?'✓ live · '+s.claude.ms+'ms':'✗ '+(s.claude.error||'no key');$('#s-claude').className='v '+(s.claude.ok?'ok':'bad')}
async function auth(){const t=prompt('GUC');if(!t)return;const r=await fetch('/api/auth',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({token:t})});if(r.ok)whoami();else alert('rejected')}
$('#authbtn').onclick=auth;
function append(id,who,body,k){const d=document.createElement('div');d.className='turn '+(k||'');d.innerHTML='<div class="who">'+who+' '+new Date().toLocaleTimeString()+'</div><div class="body"></div>';d.querySelector('.body').textContent=body;$(id).appendChild(d);$(id).scrollTop=99999}
async function ping(){const q=$('#pingq').value.trim();if(!q)return;$('#pingq').value='';append('#pings','mel.ping ⚡',q,'op');const r=await fetch('/api/ping',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({text:q,fan:4})});const j=await r.json();if(j.error){append('#pings','error',j.error);return}for(const c of(j.children||[])){append('#pings','child.'+c.agent+' '+c.ms+'ms',c.reply||'')}append('#pings','wall '+j.wall_ms+'ms','')}
$('#pingbtn').onclick=ping;$('#pingq').addEventListener('keydown',e=>{if(e.key==='Enter')ping()});
async function send(){const q=$('#q').value.trim();if(!q)return;$('#q').value='';append('#convo','mel',q,'op');const r=await fetch('/api/groq',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({text:q})});const j=await r.json();append('#convo','groq',j.body?.choices?.[0]?.message?.content||JSON.stringify(j).slice(0,300))}
$('#send').onclick=send;$('#q').addEventListener('keydown',e=>{if(e.key==='Enter')send()});
whoami();status();setInterval(status,8000);setInterval(whoami,30000);
</script></body></html>`;

async function handleStatus(res) {
  const [g, c] = await Promise.all([
    GROQ_KEY ? groqChat([{ role: 'system', content: 'one word' }, { role: 'user', content: 'ping' }], { max_tokens: 5 }) : Promise.resolve({ ok: false, error: 'no_key' }),
    ANTHROPIC_KEY ? claudeChat([{ role: 'user', content: 'one word reply: ping' }], { max_tokens: 10 }) : Promise.resolve({ ok: false, error: 'no_key' }),
  ]);
  sendJson(res, 200, {
    at: new Date().toISOString(),
    version: VERSION,
    host: HOST, port: PORT,
    operator: OPERATOR_EMAIL,
    keys_present: { groq: !!GROQ_KEY, anthropic: !!ANTHROPIC_KEY },
    groq: { ok: g.ok, ms: g.ms, reply: g.body?.choices?.[0]?.message?.content?.slice(0, 30) ?? g.error },
    claude: { ok: c.ok, ms: c.ms, error: c.error || null },
    ledger_rows: ledger.length,
    sessions_active: sessions.size,
  });
}

async function handleAuth(req, res) {
  let body; try { body = await bodyOf(req); } catch { body = {}; }
  if (body.token !== OP_GUC) return sendJson(res, 403, { error: 'GUC rejected' });
  const s = issueSession();
  res.writeHead(200, { 'Content-Type': 'application/json', 'Set-Cookie': `${SESSION_COOKIE}=${s}; Path=/; HttpOnly; SameSite=Strict; Max-Age=86400` });
  res.end(JSON.stringify({ ok: true, operator: 'Mel', email: OPERATOR_EMAIL, issued_at: new Date().toISOString() }));
}

async function handleWhoami(req, res) {
  const a = authed(req);
  sendJson(res, 200, { authed: a, operator: a ? 'Mel' : null, email: a ? OPERATOR_EMAIL : null });
}

async function handleGroq(req, res) {
  let body; try { body = await bodyOf(req); } catch (e) { return sendJson(res, 400, { error: e.message }); }
  const msgs = Array.isArray(body.messages) && body.messages.length ? body.messages : [
    { role: 'system', content: 'You are the Memelli rail-side Groq agent. Plain text. Short.' },
    { role: 'user', content: (body.text || '').toString() },
  ];
  const r = await groqChat(msgs, { model: body.model, max_tokens: body.max_tokens, temperature: body.temperature });
  sendJson(res, r.ok ? 200 : 502, r);
}

async function handleClaude(req, res) {
  let body; try { body = await bodyOf(req); } catch (e) { return sendJson(res, 400, { error: e.message }); }
  const msgs = Array.isArray(body.messages) && body.messages.length ? body.messages : [{ role: 'user', content: (body.text || '').toString() }];
  const r = await claudeChat(msgs, { model: body.model, max_tokens: body.max_tokens });
  sendJson(res, r.ok ? 200 : 502, r);
}

async function handlePing(req, res) {
  let body; try { body = await bodyOf(req); } catch { body = {}; }
  const input = (body.text || '').toString().slice(0, 1200);
  if (!input) return sendJson(res, 400, { error: 'send { text }' });
  const fan = Math.max(1, Math.min(8, parseInt(body.fan ?? 4)));
  const trace = tid(); const t0 = Date.now();
  const root = { trace_id: trace, parent: null, kind: 'root', input, fan, at: new Date().toISOString() };
  ledgerWrite(root); sseBroadcast('ping_root', root);
  const missions = [
    { agent: 'router',  prompt: `Operator ping: "${input}". Route in <2 lines.` },
    { agent: 'analyst', prompt: `Operator ping: "${input}". Intent in <2 lines.` },
    { agent: 'builder', prompt: `Operator ping: "${input}". First concrete action <2 lines.` },
    { agent: 'critic',  prompt: `Operator ping: "${input}". Blockers <2 lines.` },
    { agent: 'mirror',  prompt: `Operator ping: "${input}". Echo intent <2 lines.` },
    { agent: 'planner', prompt: `Operator ping: "${input}". 3 steps <4 lines.` },
    { agent: 'mover',   prompt: `Operator ping: "${input}". First endpoint/file <2 lines.` },
    { agent: 'historian',prompt: `Operator ping: "${input}". Related artifact <2 lines.` },
  ].slice(0, fan);
  const children = await Promise.all(missions.map(async m => {
    const cid = tid(); const ct0 = Date.now();
    const r = await groqChat([
      { role: 'system', content: 'Plain text. Short.' },
      { role: 'user', content: m.prompt },
    ], { max_tokens: 200, temperature: 0.2 });
    const reply = r.body?.choices?.[0]?.message?.content?.trim() ?? r.error ?? '';
    const row = { trace_id: cid, parent: trace, kind: 'child', state: r.ok ? 'done' : 'failed', agent: m.agent, ms: Date.now() - ct0, reply, at: new Date().toISOString() };
    ledgerWrite(row); sseBroadcast('ping_child_done', row);
    return row;
  }));
  const wall = Date.now() - t0;
  const sum = { trace_id: trace, kind: 'root_done', wall_ms: wall, fan, children_ok: children.filter(c => c.state === 'done').length, at: new Date().toISOString() };
  ledgerWrite(sum); sseBroadcast('ping_root_done', sum);
  sendJson(res, 200, { trace_id: trace, wall_ms: wall, fan, children });
}

const MUTATING = new Set(['/api/groq', '/api/claude', '/api/ping']);

const server = http.createServer(async (req, res) => {
  const { method, url } = req;
  try {
    if (method === 'GET' && (url === '/' || url === '/index.html')) return send(res, 200, 'text/html; charset=utf-8', DASHBOARD);
    if (method === 'GET' && url === '/api/health') return sendJson(res, 200, { ok: true, version: VERSION, pid: process.pid, at: new Date().toISOString() });
    if (method === 'GET' && url === '/api/status') return handleStatus(res);
    if (method === 'GET' && url === '/api/whoami') return handleWhoami(req, res);
    if (method === 'GET' && url === '/api/ledger') return sendJson(res, 200, { count: ledger.length, rows: ledger.slice(-200) });
    if (method === 'POST' && url === '/api/auth') return handleAuth(req, res);
    if (method === 'GET' && url === '/api/events') {
      res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', 'Connection': 'keep-alive' });
      res.write(`event: hello\ndata: ${JSON.stringify({ at: new Date().toISOString(), pid: process.pid })}\n\n`);
      sseClients.add(res);
      const ka = setInterval(() => { try { res.write(`: ka\n\n`); } catch {} }, 15000);
      req.on('close', () => { sseClients.delete(res); clearInterval(ka); });
      return;
    }
    if (method === 'POST' && MUTATING.has(url)) {
      if (!authed(req)) return sendJson(res, 403, { error: 'operator_auth_required', hint: 'POST /api/auth { token } first' });
      if (url === '/api/groq') return handleGroq(req, res);
      if (url === '/api/claude') return handleClaude(req, res);
      if (url === '/api/ping') return handlePing(req, res);
    }
    send(res, 404, 'text/plain', `unknown route: ${method} ${url}`);
  } catch (e) { sendJson(res, 500, { error: e.message }); }
});

server.listen(PORT, HOST, () => {
  console.log(`[claude-team-shell] v${VERSION} listening ${HOST}:${PORT}  groq=${!!GROQ_KEY}  anthropic=${!!ANTHROPIC_KEY}  operator=${OPERATOR_EMAIL}`);
});
