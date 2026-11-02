// AutoPublish Studio Pro — Cloudflare Worker v3 (PRODUCTION UPGRADE of v2)
// v2 files are left untouched. Deploy v3 ONLY after schema-v2.sql has been
// run and the new secrets in ../.env.example (incl. TOKEN_KEY) are set.
// v3 changes: (1) page-token AES-GCM encryption at rest, (2) real
// transcription via Gemini Files / OpenAI Whisper (no ffmpeg in Worker),
// (3) chunked resumable Drive upload (8 MB chunks, 308 resume, 1 retry).
// No secrets in code. All values come from Worker env (Secrets) — see
// ../.env.example for the full name list. Names only, never values, in logs.
// Replaces fb-auto-publisher-app/worker/worker.js ONLY when redeployed on
// purpose. Do NOT paste this over the live Worker until schema-v2.sql has been
// run and the new secrets below are set. Live v1 deployment stays untouched
// until that redeploy is deliberately done.
// No secrets in code. All values come from Worker env (Secrets) — see
// ../.env.example for the full name list. Names only, never values, in logs.
//
// LOCKED RULES (from the live project, never changed here):
//  - Mother Admin (first account) sets the single shared Drive + global
//    settings once; later pages only Add Page and inherit settings.
//  - Bulk Approve once -> server-side hourly publishing, PC off, no per-video
//    prompts. New videos append AFTER the last scheduled time.
//  - Auto-delete from Drive ONLY after Facebook returns a post/video ID.
//    Failed videos STAY in Drive for retry. History stays in Supabase.
//  - Drive Service Account is DEAD for personal My Drive (403 quota). Primary
//    route is Mother Admin OAuth refresh token. Service account is fallback
//    only (Shared Drives), never primary.
//  - Never claim a publish/AI result succeeded without a real API response.
//  - Hourly Cron writes a heartbeat even with an empty queue (keep-alive).
//
// Env (all optional except SUPABASE_* for DB paths):
//  SUPABASE_URL, SUPABASE_SERVICE_KEY,
//  GOOGLE_OAUTH_CLIENT_ID, GOOGLE_OAUTH_CLIENT_SECRET,
//  GOOGLE_OAUTH_REFRESH_TOKEN, DRIVE_FOLDER_ID,
//  GOOGLE_SERVICE_ACCOUNT_JSON (fallback only),
//  FACEBOOK_APP_ID, FACEBOOK_APP_SECRET,
//  GEMINI_API_KEY, OPENAI_API_KEY, AI_PROVIDER (gemini|openai), AI_MODEL,
//  TOKEN_KEY (required in v3 for POST /pages — page-token encryption key)
//
// Cron: every hour (Cloudflare Cron Trigger). Also handles 30m/2h/3h/6h
// schedules by checking publishing_queue.scheduled_at on every tick — the
// tick interval is the floor, the queue's scheduled_at is the truth.

const B64 = (buf) => { let s=''; const b=new Uint8Array(buf); for (let i=0;i<b.length;i++) s+=String.fromCharCode(b[i]); return btoa(s).replace(/\+/g,'-').replace(/\//g,'_').replace(/=+$/,''); };
const sb = (env, path, opts={}) => fetch(`${env.SUPABASE_URL}/rest/v1/${path}`, { ...opts, headers: { apikey: env.SUPABASE_SERVICE_KEY, Authorization: `Bearer ${env.SUPABASE_SERVICE_KEY}`, 'Content-Type': 'application/json', Prefer: 'return=representation', ...(opts.headers||{}) } });
const esc = (v) => String(v).replace(/&/g,'&amp;').replace(/</g,'&lt;');
const MAX_ATTEMPTS = 5;
const backoffMs = (attempt) => Math.min(60*60*1000, Math.pow(2, attempt) * 60*1000); // 2m,4m,8m,16m,32m -> capped 1h


// ---------- v3: Page-token encryption at rest (AES-GCM, env TOKEN_KEY) ----------
// Format: enc:v1:<base64url(iv 12 bytes)>:<base64url(ciphertext+tag)>
// Key derivation: SHA-256(TOKEN_KEY) — TOKEN_KEY itself is never stored in
// Supabase, never returned by any endpoint, and never logged. If TOKEN_KEY
// is missing, POST /pages refuses to store a token (fail closed) rather than
// silently storing plaintext. Tokens stored by v1/v2 without the prefix are
// legacy: they are still usable (backward compat) but every use is logged
// as needing re-add, so the owner can re-add the page and get it encrypted.
const B64URL_DEC = (s) => { const b = s.replace(/-/g,'+').replace(/_/g,'/'); const pad = '='.repeat((4 - b.length % 4) % 4); return Uint8Array.from(atob(b + pad), c => c.charCodeAt(0)); };
async function tokenKey(env) {
  if (!env.TOKEN_KEY) throw new Error('Page-token encryption not configured: set TOKEN_KEY in Worker secrets (see .env.example). No token was stored.');
  const digest = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(env.TOKEN_KEY));
  return crypto.subtle.importKey('raw', digest, { name: 'AES-GCM' }, false, ['encrypt', 'decrypt']);
}
async function encryptPageToken(env, plain) {
  const key = await tokenKey(env);
  const iv = crypto.getRandomValues(new Uint8Array(12));
  const ct = await crypto.subtle.encrypt({ name: 'AES-GCM', iv }, key, new TextEncoder().encode(plain));
  return `enc:v1:${B64(iv)}:${B64(ct)}`;
}
async function decryptPageToken(env, stored) {
  if (!stored) return { token: null, legacy: false };
  if (!String(stored).startsWith('enc:v1:')) return { token: stored, legacy: true };
  // Format is enc:v1:<iv>:<ct> — split gives 4 parts: ['enc','v1',iv,ct].
  const parts = String(stored).split(':');
  if (parts.length !== 4 || parts[0] !== 'enc' || parts[1] !== 'v1') throw new Error('Stored page token has an invalid encrypted format — re-add the page.');
  const key = await tokenKey(env);
  const pt = await crypto.subtle.decrypt({ name: 'AES-GCM', iv: B64URL_DEC(parts[2]) }, key, B64URL_DEC(parts[3]));
  return { token: new TextDecoder().decode(pt), legacy: false };
}

async function logError(env, source, message, videoId, userId) {
  try { await sb(env, 'error_logs', { method:'POST', headers:{Prefer:'return=minimal'}, body: JSON.stringify({ source, message: String(message).slice(0,500), user_id: userId||null, video_id: videoId||null }) }); } catch {}
}
async function logUsage(env, provider, operation, model, success, err, userId) {
  try { await sb(env, 'api_usage_logs', { method:'POST', headers:{Prefer:'return=minimal'}, body: JSON.stringify({ provider, operation, model: model||null, success, error: err?String(err).slice(0,300):null, user_id: userId||null }) }); } catch {}
}

// ---------- Drive token: Mother OAuth first, service account fallback only ----------
async function driveToken(env) {
  if (env.GOOGLE_OAUTH_REFRESH_TOKEN && env.GOOGLE_OAUTH_CLIENT_ID && env.GOOGLE_OAUTH_CLIENT_SECRET) {
    const r = await fetch('https://oauth2.googleapis.com/token', { method:'POST', headers:{'Content-Type':'application/x-www-form-urlencoded'}, body: `grant_type=refresh_token&refresh_token=${encodeURIComponent(env.GOOGLE_OAUTH_REFRESH_TOKEN)}&client_id=${encodeURIComponent(env.GOOGLE_OAUTH_CLIENT_ID)}&client_secret=${encodeURIComponent(env.GOOGLE_OAUTH_CLIENT_SECRET)}` });
    const j = await r.json(); if (!j.access_token) throw new Error('Drive OAuth token failed: ' + JSON.stringify(j).slice(0,200)); return j.access_token;
  }
  if (!env.GOOGLE_SERVICE_ACCOUNT_JSON) throw new Error('Drive not connected: set GOOGLE_OAUTH_* secrets (see /drive-auth-start)');
  const sa = JSON.parse(env.GOOGLE_SERVICE_ACCOUNT_JSON);
  const now = Math.floor(Date.now()/1000);
  const header = B64(new TextEncoder().encode(JSON.stringify({alg:'RS256',typ:'JWT'})));
  const claim = B64(new TextEncoder().encode(JSON.stringify({iss: sa.client_email, scope: 'https://www.googleapis.com/auth/drive', aud: 'https://oauth2.googleapis.com/token', exp: now+3600, iat: now})));
  const pem = sa.private_key.replace(/-----.*-----/g,'').replace(/\s/g,'');
  const der = Uint8Array.from(atob(pem), c=>c.charCodeAt(0));
  const key = await crypto.subtle.importKey('pkcs8', der, {name:'RSASSA-PKCS1-v1_5', hash:'SHA-256'}, false, ['sign']);
  const sig = await crypto.subtle.sign('RSASSA-PKCS1-v1_5', key, new TextEncoder().encode(`${header}.${claim}`));
  const r = await fetch('https://oauth2.googleapis.com/token', { method:'POST', headers:{'Content-Type':'application/x-www-form-urlencoded'}, body: `grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer&assertion=${header}.${claim}.${B64(sig)}` });
  const j = await r.json(); if (!j.access_token) throw new Error('Drive token failed'); return j.access_token;
}

// ---------- Supabase Auth proxy (frontend never sees service key) ----------
async function goTrue(env, path, body) {
  const r = await fetch(`${env.SUPABASE_URL}/auth/v1/${path}`, { method:'POST', headers:{ apikey: env.SUPABASE_SERVICE_KEY, 'Content-Type':'application/json' }, body: JSON.stringify(body) });
  let j = null; try { j = await r.json(); } catch {}
  return { ok: r.ok, status: r.status, data: j || {} };
}
function goTrueError(d) { return d.msg || d.message || d.error_description || (typeof d.error === 'string' ? d.error : (d.error && d.error.message)) || 'Authentication failed'; }
async function verifySession(env, request) {
  const h = request.headers.get('Authorization') || '';
  const m = h.match(/^Bearer\s+(.+)$/i); if (!m) return null;
  const r = await fetch(`${env.SUPABASE_URL}/auth/v1/user`, { headers:{ apikey: env.SUPABASE_SERVICE_KEY, Authorization:`Bearer ${m[1]}` } });
  if (!r.ok) return null;
  const u = await r.json(); if (!u || !u.id) return null;
  return { id: u.id, email: u.email || null };
}
async function profileRole(env, userId) {
  try { const r = await sb(env, `profiles?select=role&user_id=eq.${encodeURIComponent(userId)}&limit=1`); if (!r.ok) return null; const rows = await r.json(); return Array.isArray(rows) && rows[0] ? rows[0].role : null; } catch { return null; }
}
async function ensureProfile(env, userId, email) {
  const existing = await profileRole(env, userId); if (existing) return existing;
  let role = 'uploader';
  try { const r = await sb(env, 'profiles?select=user_id&limit=1'); const rows = r.ok ? await r.json() : null; if (!rows || !rows.length) role = 'admin'; } catch {}
  await sb(env, 'profiles', { method:'POST', headers:{ Prefer:'return=representation,resolution=merge-duplicates' }, body: JSON.stringify({ user_id: userId, email: email || null, role }) });
  return role;
}
async function schedulerPaused(env) {
  try { const r = await sb(env, 'settings?select=scheduler_paused&limit=1'); if (!r.ok) return false; const rows = await r.json(); return !!(rows[0] && rows[0].scheduler_paused); } catch { return false; }
}
async function scheduleGap(env) {
  try { const r = await sb(env, 'settings?select=schedule_gap_minutes&limit=1'); if (!r.ok) return 60; const rows = await r.json(); const g = rows[0] && rows[0].schedule_gap_minutes; return (typeof g === 'number' && g > 0) ? g : 60; } catch { return 60; }
}

// ---------- AI provider manager: Gemini + OpenAI, transcript is source of truth ----------
// HONEST LIMIT, documented in README: a Worker cannot run ffmpeg, so it cannot
// extract audio from a video binary by itself. /ai-generate therefore requires
// a transcript (pasted, or produced by an external transcription step) and
// generates title/description/caption/hashtags FROM THAT TRANSCRIPT ONLY.
// It never invents content and never returns success without a real API reply.
// If no provider key is set, it returns a clear 503 — never fake content.
function aiConfig(env) {
  const provider = (env.AI_PROVIDER || 'gemini').toLowerCase();
  if (provider === 'openai') return { provider, key: env.OPENAI_API_KEY || null, model: env.AI_MODEL || 'gpt-4o-mini' };
  return { provider: 'gemini', key: env.GEMINI_API_KEY || null, model: env.AI_MODEL || 'gemini-2.5-flash' };
}
function aiPrompt(transcript, language) {
  return `You are writing Facebook publishing copy from a real video transcript. Use ONLY facts present in the transcript. Do not invent people, places, events, or claims not in the transcript. If the transcript is unclear, keep the copy generic about the visible topic, never fabricated. Language preference: ${language || 'English only'}. Transcript:\n"""${transcript.slice(0, 12000)}"""\n\nReturn ONLY valid JSON with keys: title (short, natural, no clickbait), description (2-4 sentences: hook + what the viewer will see/learn + call to action), caption (1-2 engaging sentences for Facebook), hashtags (array of 3-6 relevant tags without the # sign). No markdown, no extra keys.`;
}
async function aiGenerate(env, transcript, language) {
  const cfg = aiConfig(env);
  if (!cfg.key) throw new Error('AI provider not configured: set GEMINI_API_KEY or OPENAI_API_KEY in Worker secrets (see .env.example). No content was generated.');
  if (!transcript || !transcript.trim()) throw new Error('AI generation needs a transcript first. Paste a transcript or run transcription, then regenerate.');
  const prompt = aiPrompt(transcript, language);
  if (cfg.provider === 'openai') {
    const r = await fetch('https://api.openai.com/v1/chat/completions', { method:'POST', headers:{ Authorization:`Bearer ${cfg.key}`, 'Content-Type':'application/json' }, body: JSON.stringify({ model: cfg.model, messages:[{role:'user', content: prompt}], temperature: 0.7 }) });
    const j = await r.json(); const text = j.choices && j.choices[0] && j.choices[0].message && j.choices[0].message.content;
    if (!r.ok || !text) throw new Error('OpenAI generation failed: ' + JSON.stringify(j).slice(0,300));
    return { cfg, parsed: JSON.parse(text.replace(/^```json|```$/gm,'').trim()), usage: j.usage || null };
  }
  const r = await fetch(`https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(cfg.model)}:generateContent?key=${encodeURIComponent(cfg.key)}`, { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ contents:[{ parts:[{ text: prompt }] }] }) });
  const j = await r.json(); const text = j.candidates && j.candidates[0] && j.candidates[0].content && j.candidates[0].content.parts && j.candidates[0].content.parts[0] && j.candidates[0].content.parts[0].text;
  if (!r.ok || !text) throw new Error('Gemini generation failed: ' + JSON.stringify(j).slice(0,300));
  return { cfg, parsed: JSON.parse(text.replace(/^```json|```$/gm,'').trim()), usage: j.usageMetadata || null };
}
function validateAi(parsed) {
  if (!parsed || typeof parsed !== 'object') throw new Error('AI returned an invalid result');
  const title = String(parsed.title || '').trim(); const description = String(parsed.description || '').trim();
  if (!title || !description) throw new Error('AI returned an incomplete result (missing title or description)');
  const hashtags = Array.isArray(parsed.hashtags) ? parsed.hashtags.map(h=>String(h).replace(/^#/,'').trim()).filter(Boolean).slice(0,6) : [];
  return { title, description, caption: String(parsed.caption || '').trim(), hashtags: hashtags.join(',') };
}


// ---------- v3: Real transcription — NO ffmpeg in a Worker ----------
// Why no ffmpeg: a Cloudflare Worker has no shell, no binary execution and
// no local filesystem for ffmpeg. The video is therefore NOT decoded here.
// Instead the Worker (a) downloads the video from Drive (driveToken), then
// (b) hands the file to an external transcription service that accepts video
// directly: Gemini Files API (upload -> poll ACTIVE -> generateContent with
// fileData) or OpenAI Whisper (multipart transcriptions, language auto).
// If neither GEMINI_API_KEY nor OPENAI_API_KEY is set, or Drive is not
// connected, /transcribe returns a clear 503 naming the missing secret —
// it NEVER invents a transcript. See README-V3.md for limits (Worker
// request time, file size, polling cap).
async function fetchDriveVideo(env, driveFileId) {
  const dt = await driveToken(env); // throws a clear Drive-not-connected error
  const r = await fetch(`https://www.googleapis.com/drive/v3/files/${driveFileId}?alt=media`, { headers: { Authorization: `Bearer ${dt}` } });
  if (!r.ok) throw new Error('Drive download failed for transcription: HTTP ' + r.status);
  return { bytes: await r.arrayBuffer(), token: dt };
}
function sleep(ms) { return new Promise(res => setTimeout(res, ms)); }
async function transcribeWithGemini(env, bytes, mimeType) {
  const key = env.GEMINI_API_KEY;
  // Step 1: resumable upload session to Gemini Files API.
  const start = await fetch(`https://generativelanguage.googleapis.com/upload/v1beta/files?key=${encodeURIComponent(key)}`, {
    method: 'POST',
    headers: { 'X-Goog-Upload-Protocol': 'resumable', 'X-Goog-Upload-Command': 'start', 'X-Goog-Upload-Header-Content-Length': String(bytes.byteLength), 'X-Goog-Upload-Header-Content-Type': mimeType || 'video/mp4', 'Content-Type': 'application/json' },
    body: JSON.stringify({ file: { displayName: 'autopublish-transcribe' } })
  });
  const uploadUrl = start.headers.get('X-Goog-Upload-URL');
  if (!uploadUrl) throw new Error('Gemini Files upload session failed: ' + (await start.text()).slice(0, 300));
  // Step 2: upload bytes + finalize.
  const up = await fetch(uploadUrl, { method: 'POST', headers: { 'X-Goog-Upload-Command': 'upload, finalize', 'Content-Length': String(bytes.byteLength), 'Content-Type': mimeType || 'video/mp4' }, body: bytes });
  const upJson = await up.json().catch(() => ({}));
  if (!up.ok || !upJson.file || !upJson.file.name) throw new Error('Gemini Files upload failed: ' + JSON.stringify(upJson).slice(0, 300));
  const fileName = upJson.file.name; // e.g. files/abc123
  // Step 3: poll until ACTIVE (cap: 30 x 2s = 60s; Worker time limits apply).
  let fileUri = upJson.file.uri || null, state = upJson.file.state || 'PROCESSING';
  for (let i = 0; i < 30 && state !== 'ACTIVE'; i++) {
    await sleep(2000);
    const chk = await fetch(`https://generativelanguage.googleapis.com/v1beta/${fileName}?key=${encodeURIComponent(key)}`);
    const cj = await chk.json().catch(() => ({}));
    state = cj.state || state; fileUri = cj.uri || fileUri;
    if (cj.state === 'FAILED') throw new Error('Gemini file processing failed for transcription.');
  }
  if (state !== 'ACTIVE' || !fileUri) throw new Error('Gemini file did not become ACTIVE within the polling cap — retry /transcribe (the file may still activate).');
  // Step 4: ask for a VERBATIM transcript, auto-detecting language.
  const prompt = 'Transcribe ALL speech in this video verbatim, word for word. Do not summarise, do not translate, do not add words that were not spoken. Detect the spoken language automatically: it may be Bengali, English, or mixed Bengali-English — report it exactly. If speech is unclear or inaudible at any point, write [inaudible] there instead of guessing. Return ONLY valid JSON with keys: transcript (the full verbatim text), language (one of: bn, en, bn-en, other), confidence_note (one short sentence, e.g. clear / some parts unclear). No markdown, no extra keys.';
  const gen = await fetch(`https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(env.AI_MODEL && (env.AI_PROVIDER || 'gemini') === 'gemini' ? env.AI_MODEL : 'gemini-2.5-flash')}:generateContent?key=${encodeURIComponent(key)}`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ contents: [{ parts: [{ fileData: { mimeType: mimeType || 'video/mp4', fileUri } }, { text: prompt }] }] })
  });
  const gj = await gen.json().catch(() => ({}));
  const text = gj.candidates && gj.candidates[0] && gj.candidates[0].content && gj.candidates[0].content.parts && gj.candidates[0].content.parts[0] && gj.candidates[0].content.parts[0].text;
  if (!gen.ok || !text) throw new Error('Gemini transcription failed: ' + JSON.stringify(gj).slice(0, 300));
  try { const parsed = JSON.parse(text.replace(/^```json|```$/gm, '').trim()); return { transcript: String(parsed.transcript || ''), language: String(parsed.language || 'auto'), confidence_note: String(parsed.confidence_note || '') }; }
  catch { return { transcript: text, language: 'auto', confidence_note: '' }; } // model returned raw text, not JSON — still a real transcript
}
async function transcribeWithOpenAI(env, bytes, mimeType) {
  // Whisper accepts mp4 directly; omitting `language` = auto-detect.
  const form = new FormData();
  form.append('file', new Blob([bytes], { type: mimeType || 'video/mp4' }), 'video.mp4');
  form.append('model', env.OPENAI_TRANSCRIPTION_MODEL || 'whisper-1');
  form.append('response_format', 'json');
  const r = await fetch('https://api.openai.com/v1/audio/transcriptions', { method: 'POST', headers: { Authorization: `Bearer ${env.OPENAI_API_KEY}` }, body: form });
  const j = await r.json().catch(() => ({}));
  if (!r.ok || !j.text) throw new Error('OpenAI Whisper transcription failed: ' + JSON.stringify(j).slice(0, 300));
  return { transcript: String(j.text), language: String(j.language || 'auto'), confidence_note: '' };
}

// ---------- Facebook: chunked resumable upload (large videos) ----------
async function publishToFacebook(env, page, video, bytes) {
  // Decryption happens ONLY here, at publish time. No other code path
  // (GET /pages, /analytics, logs, frontend) ever sees a plaintext token.
  const dec = await decryptPageToken(env, page.page_token_encrypted);
  if (dec.legacy) console.log('legacy page token used (not enc:v1:) — re-add this page after TOKEN_KEY is set so its token is encrypted at rest');
  const token = dec.token;
  const fbId = page.facebook_page_id;
  if (!token || !fbId) throw new Error('Page not connected: missing page token or page ID');
  const title = video.title || video.file_name || '';
  const description = [video.description, video.hashtags].filter(Boolean).join('\n');
  const start = await (await fetch(`https://graph.facebook.com/v21.0/${fbId}/videos?upload_phase=start&file_size=${bytes.byteLength}&access_token=${encodeURIComponent(token)}`, { method:'POST' })).json();
  if (!start.upload_session_id) throw new Error('FB upload start failed: ' + JSON.stringify(start).slice(0,300));
  const CHUNK = 8 * 1024 * 1024;
  let offset = 0;
  while (offset < bytes.byteLength) {
    const end = Math.min(bytes.byteLength, offset + CHUNK);
    const chunk = bytes.slice(offset, end);
    const up = await fetch(`https://graph.facebook.com/v21.0/${start.upload_session_id}`, { method:'POST', headers:{ Authorization:`OAuth ${token}`, 'Content-Type':'application/octet-stream', 'Content-Range': `bytes ${offset}-${end-1}/${bytes.byteLength}` }, body: chunk });
    const uj = await up.json().catch(()=>({}));
    if (!up.ok && uj.error) throw new Error('FB upload transfer failed: ' + JSON.stringify(uj).slice(0,300));
    offset = end;
  }
  const fin = await (await fetch(`https://graph.facebook.com/v21.0/${fbId}/videos?upload_phase=finish&upload_session_id=${start.upload_session_id}&title=${encodeURIComponent(title)}&description=${encodeURIComponent(description)}&access_token=${encodeURIComponent(token)}`, { method:'POST' })).json();
  const id = fin.video_id || fin.id;
  if (!id) throw new Error('FB finish failed (no video ID returned): ' + JSON.stringify(fin).slice(0,300));
  return id;
}

// ---------- Publisher tick: heartbeat ALWAYS, then ONE due job ----------
async function publisherTick(env) {
  const now = new Date().toISOString();
  // 1. Heartbeat first, even with empty queue / paused scheduler — keep-alive.
  try {
    const dueRes = await sb(env, `publishing_queue?select=id&status=in.(scheduled,retry_pending)&scheduled_at=lte.${now}`);
    const due = dueRes.ok ? await dueRes.json() : [];
    await sb(env, 'heartbeat', { method:'POST', body: JSON.stringify({ checked_at: now, due_count: Array.isArray(due)?due.length:0, note:'hourly publisher tick (Pro v3)' }) });
    await sb(env, `heartbeat?checked_at=lt.${new Date(Date.now()-7*864e5).toISOString()}`, { method:'DELETE' });
  } catch (e) { console.log('heartbeat error', e.message); }
  if (await schedulerPaused(env)) return { ok:true, paused:true };
  const nowIso = new Date().toISOString();
  // 2. Claim ONE due queue row. The status filter IS the job lock: a second
  // run cannot claim a row already moved to 'processing'.
  const qRes = await sb(env, `publishing_queue?select=*,videos(*)&status=in.(scheduled,retry_pending)&scheduled_at=lte.${nowIso}&order=scheduled_at.asc&limit=1`);
  if (!qRes.ok) { await logError(env, 'scheduler', 'publishing_queue read failed: ' + (await qRes.text()).slice(0,300)); return { ok:false }; }
  const jobs = await qRes.json(); if (!jobs.length) return { ok:true, due:0 };
  const job = jobs[0];
  const video = job.videos || {};
  const claim = await sb(env, `publishing_queue?id=eq.${job.id}&status=in.(scheduled,retry_pending)`, { method:'PATCH', body: JSON.stringify({ status:'processing', updated_at: nowIso }) });
  const claimed = await claim.json().catch(()=>[]);
  if (!Array.isArray(claimed) || !claimed.length) return { ok:true, skipped:'already claimed' }; // another run won
  await sb(env, `videos?id=eq.${video.id}`, { method:'PATCH', body: JSON.stringify({ status:'publishing' }) });
  try {
    // 3. Resolve page token from pages (v1) or connected_facebook_pages (v2).
    let page = null;
    if (job.page_id) { const pr = await sb(env, `pages?select=*&id=eq.${job.page_id}`); const ps = pr.ok ? await pr.json() : []; page = ps[0] || null; }
    if (!page && video.page_id) { const pr = await sb(env, `pages?select=*&id=eq.${video.page_id}`); const ps = pr.ok ? await pr.json() : []; page = ps[0] || null; }
    if (!page) { const pr = await sb(env, `connected_facebook_pages?select=*&user_id=eq.${job.user_id}&order=created_at.asc&limit=1`); const ps = pr.ok ? await pr.json() : []; if (ps[0]) page = { facebook_page_id: ps[0].facebook_page_id, page_token_encrypted: ps[0].page_token_encrypted }; }
    if (!page || !page.page_token_encrypted) throw new Error('Page not connected: no page token found for this video');
    if (!video.drive_file_id) throw new Error('Video has no Drive file ID');
    const dt = await driveToken(env);
    const fileRes = await fetch(`https://www.googleapis.com/drive/v3/files/${video.drive_file_id}?alt=media`, { headers:{Authorization:`Bearer ${dt}`} });
    if (!fileRes.ok) throw new Error('Drive download failed: HTTP ' + fileRes.status);
    const bytes = await fileRes.arrayBuffer();
    await sb(env, `publishing_queue?id=eq.${job.id}`, { method:'PATCH', body: JSON.stringify({ status:'publishing' }) });
    const fbId = await publishToFacebook(env, page, video, bytes);
    // 4. ONLY NOW: published + history + THEN delete Drive file. Order matters.
    await sb(env, `publishing_queue?id=eq.${job.id}`, { method:'PATCH', body: JSON.stringify({ status:'published', facebook_post_id: fbId, updated_at: nowIso }) });
    await sb(env, `videos?id=eq.${video.id}`, { method:'PATCH', body: JSON.stringify({ status:'published', facebook_post_id: fbId, published_at: nowIso }) });
    await sb(env, 'publishing_history', { method:'POST', body: JSON.stringify({ video_id: video.id, user_id: job.user_id, page_id: job.page_id || video.page_id || null, facebook_post_id: fbId, facebook_url: `https://www.facebook.com/${fbId}`, status:'published', title_snapshot: video.title || video.file_name || null, published_at: nowIso }) });
    await logUsage(env, 'facebook', 'video_publish', null, true, null, job.user_id);
    const del = await fetch(`https://www.googleapis.com/drive/v3/files/${video.drive_file_id}`, { method:'DELETE', headers:{Authorization:`Bearer ${dt}`} });
    if (!del.ok) await logError(env, 'drive', 'Drive auto-delete after publish failed (video already published, file kept): HTTP ' + del.status, video.id, job.user_id);
    return { ok:true, published: video.id, facebook_post_id: fbId };
  } catch (e) {
    await logError(env, 'facebook', e.message, video.id, job.user_id);
    await logUsage(env, 'facebook', 'video_publish', null, false, e.message, job.user_id);
    const attempt = (job.attempt_count || 0) + 1;
    const permanent = /permission|token|revoked|expired|not connected|OAuthException.*190/i.test(e.message);
    if (permanent || attempt >= MAX_ATTEMPTS) {
      await sb(env, `publishing_queue?id=eq.${job.id}`, { method:'PATCH', body: JSON.stringify({ status:'dead_letter', attempt_count: attempt, last_error: String(e.message).slice(0,500) }) });
      await sb(env, `videos?id=eq.${video.id}`, { method:'PATCH', body: JSON.stringify({ status:'failed', retry_count: attempt, last_error: String(e.message).slice(0,500) }) });
      await sb(env, 'publishing_history', { method:'POST', body: JSON.stringify({ video_id: video.id, user_id: job.user_id, page_id: job.page_id || null, status: permanent ? 'failed' : 'dead_letter', error: String(e.message).slice(0,500) }) });
    } else {
      const next = new Date(Date.now() + backoffMs(attempt)).toISOString();
      await sb(env, `publishing_queue?id=eq.${job.id}`, { method:'PATCH', body: JSON.stringify({ status:'retry_pending', attempt_count: attempt, next_attempt_at: next, last_error: String(e.message).slice(0,500) }) });
      await sb(env, `videos?id=eq.${video.id}`, { method:'PATCH', body: JSON.stringify({ status:'failed', retry_count: attempt, last_error: String(e.message).slice(0,500) }) });
    }
    return { ok:false, error: e.message };
  }
}

// ---------- HTTP API (frontend talks ONLY here, never to Supabase directly) ----------
export default {
  async scheduled(event, env, ctx) { ctx.waitUntil((async()=>{ try { await publisherTick(env); } catch(e){ console.log('tick error', e.message); } })()); },
  async fetch(request, env) {
    const url = new URL(request.url);
    const cors = { 'Access-Control-Allow-Origin':'*', 'Access-Control-Allow-Methods':'GET, POST, PATCH, DELETE, OPTIONS', 'Access-Control-Allow-Headers':'Content-Type, Authorization' };
    const json = (obj, status=200) => new Response(JSON.stringify(obj), { status, headers:{...cors,'Content-Type':'application/json'} });
    if (request.method === 'OPTIONS') return new Response(null, { headers: cors });
    const VIDEO_COLS = 'id,user_id,page_id,drive_file_id,file_name,file_hash,file_size_bytes,mime_type,duration_seconds,resolution,title,description,caption,hashtags,transcript,status,ai_status,ai_confidence,retry_count,last_error,scheduled_at,facebook_post_id,published_at,created_at';
    async function sbList(path) { const r = await sb(env, path); if (!r.ok) throw new Error('Supabase read failed: ' + (await r.text()).slice(0,300)); const rows = await r.json(); if (!Array.isArray(rows)) throw new Error('Supabase read failed: unexpected response'); return rows; }
    async function requireAuth() { const me = await verifySession(env, request); return me; }
    async function requireAdmin() { const me = await verifySession(env, request); if (!me) return null; if ((await profileRole(env, me.id)) !== 'admin') return null; return me; }
    async function bodyJson() { try { return await request.json(); } catch { return null; } }

    // ---- Auth (same contract as v1) ----
    if (url.pathname === '/auth-signup' && request.method === 'POST') {
      const body = await bodyJson(); if (!body) return json({ error:'invalid JSON body' }, 400);
      const email = String(body.email || '').trim(); const password = String(body.password || '');
      if (!/.+@.+\..+/.test(email)) return json({ error:'enter a valid email address' }, 400);
      if (password.length < 6) return json({ error:'password must be at least 6 characters' }, 400);
      const res = await goTrue(env, 'signup', { email, password });
      if (!res.ok) return json({ error: goTrueError(res.data) }, res.status === 422 ? 400 : res.status);
      const user = res.data.user; if (!user || !user.id) return json({ error:'signup failed: no user returned' }, 500);
      const role = await ensureProfile(env, user.id, user.email || email);
      if (!res.data.session || !res.data.session.access_token) return json({ needs_confirmation:true, role, user:{ id: user.id, email: user.email || email }, message:'Account created. Check your email to confirm, then login.' });
      return json({ access_token: res.data.session.access_token, role, user:{ id: user.id, email: user.email || email } });
    }
    if (url.pathname === '/auth-login' && request.method === 'POST') {
      const body = await bodyJson(); if (!body) return json({ error:'email and password are required' }, 400);
      const res = await goTrue(env, 'token?grant_type=password', { email: String(body.email||'').trim(), password: String(body.password||'') });
      if (!res.ok) return json({ error: goTrueError(res.data) }, res.status === 400 ? 401 : res.status);
      if (!res.data.access_token) return json({ error:'login failed: no session returned (is email confirmation still pending?)' }, 401);
      const user = res.data.user || {}; if (!user.id) return json({ error:'login failed: no user returned' }, 500);
      const role = await ensureProfile(env, user.id, user.email || email);
      return json({ access_token: res.data.access_token, role, user:{ id: user.id, email: user.email || email } });
    }
    if (url.pathname === '/auth-me' && request.method === 'GET') { const me = await verifySession(env, request); if (!me) return json({ error:'login required' }, 401); return json({ user: me, role: await profileRole(env, me.id) }); }
    if (url.pathname === '/auth-logout' && request.method === 'POST') return json({ ok:true });

    // ---- Drive OAuth (Mother Admin, same flow as v1) ----
    if (url.pathname === '/drive-auth-start' && request.method === 'GET') {
      if (!env.GOOGLE_OAUTH_CLIENT_ID) return new Response('Google OAuth client not configured: set GOOGLE_OAUTH_CLIENT_ID and GOOGLE_OAUTH_CLIENT_SECRET in Worker secrets first.', { status: 500 });
      const redirect = `${url.origin}/drive-auth-callback`;
      const auth = new URL('https://accounts.google.com/o/oauth2/v2/auth');
      auth.searchParams.set('client_id', env.GOOGLE_OAUTH_CLIENT_ID); auth.searchParams.set('redirect_uri', redirect);
      auth.searchParams.set('response_type', 'code'); auth.searchParams.set('scope', 'https://www.googleapis.com/auth/drive.file');
      auth.searchParams.set('access_type', 'offline'); auth.searchParams.set('prompt', 'consent');
      return Response.redirect(auth.toString(), 302);
    }
    if (url.pathname === '/drive-auth-callback' && request.method === 'GET') {
      const code = url.searchParams.get('code'); const err = url.searchParams.get('error');
      if (err) return new Response('Google consent was not granted: ' + err, { status: 400, headers: { 'Content-Type': 'text/plain' } });
      if (!code) return new Response('Missing authorization code.', { status: 400, headers: { 'Content-Type': 'text/plain' } });
      if (!env.GOOGLE_OAUTH_CLIENT_ID || !env.GOOGLE_OAUTH_CLIENT_SECRET) return new Response('Google OAuth client not configured in Worker secrets.', { status: 500, headers: { 'Content-Type': 'text/plain' } });
      const redirect = `${url.origin}/drive-auth-callback`;
      const tok = await fetch('https://oauth2.googleapis.com/token', { method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, body: `code=${encodeURIComponent(code)}&client_id=${encodeURIComponent(env.GOOGLE_OAUTH_CLIENT_ID)}&client_secret=${encodeURIComponent(env.GOOGLE_OAUTH_CLIENT_SECRET)}&redirect_uri=${encodeURIComponent(redirect)}&grant_type=authorization_code` });
      const tj = await tok.json();
      if (!tj.refresh_token) return new Response('No refresh token returned. Google only returns one on the first consent (prompt=consent is set); revoke the app at myaccount.google.com/permissions and retry. Details: ' + JSON.stringify(tj).slice(0, 200), { status: 500, headers: { 'Content-Type': 'text/plain' } });
      return new Response(`<!DOCTYPE html><html><body style="font-family:system-ui;max-width:720px;margin:40px auto;padding:0 16px"><h1>Drive connected — one last secret step</h1><p>Copy the refresh token below and paste it <b>only</b> into the Cloudflare Worker secret named <b>GOOGLE_OAUTH_REFRESH_TOKEN</b>. Never paste it in chat, email, or a screenshot. After saving it, close this page.</p><textarea readonly rows="4" style="width:100%">${esc(tj.refresh_token)}</textarea></body></html>`, { headers: { 'Content-Type': 'text/html' } });
    }
    if (url.pathname === '/drive-auth-status' && request.method === 'GET') {
      return json({ oauth_client_configured: Boolean(env.GOOGLE_OAUTH_CLIENT_ID && env.GOOGLE_OAUTH_CLIENT_SECRET), oauth_refresh_token_configured: Boolean(env.GOOGLE_OAUTH_REFRESH_TOKEN), drive_folder_configured: Boolean(env.DRIVE_FOLDER_ID), service_account_configured: Boolean(env.GOOGLE_SERVICE_ACCOUNT_JSON), ai_configured: Boolean(env.GEMINI_API_KEY || env.OPENAI_API_KEY), ai_provider: (env.AI_PROVIDER || 'gemini'), token_encryption_configured: Boolean(env.TOKEN_KEY) });
    }

    // ---- All remaining endpoints require login ----
    const me = await verifySession(env, request);
    if (!me) {
      // Public root message only; every data endpoint below needs auth.
      if (url.pathname === '/' || url.pathname === '') return new Response('AutoPublish Studio Pro worker — runs on hourly Cron, not browser visits.');
      return json({ error:'login required' }, 401);
    }
    const isAdmin = (await profileRole(env, me.id)) === 'admin';

    // ---- Videos: all / queue / history (real DB rows, token-free) ----
    if (url.pathname === '/videos' && request.method === 'GET') {
      try {
        const status = url.searchParams.get('status'); const pageId = url.searchParams.get('page_id'); const q = url.searchParams.get('q');
        let path = `videos?select=${VIDEO_COLS}&order=created_at.desc&limit=1000`;
        if (status) path = `videos?select=${VIDEO_COLS}&status=eq.${encodeURIComponent(status)}&order=created_at.desc&limit=1000`;
        if (pageId) path += `&page_id=eq.${encodeURIComponent(pageId)}`;
        const rows = await sbList(path);
        const filtered = q ? rows.filter(r => (r.file_name||'').toLowerCase().includes(q.toLowerCase()) || (r.title||'').toLowerCase().includes(q.toLowerCase())) : rows;
        return json({ videos: filtered });
      } catch (e) { return json({ error: e.message }, 500); }
    }
    if (url.pathname === '/queue' && request.method === 'GET') {
      try { const rows = await sbList(`videos?select=${VIDEO_COLS}&status=in.(pending,ready,approved)&order=created_at.asc&limit=500`); return json({ videos: rows }); } catch (e) { return json({ error: e.message }, 500); }
    }
    if (url.pathname === '/publishing-queue' && request.method === 'GET') {
      try { const rows = await sbList('publishing_queue?select=*,videos(file_name,title,status)&order=scheduled_at.asc&limit=1000'); return json({ queue: rows }); } catch (e) { return json({ error: e.message }, 500); }
    }
    if (url.pathname === '/history' && request.method === 'GET') {
      try { const rows = await sbList('publishing_history?select=*&order=created_at.desc&limit=1000'); return json({ history: rows }); } catch (e) { return json({ error: e.message }, 500); }
    }
    if (url.pathname === '/analytics' && request.method === 'GET') {
      try {
        const counts = {};
        for (const s of ['pending','ready','approved','published','failed','publishing']) {
          const r = await sb(env, `videos?select=id&status=eq.${s}`); counts[s] = r.ok ? (await r.json()).length : 0;
        }
        const totalRes = await sb(env, 'videos?select=id'); const total = totalRes.ok ? (await totalRes.json()).length : 0;
        const pagesRes = await sb(env, 'pages?select=id'); const pages = pagesRes.ok ? (await pagesRes.json()).length : 0;
        const aiRes = await sb(env, 'ai_generated_contents?select=id'); const aiDone = aiRes.ok ? (await aiRes.json()).length : 0;
        const hbRes = await sb(env, 'heartbeat?select=checked_at&order=checked_at.desc&limit=1'); const hb = hbRes.ok ? await hbRes.json() : [];
        const errRes = await sb(env, 'error_logs?select=source,message,created_at&order=created_at.desc&limit=20'); const errors = errRes.ok ? await errRes.json() : [];
        return json({ total, uploaded: total, published: counts.published||0, failed: counts.failed||0, scheduled: counts.approved||0, pending: counts.pending||0, ready: counts.ready||0, ai_completed: aiDone, pages, last_heartbeat: hb[0] ? hb[0].checked_at : null, recent_errors: errors, success_rate: total ? Math.round((counts.published||0)/total*100) : 0 });
      } catch (e) { return json({ error: e.message }, 500); }
    }

    // ---- Pages: list (no token) / add (admin only) ----
    if (url.pathname === '/pages' && request.method === 'GET') {
      try { const rows = await sbList('pages?select=id,user_id,page_name,facebook_page_id,drive_folder_id,status,created_at&order=created_at.asc&limit=500'); return json({ pages: rows }); } catch (e) { return json({ error: e.message }, 500); }
    }
    if (url.pathname === '/pages' && request.method === 'POST') {
      if (!isAdmin) return json({ error:'only Mother Admin can add pages' }, 403);
      const body = await bodyJson(); if (!body || !body.facebook_page_id || !body.page_token) return json({ error:'facebook_page_id and page_token are required (token is stored server-side only, never returned)' }, 400);
      // v3: token is AES-GCM encrypted at rest (enc:v1:...) before storage.
      // If TOKEN_KEY is missing, encryptPageToken throws and NOTHING is stored.
      let encrypted;
      try { encrypted = await encryptPageToken(env, body.page_token); } catch (e) { return json({ error: e.message }, 500); }
      const ins = await sb(env, 'pages', { method:'POST', body: JSON.stringify({ user_id: me.id, page_name: body.page_name || body.facebook_page_id, facebook_page_id: body.facebook_page_id, page_token_encrypted: encrypted, drive_folder_id: body.drive_folder_id || null, status:'connected' }) });
      if (!ins.ok) return json({ error:'page insert failed: ' + (await ins.text()).slice(0,300) }, 500);
      const row = await ins.json(); return json({ page: Array.isArray(row) ? { ...row[0], page_token_encrypted: undefined } : row });
    }

    // ---- Upload: single file per call; frontend runs N in parallel (default 3).
    // Duplicate detection: file_hash (client sends name+size+lastModified hash).
    // A repeat hash for the same user returns the EXISTING row, no new Drive file.
    if (url.pathname === '/upload' && request.method === 'POST') {
      try {
        const form = await request.formData(); const file = form.get('file');
        if (!file) return json({ error:'no file' }, 400);
        const fileHash = form.get('file_hash') ? String(form.get('file_hash')) : null;
        if (fileHash) {
          const dup = await sb(env, `videos?select=${VIDEO_COLS}&user_id=eq.${me.id}&file_hash=eq.${encodeURIComponent(fileHash)}&limit=1`);
          if (dup.ok) { const rows = await dup.json(); if (rows.length) return json({ duplicate:true, video_id: rows[0].id, drive_file_id: rows[0].drive_file_id, file_name: rows[0].file_name, message:'Duplicate file detected — existing video reused, no new upload.' }); }
        }
        const dt = await driveToken(env);
        const meta = { name: file.name, mimeType: file.type || 'video/mp4' };
        if (env.DRIVE_FOLDER_ID) meta.parents = [env.DRIVE_FOLDER_ID];
        const init = await fetch('https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable', { method:'POST', headers:{ Authorization:`Bearer ${dt}`, 'Content-Type':'application/json' }, body: JSON.stringify(meta) });
        const session = init.headers.get('Location'); if (!session) throw new Error('Drive upload session failed');
        const bytes = await file.arrayBuffer();
        // v3 resumable upload: 8 MB chunks. Google returns 308 (Resume
        // Incomplete) for non-final chunks, with a Range header telling us
        // how far it got; the final chunk returns 200/201 + file metadata.
        // A failed chunk (network/5xx) is retried ONCE at the same offset.
        // LIMIT: this whole loop runs inside ONE Worker request, so a very
        // large file can hit Worker CPU/time limits — see README-V3.md.
        // For such files, upload in smaller pieces or use Drive directly.
        const CHUNK = 8 * 1024 * 1024;
        let created = null, offset = 0;
        while (offset < bytes.byteLength) {
          const end = Math.min(bytes.byteLength, offset + CHUNK);
          const chunk = bytes.slice(offset, end);
          let chunkDone = false;
          for (let attempt = 0; attempt < 2 && !chunkDone; attempt++) {
            let res;
            try {
              res = await fetch(session, { method:'PUT', headers:{ 'Content-Length': String(chunk.byteLength), 'Content-Range': `bytes ${offset}-${end - 1}/${bytes.byteLength}` }, body: chunk });
            } catch (e) {
              if (attempt === 0) continue; // one retry for a network failure
              throw new Error('Drive chunk upload failed (network): ' + e.message);
            }
            if (res.status === 308) {
              const range = res.headers.get('Range') || '';
              const m = range.match(/bytes=0-(\d+)/);
              offset = m ? parseInt(m[1], 10) + 1 : end; // server-confirmed position
              chunkDone = true;
            } else if (res.ok) {
              created = await res.json().catch(() => ({})); offset = end; chunkDone = true;
            } else {
              if (attempt === 0) continue; // one retry for a failed chunk
              throw new Error('Drive chunk upload failed: HTTP ' + res.status + ' ' + (await res.text()).slice(0, 200));
            }
          }
          if (!chunkDone) throw new Error('Drive chunk upload failed after retry');
        }
        if (!created || !created.id) throw new Error('Drive upload failed: no file ID returned after final chunk');
        const row = { user_id: me.id, drive_file_id: created.id, file_name: file.name, title: file.name.replace(/\.(mp4|mov|mkv|webm)$/i,''), status:'pending', ai_status:'pending', file_hash: fileHash, file_size_bytes: bytes.byteLength, mime_type: file.type || 'video/mp4' };
        const pid = form.get('page_id'); if (pid) row.page_id = pid;
        const ins = await sb(env, 'videos', { method:'POST', body: JSON.stringify(row) });
        if (!ins.ok) throw new Error('Supabase videos insert failed: ' + (await ins.text()).slice(0,300));
        const inserted = await ins.json();
        const vid = Array.isArray(inserted) && inserted[0] ? inserted[0] : null;
        if (vid) await sb(env, 'background_jobs', { method:'POST', headers:{Prefer:'return=minimal'}, body: JSON.stringify({ job_type:'ai_process', video_id: vid.id, user_id: me.id, status:'pending' }) });
        return json({ drive_file_id: created.id, file_name: file.name, video_id: vid ? vid.id : null });
      } catch (e) { await logError(env, 'drive', e.message, null, me.id); return json({ error: e.message }, 500); }
    }

    // ---- Approve once (admin only): appends after last scheduled time ----
    if (url.pathname === '/approve' && request.method === 'POST') {
      if (!isAdmin) return json({ error:'only Mother Admin can approve' }, 403);
      const body = await bodyJson(); if (!body) return json({ error:'invalid JSON body' }, 400);
      const items = Array.isArray(body) ? body : (Array.isArray(body?.items) ? body.items : null);
      const gap = body.gap_minutes && Number(body.gap_minutes) > 0 ? Number(body.gap_minutes) : await scheduleGap(env);
      let approved = 0, skipped = 0; const results = [];
      // Append-after-last: base = max existing scheduled_at, else now.
      let base = new Date();
      try { const last = await sbList('publishing_queue?select=scheduled_at&order=scheduled_at.desc&limit=1'); if (last.length && last[0].scheduled_at) base = new Date(last[0].scheduled_at); } catch {}
      // Also respect explicit items with scheduled_at when the caller provides them.
      const list = items || [];
      if (!list.length && body.video_ids && Array.isArray(body.video_ids)) {
        for (const vid of body.video_ids) list.push({ video_id: vid });
      }
      if (!list.length) return json({ error:'no videos to approve: send {items:[{video_id, scheduled_at?}]} or {video_ids:[...]}' }, 400);
      for (const it of list) {
        const video_id = it?.video_id || it?.id; if (!video_id) { skipped++; continue; }
        let when = it?.scheduled_at && !isNaN(Date.parse(it.scheduled_at)) ? new Date(it.scheduled_at) : new Date(base.getTime() + gap*60000);
        if (!it?.scheduled_at) base = when;
        const idem = `pub-${video_id}-${when.toISOString()}`;
        const r = await sb(env, `videos?id=eq.${encodeURIComponent(video_id)}&status=in.(pending,ready)`, { method:'PATCH', body: JSON.stringify({ status:'approved', scheduled_at: when.toISOString() }) });
        if (!r.ok) { skipped++; results.push({ video_id, status:'skipped', reason:'Supabase update failed' }); continue; }
        const changed = await r.json();
        if (!Array.isArray(changed) || !changed.length) { skipped++; results.push({ video_id, status:'skipped', reason:'row not pending/ready (already approved, publishing, published, failed, or not found)' }); continue; }
        const v = changed[0];
        await sb(env, 'publishing_queue', { method:'POST', headers:{Prefer:'return=minimal,resolution=ignore-duplicates'}, body: JSON.stringify({ video_id, user_id: v.user_id || me.id, page_id: v.page_id || null, scheduled_at: when.toISOString(), status:'scheduled', idempotency_key: idem }) });
        approved++; results.push({ video_id, status:'approved', scheduled_at: when.toISOString() });
      }
      return json({ approved, skipped, results, gap_minutes: gap });
    }

    // ---- Scheduler: pause / resume / status (admin only for writes) ----
    if (url.pathname === '/scheduler' && request.method === 'GET') {
      try { const rows = await sbList('publishing_schedules?select=*&order=created_at.desc&limit=100'); return json({ schedules: rows, paused: await schedulerPaused(env), gap_minutes: await scheduleGap(env) }); } catch (e) { return json({ error: e.message }, 500); }
    }
    if (url.pathname === '/scheduler/pause' && request.method === 'POST') {
      if (!isAdmin) return json({ error:'only Mother Admin can pause the scheduler' }, 403);
      await sb(env, 'settings?user_id=eq.' + me.id, { method:'PATCH', body: JSON.stringify({ scheduler_paused:true }) });
      return json({ paused:true });
    }
    if (url.pathname === '/scheduler/resume' && request.method === 'POST') {
      if (!isAdmin) return json({ error:'only Mother Admin can resume the scheduler' }, 403);
      await sb(env, 'settings?user_id=eq.' + me.id, { method:'PATCH', body: JSON.stringify({ scheduler_paused:false }) });
      return json({ paused:false });
    }
    if (url.pathname === '/scheduler' && request.method === 'POST') {
      if (!isAdmin) return json({ error:'only Mother Admin can change the schedule' }, 403);
      const body = await bodyJson(); if (!body) return json({ error:'invalid JSON body' }, 400);
      const ins = await sb(env, 'publishing_schedules', { method:'POST', body: JSON.stringify({ user_id: me.id, page_id: body.page_id || null, name: body.name || 'Default schedule', interval_minutes: Number(body.interval_minutes) || 60, start_at: body.start_at || null, specific_times: body.specific_times ? JSON.stringify(body.specific_times) : null, mode: body.mode || 'interval' }) });
      if (!ins.ok) return json({ error:'schedule insert failed: ' + (await ins.text()).slice(0,300) }, 500);
      if (body.interval_minutes) await sb(env, 'settings?user_id=eq.' + me.id, { method:'PATCH', body: JSON.stringify({ schedule_gap_minutes: Number(body.interval_minutes) }) });
      return json({ schedule: await ins.json() });
    }

    // ---- AI Content Studio: list / edit / regenerate / approve ----
    if (url.pathname === '/ai-content' && request.method === 'GET') {
      try { const rows = await sbList('ai_generated_contents?select=*,videos(file_name,status)&order=generated_at.desc&limit=1000'); return json({ contents: rows }); } catch (e) { return json({ error: e.message }, 500); }
    }
    if (url.pathname === '/ai-content' && request.method === 'PATCH') {
      const body = await bodyJson(); if (!body || !body.video_id) return json({ error:'video_id is required' }, 400);
      const patch = {}; for (const k of ['title','description','caption','hashtags']) if (body[k] !== undefined) patch[k] = body[k];
      if (body.approved !== undefined) patch.approved = !!body.approved;
      if (body.rejected !== undefined) patch.rejected = !!body.rejected;
      const r = await sb(env, `ai_generated_contents?video_id=eq.${encodeURIComponent(body.video_id)}`, { method:'PATCH', body: JSON.stringify(patch) });
      if (!r.ok) return json({ error:'AI content update failed: ' + (await r.text()).slice(0,300) }, 500);
      const changed = await r.json();
      if (Array.isArray(changed) && changed.length && patch.title) await sb(env, `videos?id=eq.${encodeURIComponent(body.video_id)}`, { method:'PATCH', body: JSON.stringify({ title: patch.title, description: patch.description || undefined, caption: patch.caption || undefined, hashtags: patch.hashtags || undefined, status:'ready' }) });
      return json({ updated: changed });
    }
    if (url.pathname === '/ai-generate' && request.method === 'POST') {
      const body = await bodyJson(); if (!body || !body.video_id) return json({ error:'video_id is required' }, 400);
      // Transcript source: body.transcript, else stored transcript row, else error (never fake).
      let transcript = (body.transcript || '').trim();
      if (!transcript) { try { const t = await sbList(`video_transcripts?select=transcript,language&video_id=eq.${encodeURIComponent(body.video_id)}&limit=1`); if (t.length) transcript = t[0].transcript || ''; } catch {} }
      try {
        const { cfg, parsed, usage } = await aiGenerate(env, transcript, body.language);
        const valid = validateAi(parsed);
        // Never reuse another video's content: uniqueness check against stored contents.
        try { const all = await sbList('ai_generated_contents?select=description&limit=1000'); const dup = all.some(c => c.description && c.description === valid.description); if (dup) throw new Error('Generated description duplicates an existing video — regenerate instead of reusing.'); } catch (e) { if (/duplicates/.test(e.message)) throw e; }
        const upsert = await sb(env, 'ai_generated_contents', { method:'POST', headers:{ Prefer:'resolution=merge-duplicates' }, body: JSON.stringify({ video_id: body.video_id, user_id: me.id, title: valid.title, description: valid.description, caption: valid.caption, hashtags: valid.hashtags, provider: cfg.provider, model: cfg.model, approved:false }) });
        if (!upsert.ok) return json({ error:'AI content save failed: ' + (await upsert.text()).slice(0,300) }, 500);
        await sb(env, `videos?id=eq.${encodeURIComponent(body.video_id)}`, { method:'PATCH', body: JSON.stringify({ title: valid.title, description: valid.description, caption: valid.caption, hashtags: valid.hashtags, ai_status:'completed', status:'ready' }) });
        await logUsage(env, cfg.provider, 'content_generate', cfg.model, true, null, me.id);
        return json({ content: valid, provider: cfg.provider, model: cfg.model });
      } catch (e) {
        await logError(env, 'ai', e.message, body.video_id, me.id);
        await logUsage(env, (env.AI_PROVIDER || 'gemini'), 'content_generate', env.AI_MODEL || null, false, e.message, me.id);
        const status = /not configured|needs a transcript/.test(e.message) ? 503 : 500;
        return json({ error: e.message }, status);
      }
    }
    if (url.pathname === '/transcripts' && request.method === 'POST') {
      const body = await bodyJson(); if (!body || !body.video_id || !body.transcript) return json({ error:'video_id and transcript are required' }, 400);
      const low = /unclear|inaudible|\[inaudible\]/i.test(body.transcript) || body.transcript.trim().length < 20;
      const ins = await sb(env, 'video_transcripts', { method:'POST', headers:{ Prefer:'resolution=merge-duplicates' }, body: JSON.stringify({ video_id: body.video_id, user_id: me.id, transcript: body.transcript, language: body.language || 'auto', confidence: body.confidence ?? null, confidence_flag: low ? 'low' : 'high', source: body.source || 'manual' }) });
      if (!ins.ok) return json({ error:'transcript save failed: ' + (await ins.text()).slice(0,300) }, 500);
      await sb(env, `videos?id=eq.${encodeURIComponent(body.video_id)}`, { method:'PATCH', body: JSON.stringify({ transcript: body.transcript, ai_status:'transcribed' }) });
      return json({ transcript: await ins.json(), low_confidence: low });
    }

    // ---- v3: Real transcription (Gemini Files / Whisper, no ffmpeg) ----
    if (url.pathname === '/transcribe' && request.method === 'POST') {
      const body = await bodyJson(); if (!body || !body.video_id) return json({ error:'video_id is required' }, 400);
      try {
        const rows = await sbList(`videos?select=${VIDEO_COLS}&id=eq.${encodeURIComponent(body.video_id)}&limit=1`);
        if (!rows.length) return json({ error:'video not found' }, 404);
        const video = rows[0];
        if (!video.drive_file_id) return json({ error:'video has no Drive file ID — upload it first' }, 400);
        if (!env.GEMINI_API_KEY && !env.OPENAI_API_KEY) return json({ error:'Transcription not configured: set GEMINI_API_KEY or OPENAI_API_KEY in Worker secrets. No transcript was generated.' }, 503);
        const { bytes } = await fetchDriveVideo(env, video.drive_file_id); // throws Drive-not-connected error (503 below) if no OAuth/service token
        const provider = env.GEMINI_API_KEY ? 'gemini' : 'openai';
        const res = provider === 'gemini' ? await transcribeWithGemini(env, bytes, video.mime_type || 'video/mp4') : await transcribeWithOpenAI(env, bytes, video.mime_type || 'video/mp4');
        const transcript = (res.transcript || '').trim();
        const low = transcript.length < 20 || /inaudible|unclear|\[inaudible\]/i.test(transcript);
        await sb(env, 'video_transcripts', { method:'POST', headers:{ Prefer:'resolution=merge-duplicates' }, body: JSON.stringify({ video_id: video.id, user_id: me.id, transcript, language: res.language || 'auto', confidence: null, confidence_flag: low ? 'low' : 'high', source: provider }) });
        await sb(env, `videos?id=eq.${encodeURIComponent(video.id)}`, { method:'PATCH', body: JSON.stringify({ transcript, ai_status:'transcribed' }) });
        await logUsage(env, provider, 'transcribe', provider === 'gemini' ? 'gemini-files' : (env.OPENAI_TRANSCRIPTION_MODEL || 'whisper-1'), true, null, me.id);
        return json({ video_id: video.id, provider, transcript, language: res.language || 'auto', confidence_note: res.confidence_note || null, low_confidence: low, ai_status:'transcribed' });
      } catch (e) {
        await logError(env, 'transcribe', e.message, body.video_id, me.id);
        const status = /not configured|Drive not connected|Drive OAuth|Drive token/i.test(e.message) ? 503 : 500;
        return json({ error: e.message }, status);
      }
    }
    if (url.pathname === '/transcripts' && request.method === 'GET') {
      try { const rows = await sbList('video_transcripts?select=*,videos(file_name)&order=created_at.desc&limit=1000'); return json({ transcripts: rows }); } catch (e) { return json({ error: e.message }, 500); }
    }

    // ---- Settings (admin writes; everyone reads own/global view) ----
    if (url.pathname === '/settings' && request.method === 'GET') {
      try { const rows = await sbList('settings?select=*&limit=1'); const sys = await sbList('system_settings?select=*&limit=100'); return json({ settings: rows[0] || null, system: sys, ai: aiConfig(env).provider, ai_configured: Boolean(env.GEMINI_API_KEY || env.OPENAI_API_KEY) }); } catch (e) { return json({ error: e.message }, 500); }
    }
    if (url.pathname === '/settings' && request.method === 'POST') {
      if (!isAdmin) return json({ error:'only Mother Admin can change settings' }, 403);
      const body = await bodyJson(); if (!body) return json({ error:'invalid JSON body' }, 400);
      const patch = {}; for (const k of ['drive_folder_id','schedule_gap_minutes','language','ai_provider','ai_model','ai_language','max_concurrent_uploads','max_retries']) if (body[k] !== undefined) patch[k] = body[k];
      const r = await sb(env, 'settings?user_id=eq.' + me.id, { method:'PATCH', body: JSON.stringify(patch) });
      return json({ ok: r.ok });
    }
    if (url.pathname === '/errors' && request.method === 'GET') {
      try { const rows = await sbList('error_logs?select=*&order=created_at.desc&limit=100'); return json({ errors: rows }); } catch (e) { return json({ error: e.message }, 500); }
    }
    if (url.pathname === '/publish-now' && request.method === 'POST') {
      if (!isAdmin) return json({ error:'only Mother Admin can trigger a manual tick' }, 403);
      const result = await publisherTick(env); return json(result);
    }
    return new Response('AutoPublish Studio Pro worker — unknown endpoint.', { status: 404, headers: cors });
  }
};
