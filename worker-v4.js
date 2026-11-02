// ============================================================================
// AutoPublish Studio Pro — Cloudflare Worker v4
// V4 (built from worker-v3.2.js, v3.2 files untouched, NOT live, NOT deployed):
//
// NORMAL USER FLOW (no manual clicks after upload):
//   Login -> Dashboard -> Bulk Upload -> upload multiple -> Finish.
//   Everything after upload is automatic, server-side, PC off:
//   upload -> Cron tick migration -> auto AI pipeline (transcribe -> SRT ->
//   unique title/description/caption/hashtags) -> auto-schedule (next
//   sequential slot, Mother Admin GLOBAL gap) -> auto FB publish on schedule.
//   There is NO Generate / Transcribe / Retry / video-ID paste / manual
//   title / Approve / manual schedule click for normal users in V4.
//
// ADMIN vs NORMAL RESOLUTION:
//   - AI for EVERYONE (normal users AND Admin pipeline): Mother Admin
//     GLOBAL AI settings (system_settings key 'ai_settings', scope=global)
//     > Worker env fallback (GEMINI_API_KEY / OPENAI_API_KEY / AI_PROVIDER /
//     AI_MODEL). A normal user's own per-profile ai_settings row
//     (ai_settings:<user_id>), if one exists from V3.1/V3.2, is KEPT in the
//     DB untouched but is NEVER read for generation or transcription in V4.
//   - Schedule gap for EVERYONE: Mother Admin GLOBAL scheduler settings
//     (system_settings key 'global_scheduler' {gap_minutes: 30|60|120|custom})
//     > Admin profile settings.schedule_gap_minutes > 60 minutes.
//   - Admin = first profiles row only (existing rule, NO email hardcoded).
//     Admin default workspace = own user_id; management view = explicit
//     ?profile_id= selecting ONE profile (invalid -> 404). Clients'
//     profile_id params are ignored.
//   - Normal users are 403 on all technical endpoints (see V4 gate in
//     fetch()): /ai-settings, /admin/*, /pipeline, /srt, /transcripts,
//     /ai-content, /ai-generate, /transcribe, /ai-retry, /ai-process,
//     /errors, /drive-auth-status, /drive-auth-start, /publish-now,
//     /approve, /scheduler (+pause/resume). /approve is Admin manual
//     override only in V4; normal scheduling is fully automatic.
//   - Normal users may use ONLY their own data: /upload, /videos, /queue,
//     /publishing-queue, /history, /analytics (own counts, NO error logs),
//     /pages GET+POST (own only), /settings GET (own language +
//     global_gap_minutes only) / POST (own language only), and the new
//     GET /scheduler-info (global gap + own scheduled count, no keys).
//   - /videos for normal users NEVER returns transcript, last_error,
//     retry_count, ai_status, drive_file_id or file_hash — only the
//     All-Videos modal fields (see normalVideoView). Admin full rows
//     (incl. transcript) are available ONLY via GET /admin/video?id=
//     (admin only, ownership-checked against the effective profile).
//
// RLS DEFENSE-IN-DEPTH: the Worker uses the Supabase SERVICE key, which
// bypasses Row Level Security, so Worker-side enforcement (effective
// user_id filtering + ownership 404 + admin gating above) is PRIMARY.
// Any additive RLS SQL is defense-in-depth only and must NOT be claimed
// to secure the Worker path on its own.
//
// OTHER V4 RULES (unchanged from V3.2 unless noted):
//   - Model Switch preserved: Admin-selected models only, in order,
//     3500ms (MODEL_FALLBACK_DELAY_MS) between attempts, every attempt
//     logged in api_usage_logs, successful model recorded, all-fail error
//     lists every model tried. No model is ever shown as 'available'.
//   - Publishing: Meta Graph API only, status-filter claim lock (second
//     claim = 0 rows), backoff then dead-letter, Drive delete ONLY after
//     a real Facebook post/video ID. Never Published without a Meta ID.
//   - Cron tick = heartbeat + migration + ai_process (1 job/tick, as
//     before — documented limit, bulk queues drain gradually) +
//     auto-schedule (inside pipeline + migration sweep) + publisherTick.
//     The Cron INTERVAL itself is a Cloudflare dashboard setting, NOT
//     changed in code here.
//   - No mock AI, no fake publishing, no real keys/tokens/passwords in
//     this file, no hardcoded email. Keys are AES-GCM encrypted with
//     TOKEN_KEY and are NEVER returned by any GET endpoint.
// ============================================================================

// AutoPublish Studio Pro — Cloudflare Worker v3.2 (Model Switch / Fallback)
// v3.2 changes (v3.1 files untouched): per-provider SELECTED model lists in
// AI Settings (Select All or one-by-one, plus custom models). Generation
// and Gemini transcription try ONLY the selected models, in order, waiting
// 3500ms between model attempts, and keep switching until one succeeds
// or the whole selected list has failed. Transcription falls back to the
// other provider (if configured) as a second stage. No model is ever
// presented as 'available' — only 'selected', 'last successful', or
// 'last tried / failed'. Suggested lists are UI suggestions only;
// availability depends on the user's own API key/account.
// v2 files are left untouched. Deploy v3 ONLY after schema-v2.sql has been
// run and the new secrets in ../.env.example (incl. TOKEN_KEY) are set.
// v3 changes: (1) page-token AES-GCM encryption at rest, (2) real
// transcription via Gemini Files / OpenAI Whisper (no ffmpeg in Worker),
//  (3) chunked resumable Drive upload (8 MB chunks, 308 resume, 1 retry).
// v3.1 changes (v3 files untouched): AI Settings (Gemini + OpenAI) can be set
//  from the site by Mother Admin only — Base URL, Model, API Key per provider.
//  Keys are AES-GCM encrypted at rest in Supabase system_settings (key
//  'ai_settings', same TOKEN_KEY scheme as page tokens) and are NEVER
//  returned by any GET endpoint. Resolution order: DB settings first, then
//  Worker env fallback (GEMINI_API_KEY / OPENAI_API_KEY / AI_PROVIDER /
//  AI_MODEL). No schema change: system_settings already exists in schema-v2.
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
//  (v3.1) AI settings may instead be stored in Supabase system_settings
//  under key 'ai_settings' via POST /ai-settings (Admin only, encrypted).
//  Env remains the fallback when no DB setting exists for a provider.
//  TOKEN_KEY (required in v3 for POST /pages — page-token encryption key)
//
// Cron: every hour (Cloudflare Cron Trigger). Also handles 30m/2h/3h/6h
// schedules by checking publishing_queue.scheduled_at on every tick — the
// tick interval is the floor, the queue's scheduled_at is the truth.

const B64 = (buf) => { let s=''; const b=new Uint8Array(buf); for (let i=0;i<b.length;i++) s+=String.fromCharCode(b[i]); return btoa(s).replace(/\+/g,'-').replace(/\//g,'_').replace(/=+$/,''); };
const sb = (env, path, opts={}) => fetch(`${env.SUPABASE_URL}/rest/v1/${path}`, { ...opts, headers: { apikey: env.SUPABASE_SERVICE_KEY, Authorization: `Bearer ${env.SUPABASE_SERVICE_KEY}`, 'Content-Type': 'application/json', Prefer: 'return=representation', ...(opts.headers||{}) } });
const esc = (v) => String(v).replace(/&/g,'&amp;').replace(/</g,'&lt;');
const MAX_ATTEMPTS = 5;
const PIPELINE_VIDEO_COLS = 'id,user_id,page_id,drive_file_id,file_name,file_hash,file_size_bytes,mime_type,duration_seconds,resolution,title,description,caption,hashtags,transcript,status,ai_status,ai_confidence,retry_count,last_error,scheduled_at,facebook_post_id,published_at,created_at';
const backoffMs = (attempt) => Math.min(60*60*1000, Math.pow(2, attempt) * 60*1000); // 2m,4m,8m,16m,32m -> capped 1h

// ---------- v3.2: Model Switch / Fallback ----------
// Suggested model lists are UI suggestions ONLY. They are not a promise
// that a model exists, is enabled, or is available on the user's key or
// account — the API reply is the truth. The user selects which models the
// system may try; the system tries ONLY those, in order.
// Order rule (documented, no drag reorder in v3.2): suggested-list order
// first, then custom models in the order they were added.
const SUGGESTED_MODELS = {
  gemini: ['gemini-2.5-flash', 'gemini-2.5-pro', 'gemini-2.0-flash', 'gemini-1.5-flash', 'gemini-1.5-pro'],
  openai: ['gpt-4o-mini', 'gpt-4o', 'gpt-4.1-mini', 'gpt-4.1'],
};
const MODEL_FALLBACK_DELAY_MS = 3500; // user-specified 3-4 seconds between model attempts
const sleepMs = (ms) => new Promise(res => setTimeout(res, ms));
function validSelectedModels(raw) {
  if (!Array.isArray(raw)) return null;
  const out = [];
  for (const m of raw) {
    const v = validModel(m);
    if (!v) return null;
    if (!out.includes(v)) out.push(v);
    if (out.length >= 20) break;
  }
  return out;
}
// Effective selected list for a resolved provider: stored list if any,
// otherwise the single resolved model as a one-model list (backward compat
// with v3.1 rows that have no selected_models field).
function selectedModelsFor(resolvedProv) {
  if (resolvedProv && Array.isArray(resolvedProv.selected_models) && resolvedProv.selected_models.length) return resolvedProv.selected_models;
  return resolvedProv && resolvedProv.model ? [resolvedProv.model] : [];
}
function truncateErr(e, n) { return String((e && e.message) || e || 'unknown error').slice(0, n); }


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

// ---------- v3.1: Site-managed AI settings (Gemini + OpenAI) ----------
// Storage: ONE row in system_settings, key 'ai_settings', value (jsonb):
//   { active_provider: 'gemini'|'openai',
//     gemini: { base_url, model, api_key_enc },   // api_key_enc = enc:v1:...
//     openai: { base_url, model, api_key_enc } }
// api_key_enc is produced by encryptPageToken() (TOKEN_KEY AES-GCM) and is
// NEVER returned by any endpoint. GET /ai-settings returns only base_url,
// model and a configured boolean per provider. GET /settings filters the
// 'ai_settings' row out entirely, so the encrypted blob never leaks there.
// Resolution order (per provider): DB setting first, then Worker env.
// Base URL contract: the API ROOT, without a trailing /v1 or /v1beta
// (e.g. https://api.openai.com, https://generativelanguage.googleapis.com).
// The code appends /v1/... (OpenAI) or /v1beta/... (Gemini) itself, and
// tolerates a base that already ends in /v1 or /v1beta.
// OpenAI transcription keeps using OPENAI_TRANSCRIPTION_MODEL or
// 'whisper-1' (NOT the chat model) — the UI Model field for OpenAI is the
// generation/chat model, matching v3 behaviour.
const AI_DEFAULTS = {
  gemini: { base_url: 'https://generativelanguage.googleapis.com', model: 'gemini-2.5-flash' },
  openai: { base_url: 'https://api.openai.com', model: 'gpt-4o-mini' },
};
function stripTrailingSlash(u) { return String(u).replace(/\/+$/, ''); }
function validBaseUrl(raw) {
  if (typeof raw !== 'string') return null;
  const t = stripTrailingSlash(raw.trim());
  if (!t || t.length > 200) return null;
  let u;
  try { u = new URL(t); } catch { return null; }
  if (u.protocol !== 'https:' && u.protocol !== 'http:') return null;
  if (u.username || u.password) return null; // no credentials in URL, ever
  if (u.search || u.hash) return null;
  return u.origin + (u.pathname === '/' ? '' : u.pathname);
}
function validModel(raw) {
  if (typeof raw !== 'string') return null;
  const t = raw.trim();
  if (!/^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/.test(t)) return null;
  return t;
}
function geminiUrl(base, suffix) {
  const b = stripTrailingSlash(base);
  if (b.endsWith('/v1beta')) return b + suffix.replace(/^\/v1beta/, '');
  return b + suffix;
}
function openaiUrl(base, suffix) {
  const b = stripTrailingSlash(base);
  if (b.endsWith('/v1')) return b + suffix.replace(/^\/v1/, '');
  return b + suffix;
}
async function readSystemValue(env, key) {
  try {
    const r = await sb(env, `system_settings?select=value&key=eq.${encodeURIComponent(key)}&limit=1`);
    if (!r.ok) return null;
    const rows = await r.json();
    if (!Array.isArray(rows) || !rows.length) return null;
    const v = rows[0] && rows[0].value;
    return (v && typeof v === 'object') ? v : null;
  } catch { return null; }
}
async function readAiSettingsRow(env) { return readSystemValue(env, 'ai_settings'); }
async function readAiSettingsRowFor(env, userId) {
  if (!userId) return null;
  return readSystemValue(env, 'ai_settings:' + userId);
}
// v3.1 isolation: a DB AI key exists if the GLOBAL row OR ANY profile row
// (ai_settings:<user_id>) holds an encrypted key. Presence only — the blob
// itself is never returned or logged.
async function dbAiKeyPresent(env) {
  const has = (row) => {
    if (!row) return false;
    for (const prov of ['gemini', 'openai']) {
      const e = row[prov] && row[prov].api_key_enc;
      if (typeof e === 'string' && e.length > 0) return true;
    }
    return false;
  };
  if (has(await readAiSettingsRow(env))) return true;
  try {
    const r = await sb(env, `system_settings?select=value&key=like.${encodeURIComponent('ai_settings:%')}`);
    if (r.ok) { const rows = await r.json(); if (Array.isArray(rows) && rows.some(x => has(x.value))) return true; }
  } catch {}
  return false;
}
// Full resolution (decrypts DB keys in memory only, never returns them to
// a caller that is not an internal AI call). Throws a clear, key-free
// error if a stored key cannot be decrypted (e.g. TOKEN_KEY changed).
// v3.1 isolation resolution order, per provider (field-level merge):
//   profile row (ai_settings:<userId>) > global row (ai_settings) > env.
// active_provider: profile row > global row > AI_PROVIDER env > gemini.
// With userId null (public /drive-auth-status, global scope) only the
// global row + env are considered. `source` is the KEY source (profile /
// global / env / none) — it is safe to return to the owner in GET
// /ai-settings because it reveals nothing about the key itself.
async function resolveAiSettings(env, userId) {
  const globalRow = await readAiSettingsRow(env);
  const profileRow = userId ? await readAiSettingsRowFor(env, userId) : null;
  const pickActive = (row) => (row && (row.active_provider === 'gemini' || row.active_provider === 'openai')) ? row.active_provider : null;
  const active = pickActive(profileRow) || pickActive(globalRow)
    || (((env.AI_PROVIDER || 'gemini').toLowerCase() === 'openai') ? 'openai' : 'gemini');
  const out = { active_provider: active, gemini: null, openai: null };
  for (const prov of ['gemini', 'openai']) {
    const prof = (profileRow && profileRow[prov] && typeof profileRow[prov] === 'object') ? profileRow[prov] : {};
    const glob = (globalRow && globalRow[prov] && typeof globalRow[prov] === 'object') ? globalRow[prov] : {};
    const base = validBaseUrl(prof.base_url) || validBaseUrl(glob.base_url) || AI_DEFAULTS[prov].base_url;
    const model = validModel(prof.model) || validModel(glob.model) || validModel(env.AI_MODEL) || AI_DEFAULTS[prov].model;
    // v3.2: selected model list — profile list wins, then global list,
    // then (empty) which callers turn into [model] via selectedModelsFor().
    const profSel = (prof.selected_models && Array.isArray(prof.selected_models)) ? prof.selected_models.map(validModel).filter(Boolean) : [];
    const globSel = (glob.selected_models && Array.isArray(glob.selected_models)) ? glob.selected_models.map(validModel).filter(Boolean) : [];
    const storedSel = profSel.length ? profSel : globSel;
    const selected_models = storedSel.length ? [...new Set(storedSel)] : [model];
    let key = null, source = 'none';
    const enc = (typeof prof.api_key_enc === 'string' && prof.api_key_enc.length > 0) ? prof.api_key_enc
      : (typeof glob.api_key_enc === 'string' && glob.api_key_enc.length > 0) ? glob.api_key_enc : null;
    const encSource = (typeof prof.api_key_enc === 'string' && prof.api_key_enc.length > 0) ? 'profile' : (enc ? 'global' : null);
    if (enc) {
      let dec;
      try { dec = await decryptPageToken(env, enc); }
      catch { throw new Error('Stored AI key cannot be decrypted — TOKEN_KEY may have changed. Enter a new key in AI Settings.'); }
      if (dec && dec.token) { key = dec.token; source = encSource; }
    }
    if (!key) key = (prov === 'gemini' ? env.GEMINI_API_KEY : env.OPENAI_API_KEY) || null;
    if (key && source === 'none') source = 'env';
    out[prov] = { base_url: base, model, key, configured: Boolean(key), source, selected_models };
  }
  return out;
}
function aiSettingsSafeView(resolved) {
  return {
    active_provider: resolved.active_provider,
    gemini: { base_url: resolved.gemini.base_url, model: resolved.gemini.model, configured: resolved.gemini.configured, source: resolved.gemini.source || 'none', selected_models: resolved.gemini.selected_models || [resolved.gemini.model] },
    openai: { base_url: resolved.openai.base_url, model: resolved.openai.model, configured: resolved.openai.configured, source: resolved.openai.source || 'none', selected_models: resolved.openai.selected_models || [resolved.openai.model] },
  };
}
// Internal system_settings keys that must NEVER appear in GET /settings
// (they hold encrypted keys, SRT Drive file IDs, or per-profile state).
function isInternalSystemKey(key) {
  if (!key) return false;
  return key === 'ai_settings' || key === 'pipeline_settings' || key.startsWith('ai_settings:') || key.startsWith('srt_meta:') || key.startsWith('drive_folder:') || key.startsWith('profile_status:') || key.startsWith('pipeline_settings:');
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
  let role = 'client';
  try { const r = await sb(env, 'profiles?select=user_id&limit=1'); const rows = r.ok ? await r.json() : null; if (!rows || !rows.length) role = 'admin'; } catch {}
  await sb(env, 'profiles', { method:'POST', headers:{ Prefer:'return=representation,resolution=merge-duplicates' }, body: JSON.stringify({ user_id: userId, email: email || null, role }) });
  return role;
}
// v3.1 isolation: schedule gap, pause, language and auto-approve are
// PER PROFILE (that profile's settings row / pipeline_settings row).
// Fallbacks: gap falls back to the first settings row, then 60; pause
// defaults to false when the profile has no settings row; auto-approve
// falls back to the legacy global pipeline_settings row (pre-isolation
// V3.1) and defaults OFF.
async function settingsRowFor(env, userId) {
  if (!userId) return null;
  try { const r = await sb(env, `settings?select=*&user_id=eq.${encodeURIComponent(userId)}&limit=1`); if (!r.ok) return null; const rows = await r.json(); return (Array.isArray(rows) && rows[0]) ? rows[0] : null; } catch { return null; }
}
async function schedulerPausedFor(env, userId) {
  const row = await settingsRowFor(env, userId);
  return !!(row && row.scheduler_paused);
}
async function scheduleGapFor(env, userId) {
  const row = await settingsRowFor(env, userId);
  const g = row && row.schedule_gap_minutes;
  if (typeof g === 'number' && g > 0) return g;
  return 60; // NO fallback to another profile's row (isolation)
}
async function readPipelineSettingsFor(env, userId) {
  let p = userId ? await readSystemValue(env, 'pipeline_settings:' + userId) : null;
  if (!p) p = await readSystemValue(env, 'pipeline_settings'); // legacy global fallback
  return p;
}
async function autoApproveEnabled(env, userId) { const p = await readPipelineSettingsFor(env, userId); return !!(p && p.auto_approve_ready); }
// NOTE (v4): autoApproveEnabled is kept for Admin /settings display only.
// The V4 pipeline does NOT consult it — ready videos are ALWAYS auto-
// scheduled via autoScheduleVideo() below, with no manual Approve gate.

// ---------- v4: Global AI / global scheduler / auto-schedule / migration ----------
// V4 GLOBAL resolution (see header). resolveAiSettings(env, null) already
// means global-row + env only, because its profile row is null; this
// wrapper makes the intent explicit at every V4 call site. Per-profile
// ai_settings:<user_id> rows are NEVER passed here in V4.
async function resolveGlobalAiSettings(env) { return resolveAiSettings(env, null); }
// First profiles row = Mother Admin (existing rule, no hardcoded email).
async function adminUserId(env) {
  try { const r = await sb(env, 'profiles?select=user_id&order=created_at.asc&limit=1'); if (!r.ok) return null; const rows = await r.json(); return (Array.isArray(rows) && rows[0]) ? rows[0].user_id : null; } catch { return null; }
}
// V4 global gap: system_settings 'global_scheduler' {gap_minutes} >
// Admin profile settings.schedule_gap_minutes > 60. gap_minutes may be
// 30 / 60 / 120 or any custom positive value (cap 10080 = 1 week).
async function readGlobalSchedulerGap(env) {
  try { const v = await readSystemValue(env, 'global_scheduler'); const g = v && v.gap_minutes; if (typeof g === 'number' && g > 0 && g <= 10080) return Math.floor(g); } catch {}
  try { const aid = await adminUserId(env); if (aid) { const row = await settingsRowFor(env, aid); const g = row && row.schedule_gap_minutes; if (typeof g === 'number' && g > 0) return g; } } catch {}
  return 60;
}
// Global-row AI key presence only (V4 /drive-auth-status must NOT scan
// per-profile rows — those rows are not used for AI in V4).
async function globalAiKeyPresent(env) {
  const row = await readAiSettingsRow(env);
  if (!row) return false;
  for (const prov of ['gemini', 'openai']) { const e = row[prov] && row[prov].api_key_enc; if (typeof e === 'string' && e.length > 0) return true; }
  return false;
}
// Normal-user video view: ONLY the All-Videos modal fields + harmless
// file metadata. NEVER transcript, last_error, retry_count, ai_status,
// ai_confidence, drive_file_id, or file_hash. facebook_url is derived
// from facebook_post_id (no extra table/secret involved).
function normalVideoView(r) {
  return { id: r.id, user_id: r.user_id, page_id: r.page_id || null, file_name: r.file_name || null, file_size_bytes: r.file_size_bytes || null, mime_type: r.mime_type || null, duration_seconds: r.duration_seconds || null, title: r.title || null, description: r.description || null, caption: r.caption || null, hashtags: r.hashtags || null, status: r.status || null, scheduled_at: r.scheduled_at || null, facebook_post_id: r.facebook_post_id || null, facebook_url: r.facebook_post_id ? `https://www.facebook.com/${r.facebook_post_id}` : null, created_at: r.created_at || null };
}
// V4 auto-schedule (replaces Approve for normal users). Safe + idempotent:
//  - Skips already approved/scheduled/publishing/published videos and
//    videos that already have ANY publishing_queue row (no duplicates).
//  - Slot = max(existing max scheduled_at for THIS user + gap, now + gap),
//    so earlier times are never moved and each video gets +gap after the
//    previous one (sequential calls see the previous row just inserted).
//  - Idempotency key keeps the existing format pub-<video>-<scheduled_at>.
//  - page_id = video's page, else the owner's first page, else null.
//  - Video status moves ready/pending -> approved with scheduled_at set
//    (guarded PATCH: 0 rows = already moved, so no double-schedule).
//  - 'approved' here means 'auto-scheduled by V4', NOT a manual Approve.
async function autoScheduleVideo(env, video) {
  if (!video || !video.id || !video.user_id) return { scheduled: false, reason: 'invalid video' };
  if (['approved', 'scheduled', 'publishing', 'published'].includes(video.status)) return { scheduled: false, reason: 'already approved/scheduled/published' };
  try { const q = await sb(env, `publishing_queue?select=id&video_id=eq.${encodeURIComponent(video.id)}&limit=1`); if (q.ok) { const rows = await q.json(); if (Array.isArray(rows) && rows.length) return { scheduled: false, reason: 'queue row already exists' }; } } catch {}
  const gap = await readGlobalSchedulerGap(env);
  let base = new Date();
  try { const r = await sb(env, `publishing_queue?select=scheduled_at&user_id=eq.${encodeURIComponent(video.user_id)}&order=scheduled_at.desc&limit=1`); if (r.ok) { const rows = await r.json(); if (rows.length && rows[0].scheduled_at) base = new Date(rows[0].scheduled_at); } } catch {}
  const slot = new Date(Math.max(base.getTime() + gap * 60000, Date.now() + gap * 60000));
  const patch = await sb(env, `videos?id=eq.${encodeURIComponent(video.id)}&user_id=eq.${encodeURIComponent(video.user_id)}&status=in.(ready,pending)`, { method: 'PATCH', body: JSON.stringify({ status: 'approved', scheduled_at: slot.toISOString() }) });
  if (!patch.ok) return { scheduled: false, reason: 'video status update failed' };
  const changed = await patch.json().catch(() => []);
  if (!Array.isArray(changed) || !changed.length) return { scheduled: false, reason: 'video not ready/pending (already approved/scheduled/published)' };
  let pageId = video.page_id || null;
  if (!pageId) { try { const pr = await sb(env, `pages?select=id&user_id=eq.${encodeURIComponent(video.user_id)}&order=created_at.asc&limit=1`); if (pr.ok) { const ps = await pr.json(); if (ps.length) pageId = ps[0].id; } } catch {} }
  const idem = `pub-${video.id}-${slot.toISOString()}`;
  await sb(env, 'publishing_queue', { method: 'POST', headers: { Prefer: 'return=minimal,resolution=ignore-duplicates' }, body: JSON.stringify({ video_id: video.id, user_id: video.user_id, page_id: pageId, scheduled_at: slot.toISOString(), status: 'scheduled', idempotency_key: idem }) });
  return { scheduled: true, scheduled_at: slot.toISOString(), gap_minutes: gap, page_id: pageId };
}
// V4 migration, run on every Cron tick (best-effort, never breaks the tick).
// Bounded to 100 oldest pending/ready videos per tick so the tick stays
// within Worker limits; the next tick continues where this one stopped.
//  - Pending/ready video with NO ai_generated_contents and NO ai_process
//    job row at all -> create exactly ONE ai_process job (pending). It
//    never overwrites existing generated title/description (content is
//    only created when none exists, inside runAiPipeline).
//  - Ready video WITH content but no queue row -> auto-schedule (rule 2).
//    A pending video with content is first moved pending -> ready, then
//    auto-scheduled, so it cannot get stuck.
//  - Approved / scheduled / publishing / published videos are never
//    selected here (status filter) and keep status/schedule/post ID
//    untouched. Dead-letter / done jobs are NOT resurrected: a video that
//    already has any ai_process job row is skipped for job creation.
async function migrateLegacyVideos(env) {
  const out = { checked: 0, jobs_created: 0, auto_scheduled: 0, skipped: 0 };
  try {
    const r = await sb(env, 'videos?select=id,user_id,page_id,status&status=in.(pending,ready)&order=created_at.asc&limit=100');
    if (!r.ok) return { ...out, error: 'videos read failed' };
    const videos = await r.json();
    for (const v of videos) {
      out.checked++;
      let content = null;
      try { const cr = await sb(env, `ai_generated_contents?select=id,title,description&video_id=eq.${encodeURIComponent(v.id)}&limit=1`); if (cr.ok) { const rows = await cr.json(); if (rows.length && rows[0].title && rows[0].description) content = rows[0]; } } catch {}
      if (!content) {
        let hasJob = false;
        try { const jr = await sb(env, `background_jobs?select=id&job_type=eq.ai_process&video_id=eq.${encodeURIComponent(v.id)}&limit=1`); if (jr.ok) { const rows = await jr.json(); if (rows.length) hasJob = true; } } catch {}
        if (hasJob) { out.skipped++; continue; }
        const ins = await sb(env, 'background_jobs', { method: 'POST', headers: { Prefer: 'return=minimal' }, body: JSON.stringify({ job_type: 'ai_process', video_id: v.id, user_id: v.user_id, status: 'pending' }) });
        if (ins.ok) out.jobs_created++; else out.skipped++;
        continue;
      }
      if (v.status === 'pending') { await sb(env, `videos?id=eq.${encodeURIComponent(v.id)}&status=eq.pending`, { method: 'PATCH', body: JSON.stringify({ status: 'ready' }) }); v.status = 'ready'; }
      if (v.status === 'ready') { const res = await autoScheduleVideo(env, { ...v, status: 'ready' }); if (res.scheduled) out.auto_scheduled++; else out.skipped++; }
    }
  } catch (e) { out.error = e.message; }
  return out;
}
async function pipelineLanguageFor(env, userId) {
  const row = await settingsRowFor(env, userId);
  if (row && row.ai_language) return row.ai_language;
  return 'auto'; // NO fallback to another profile's row (isolation)
}
async function upsertSettings(env, userId, patch) {
  const r = await sb(env, `settings?user_id=eq.${encodeURIComponent(userId)}`, { method:'PATCH', body: JSON.stringify(patch) });
  if (r.ok) { const changed = await r.json().catch(() => []); if (Array.isArray(changed) && changed.length) return true; }
  const ins = await sb(env, 'settings', { method:'POST', headers:{ Prefer:'resolution=merge-duplicates' }, body: JSON.stringify({ user_id: userId, ...patch }) });
  return ins.ok;
}
// Deactivation (no profiles.status column exists in v1/v2 schema, so the
// flag lives in system_settings profile_status:<userId> {active:bool}).
// Missing row = active. Worker data endpoints check this; Supabase Auth
// token issuance itself is NOT blocked (see docs) — a deactivated account
// gets 401 on every Worker data call, which is what the site uses.
async function isProfileActive(env, userId) {
  if (!userId) return true;
  const v = await readSystemValue(env, 'profile_status:' + userId);
  if (v && v.active === false) return false;
  return true;
}
async function profileInfo(env, userId) {
  try {
    const r = await sb(env, `profiles?select=user_id,email,display_name,role,created_at&user_id=eq.${encodeURIComponent(userId)}&limit=1`);
    if (r.ok) { const rows = await r.json(); if (Array.isArray(rows) && rows[0]) return rows[0]; }
  } catch {}
  try {
    const r = await sb(env, `profiles?select=user_id,email,role,created_at&user_id=eq.${encodeURIComponent(userId)}&limit=1`);
    if (r.ok) { const rows = await r.json(); if (Array.isArray(rows) && rows[0]) return rows[0]; }
  } catch {}
  return null;
}
// Per-profile Drive folder on the ONE shared backend Drive (Mother OAuth).
// Priority: the profile's settings.drive_folder_id (explicit override) >
// a previously created ap-profile-<id> subfolder under the global FB Queue
// folder (remembered in system_settings drive_folder:<userId>) > create that
// subfolder now > null (caller falls back to the global folder).
// This is site-level + folder-level isolation, NOT a separate Google
// account per client (future feature, not claimed here).
async function profileDriveFolderId(env, userId) {
  try {
    const row = await settingsRowFor(env, userId);
    if (row && row.drive_folder_id) return row.drive_folder_id;
    const remembered = await readSystemValue(env, 'drive_folder:' + userId);
    if (remembered && remembered.folder_id) return remembered.folder_id;
    const dt = await driveToken(env);
    const name = 'ap-profile-' + String(userId).slice(0, 8);
    const q = `name='${name}' and mimeType='application/vnd.google-apps.folder' and trashed=false`;
    const found = await fetch('https://www.googleapis.com/drive/v3/files?q=' + encodeURIComponent(q) + '&fields=files(id,name)', { headers:{ Authorization:`Bearer ${dt}` } });
    if (found.ok) { const fj = await found.json().catch(() => ({})); if (fj.files && fj.files.length) { const fid = fj.files[0].id; await sb(env, 'system_settings', { method:'POST', headers:{ Prefer:'resolution=merge-duplicates' }, body: JSON.stringify({ key:'drive_folder:' + userId, value:{ folder_id: fid, name } }) }); return fid; } }
    const meta = { name, mimeType:'application/vnd.google-apps.folder' };
    if (env.DRIVE_FOLDER_ID) meta.parents = [env.DRIVE_FOLDER_ID];
    const cr = await fetch('https://www.googleapis.com/drive/v3/files', { method:'POST', headers:{ Authorization:`Bearer ${dt}`, 'Content-Type':'application/json' }, body: JSON.stringify(meta) });
    if (!cr.ok) return null;
    const created = await cr.json().catch(() => ({}));
    if (!created.id) return null;
    await sb(env, 'system_settings', { method:'POST', headers:{ Prefer:'resolution=merge-duplicates' }, body: JSON.stringify({ key:'drive_folder:' + userId, value:{ folder_id: created.id, name } }) });
    return created.id;
  } catch { return null; }
}
async function driveUploadSmall(env, name, mimeType, bytes, parents) {
  const dt = await driveToken(env);
  const meta = { name, mimeType };
  if (parents && parents.length) meta.parents = parents;
  const init = await fetch('https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable', { method:'POST', headers:{ Authorization:`Bearer ${dt}`, 'Content-Type':'application/json' }, body: JSON.stringify(meta) });
  const session = init.headers.get('Location'); if (!session) throw new Error('Drive upload session failed');
  const res = await fetch(session, { method:'PUT', headers:{ 'Content-Length': String(bytes.byteLength), 'Content-Range': `bytes 0-${bytes.byteLength - 1}/${bytes.byteLength}` }, body: bytes });
  if (!res.ok) throw new Error('Drive upload failed: HTTP ' + res.status);
  const j = await res.json().catch(() => ({}));
  if (!j.id) throw new Error('Drive upload failed: no file ID');
  return j.id;
}

// ---------- v3.1: SRT generation ----------
// SRT timing honesty (never present estimated times as measured):
//  - 'real': built from OpenAI Whisper verbose_json segments (start/end
//    timestamps returned by the transcription API itself).
//  - 'estimated': built by sentence-splitting a plain-text transcript and
//    distributing sentences over the video's known duration_seconds.
//    Sentence boundaries and per-sentence times are ESTIMATES, even though
//    the total duration is the real file duration. The Studio UI labels
//    estimated SRTs as such, and this code never claims word timestamps.
//  - If there are no segments AND duration is unknown, no SRT is produced
//    (a clear error is returned instead of invented timing).
// Storage (NO new Supabase table — schema-v2 is already done and must
// not be re-run): the SRT FILE is uploaded to Drive (into the video
// owner's per-profile folder) and only a small meta row is kept in
// system_settings under key srt_meta:<video_id> {video_id, user_id,
// drive_file_id, timing}. If the Drive upload or the meta save fails,
// saveSrt returns false and the pipeline still succeeds on transcript +
// generation; /srt then falls back to on-demand estimated SRT when a
// transcript + duration are known. For Whisper 'real' segments, a failed
// save means the real segments are lost (they are not re-derivable from
// plain text) — /srt then falls back to estimated timing, never fake
// 'real' timing.
function fmtSrtTime(sec) {
  const ms = Math.max(0, Math.round(sec * 1000));
  const p = (n, w) => String(n).padStart(w, '0');
  return `${p(Math.floor(ms / 3600000), 2)}:${p(Math.floor(ms % 3600000 / 60000), 2)}:${p(Math.floor(ms % 60000 / 1000), 2)},${p(ms % 1000, 3)}`;
}
function srtFromSegments(segments) {
  const segs = segments.map(x => ({ start: Number(x.start), end: Number(x.end), text: String(x.text || '').trim() }))
    .filter(x => x.text && Number.isFinite(x.start) && Number.isFinite(x.end) && x.start >= 0)
    .sort((a, b) => a.start - b.start);
  if (!segs.length) return null;
  return segs.map((x, i) => `${i + 1}\n${fmtSrtTime(x.start)} --> ${fmtSrtTime(x.end > x.start ? x.end : x.start + 1)}\n${x.text}`).join('\n\n') + '\n';
}
function splitSentences(transcript) {
  const flat = String(transcript).replace(/\s+/g, ' ').trim();
  if (!flat) return [];
  const parts = flat.match(/[^.!?\u0964]+[.!?\u0964]+|[^.!?\u0964]+$/g) || [flat];
  const out = [];
  for (const part of parts) {
    const t = part.trim(); if (!t) continue;
    if (t.length <= 200) { out.push(t); continue; }
    for (const sub of t.split(/[,;]\s+/)) { const st = sub.trim(); if (st) out.push(st); }
  }
  return out;
}
function srtEstimated(transcript, durationSeconds) {
  const sentences = splitSentences(transcript);
  const dur = Number(durationSeconds);
  if (!sentences.length || !dur || dur <= 0) return null;
  const total = sentences.reduce((a, x) => a + x.length, 0) || 1;
  const cues = [];
  let cursor = 0;
  for (let i = 0; i < sentences.length; i++) {
    if (cursor >= dur && cues.length) { cues[cues.length - 1].text += ' ' + sentences[i]; continue; }
    const last = i === sentences.length - 1;
    let end = last ? dur : cursor + dur * (sentences[i].length / total);
    if (!last && end <= cursor) end = cursor + 0.2;
    if (end > dur) end = dur;
    cues.push({ start: cursor, end, text: sentences[i] });
    cursor = end;
  }
  if (!cues.length) return null;
  return cues.map((c, i) => `${i + 1}\n${fmtSrtTime(c.start)} --> ${fmtSrtTime(c.end)}\n${c.text}`).join('\n\n') + '\n';
}
async function saveSrt(env, videoId, userId, srtText, timing) {
  try {
    let parents = [];
    try { const pf = await profileDriveFolderId(env, userId); if (pf) parents = [pf]; else if (env.DRIVE_FOLDER_ID) parents = [env.DRIVE_FOLDER_ID]; } catch { if (env.DRIVE_FOLDER_ID) parents = [env.DRIVE_FOLDER_ID]; }
    const base = 'video-' + String(videoId).slice(0, 12);
    const fileId = await driveUploadSmall(env, base + '.srt', 'text/plain', new TextEncoder().encode(srtText), parents);
    const up = await sb(env, 'system_settings', { method: 'POST', headers: { Prefer: 'resolution=merge-duplicates' }, body: JSON.stringify({ key: 'srt_meta:' + videoId, value: { video_id: videoId, user_id: userId, drive_file_id: fileId, timing } }) });
    return up.ok;
  } catch { return false; }
}
async function getStoredSrt(env, videoId) {
  try {
    const meta = await readSystemValue(env, 'srt_meta:' + videoId);
    if (!meta || !meta.drive_file_id) return null;
    const dt = await driveToken(env);
    const res = await fetch(`https://www.googleapis.com/drive/v3/files/${meta.drive_file_id}?alt=media`, { headers: { Authorization: `Bearer ${dt}` } });
    if (!res.ok) return null;
    const text = await res.text();
    return { srt_text: text, timing: meta.timing || null };
  } catch { return null; }
}
async function buildSrtForVideo(env, video, transcript, segments) {
  if (segments && segments.length) {
    const srt = srtFromSegments(segments);
    if (srt) { const saved = await saveSrt(env, video.id, video.user_id, srt, 'real'); return { available: saved, timing: saved ? 'real' : null, stored: saved }; }
  }
  const dur = Number(video.duration_seconds);
  if (dur && dur > 0 && transcript && transcript.trim()) {
    const srt = srtEstimated(transcript, dur);
    if (srt) { const saved = await saveSrt(env, video.id, video.user_id, srt, 'estimated'); return { available: true, timing: 'estimated', stored: saved }; }
  }
  return { available: false, timing: null, stored: false };
}

// ---------- v3.1: Automatic AI pipeline ----------
// Called by the Cron tick (processAiJobs, ONE job per tick) and by the
// Admin-only POST /ai-process endpoint. It never invents a transcript or
// content: every step needs a real API reply, and failures retry with the
// existing backoff (2/4/8/16/32 min) then dead-letter via payload flag.
async function approveVideoIds(env, videoIds, ownerId) {
  // V4: Admin manual-override helper only (pipeline no longer calls it).
  // Gap is the GLOBAL gap; base is THIS owner's max scheduled_at (not global).
  const gap = await readGlobalSchedulerGap(env);
  let base = new Date();
  try { const r = await sb(env, `publishing_queue?select=scheduled_at&user_id=eq.${encodeURIComponent(ownerId)}&order=scheduled_at.desc&limit=1`); if (r.ok) { const rows = await r.json(); if (rows.length && rows[0].scheduled_at) base = new Date(rows[0].scheduled_at); } } catch {}
  let approved = 0, skipped = 0; const results = [];
  for (const video_id of videoIds) {
    const when = new Date(base.getTime() + gap * 60000); base = when;
    const idem = `pub-${video_id}-${when.toISOString()}`;
    const r = await sb(env, `videos?id=eq.${encodeURIComponent(video_id)}&status=in.(pending,ready)`, { method: 'PATCH', body: JSON.stringify({ status: 'approved', scheduled_at: when.toISOString() }) });
    if (!r.ok) { skipped++; results.push({ video_id, status: 'skipped', reason: 'Supabase update failed' }); continue; }
    const changed = await r.json();
    if (!Array.isArray(changed) || !changed.length) { skipped++; results.push({ video_id, status: 'skipped', reason: 'row not pending/ready' }); continue; }
    const v = changed[0];
    await sb(env, 'publishing_queue', { method: 'POST', headers: { Prefer: 'return=minimal,resolution=ignore-duplicates' }, body: JSON.stringify({ video_id, user_id: v.user_id, page_id: v.page_id || null, scheduled_at: when.toISOString(), status: 'scheduled', idempotency_key: idem }) });
    approved++; results.push({ video_id, status: 'approved', scheduled_at: when.toISOString() });
  }
  return { approved, skipped, results, gap_minutes: gap };
}
async function markAiJobResult(env, job, err) {
  if (!err) { await sb(env, `background_jobs?id=eq.${job.id}`, { method: 'PATCH', body: JSON.stringify({ status: 'done', updated_at: new Date().toISOString() }) }); return 'done'; }
  const attempt = (job.attempt_count || 0) + 1;
  const permanent = /video not found|no Drive file/i.test(err.message || '');
  if (permanent || attempt >= MAX_ATTEMPTS) {
    await sb(env, `background_jobs?id=eq.${job.id}`, { method: 'PATCH', body: JSON.stringify({ status: 'failed', attempt_count: attempt, last_error: String(err.message).slice(0, 500), payload: { ...(job.payload || {}), dead_letter: true }, updated_at: new Date().toISOString() }) });
    return 'dead_letter';
  }
  const next = new Date(Date.now() + backoffMs(attempt)).toISOString();
  await sb(env, `background_jobs?id=eq.${job.id}`, { method: 'PATCH', body: JSON.stringify({ status: 'failed', attempt_count: attempt, last_error: String(err.message).slice(0, 500), payload: { ...(job.payload || {}), next_attempt_at: next }, updated_at: new Date().toISOString() }) });
  return 'failed_retry';
}
async function runAiPipeline(env, video) {
  const userId = video.user_id;
  let transcript = (video.transcript || '').trim();
  let language = 'auto', segments = null;
  if (!transcript) {
    try { const r = await sb(env, `video_transcripts?select=transcript,language,source&video_id=eq.${encodeURIComponent(video.id)}&limit=1`); if (r.ok) { const rows = await r.json(); if (rows.length && rows[0].transcript) { transcript = rows[0].transcript; language = rows[0].language || 'auto'; } } } catch {}
  }
  let transcribeProvider = null;
  if (!transcript) {
    if (!video.drive_file_id) throw new Error('Video has no Drive file ID — upload it first');
    const resolved = await resolveAiSettings(env, null); // V4: GLOBAL AI only — per-profile rows are never used for normal users.
    // v3.2: transcription fallback chain (active provider, then other
    // provider if configured). The chain logs each model/provider attempt;
    // do NOT log a second success entry here. Dead-lettering in
    // markAiJobResult only happens after this whole chain throws, so a
    // job is never dead-lettered while a selected model remains untried.
    const { bytes } = await fetchDriveVideo(env, video.drive_file_id);
    const res = await transcribeWithFallback(env, bytes, video.mime_type || 'video/mp4', resolved, userId);
    transcribeProvider = res.provider || resolved.active_provider;
    transcript = (res.transcript || '').trim(); segments = res.segments || null; language = res.language || 'auto';
    if (!transcript) throw new Error('Transcription returned an empty transcript — nothing to generate from.');
    const low = transcript.length < 20 || /inaudible|unclear|\[inaudible\]/i.test(transcript);
    await sb(env, 'video_transcripts', { method: 'POST', headers: { Prefer: 'resolution=merge-duplicates' }, body: JSON.stringify({ video_id: video.id, user_id: userId, transcript, language, confidence: null, confidence_flag: low ? 'low' : 'high', source: transcribeProvider }) });
    await sb(env, `videos?id=eq.${encodeURIComponent(video.id)}`, { method: 'PATCH', body: JSON.stringify({ transcript, ai_status: 'transcribed' }) });
    // (transcription attempts already logged by transcribeWithFallback)
  }
  let srtInfo = { available: false, timing: null, stored: false };
  try { const sr = await buildSrtForVideo(env, video, transcript, segments); if (sr) srtInfo = sr; } catch {}
  let content = null, genProvider = null, genModel = null, genTried = null;
  try { const r = await sb(env, `ai_generated_contents?select=title,description,caption,hashtags,provider,model&video_id=eq.${encodeURIComponent(video.id)}&limit=1`); if (r.ok) { const rows = await r.json(); if (rows.length && rows[0].title && rows[0].description) content = rows[0]; } } catch {}
  if (!content) {
    const lang = await pipelineLanguageFor(env, userId);
    const { cfg, parsed, tried } = await aiGenerate(env, transcript, lang, userId); genTried = tried;
    const valid = validateAi(parsed);
    try { const r = await sb(env, `ai_generated_contents?select=description&user_id=eq.${encodeURIComponent(userId)}&limit=1000`); if (r.ok) { const all = await r.json(); if (all.some(c => c.description && c.description === valid.description)) throw new Error('Generated description duplicates an existing video — regenerate instead of reusing.'); } } catch (e) { if (/duplicates/.test(e.message)) throw e; }
    const up = await sb(env, 'ai_generated_contents', { method: 'POST', headers: { Prefer: 'resolution=merge-duplicates' }, body: JSON.stringify({ video_id: video.id, user_id: userId, title: valid.title, description: valid.description, caption: valid.caption, hashtags: valid.hashtags, provider: cfg.provider, model: cfg.model, approved: false }) });
    if (!up.ok) throw new Error('AI content save failed');
    content = valid; genProvider = cfg.provider; genModel = cfg.model;
    // (generation attempts already logged by aiGenerate fallback chain)
  } else { genProvider = content.provider || null; genModel = content.model || null; }
  await sb(env, `videos?id=eq.${encodeURIComponent(video.id)}`, { method: 'PATCH', body: JSON.stringify({ title: content.title, description: content.description, caption: content.caption || undefined, hashtags: content.hashtags || undefined, ai_status: 'completed' }) });
  let becameReady = false;
  if (video.status === 'pending' || video.status === 'failed') {
    const r = await sb(env, `videos?id=eq.${encodeURIComponent(video.id)}&status=in.(pending,failed)`, { method: 'PATCH', body: JSON.stringify({ status: 'ready' }) });
    if (r.ok) { const changed = await r.json().catch(() => []); becameReady = Array.isArray(changed) && changed.length > 0; }
  }
  // V4: no Approve gate. A video that just became ready is ALWAYS
  // auto-scheduled to the next sequential GLOBAL-gap slot (rule 2).
  // Failure to schedule is logged but does NOT fail the AI job: the
  // Cron migration sweep will auto-schedule this ready video next tick.
  let autoScheduled = null;
  if (becameReady) { try { autoScheduled = await autoScheduleVideo(env, { ...video, id: video.id, user_id: userId, page_id: video.page_id, status: 'ready' }); } catch (e) { await logError(env, 'auto-schedule', e.message, video.id, userId); autoScheduled = { scheduled: false, reason: e.message }; } }
  return { transcript, language, provider: genProvider || transcribeProvider, model: genModel, content, srt: srtInfo, became_ready: becameReady, auto_approved: autoScheduled, auto_scheduled: autoScheduled, attempted_models: genTried || null };
}
async function processAiJobs(env, limit, ownerId) {
  const results = [];
  for (let i = 0; i < limit; i++) {
    let jobs = [];
    try { const ownerFilter = ownerId ? `&user_id=eq.${encodeURIComponent(ownerId)}` : ''; const r = await sb(env, `background_jobs?select=*&job_type=eq.ai_process&status=in.(pending,failed)${ownerFilter}&order=created_at.asc&limit=20`); if (!r.ok) return { ok: false, error: 'background_jobs read failed', results }; jobs = await r.json(); } catch (e) { return { ok: false, error: e.message, results }; }
    const nowMs = Date.now();
    const due = jobs.filter(j => {
      if (j.status === 'pending') return true;
      if (j.status !== 'failed') return false;
      if (j.payload && j.payload.dead_letter) return false; // dead-letter never re-runs on its own
      if (!j.payload || !j.payload.next_attempt_at) return true;
      return Date.parse(j.payload.next_attempt_at) <= nowMs;
    });
    if (!due.length) break;
    let job = null, claimed = [];
    for (const candidate of due) {
      if (candidate.user_id && !(await isProfileActive(env, candidate.user_id))) continue; // deactivated profile: skip, never process
      const claim = await sb(env, `background_jobs?id=eq.${candidate.id}&status=in.(pending,failed)`, { method: 'PATCH', body: JSON.stringify({ status: 'running', updated_at: new Date().toISOString() }) });
      claimed = claim.ok ? await claim.json().catch(() => []) : [];
      if (Array.isArray(claimed) && claimed.length) { job = candidate; break; }
    }
    if (!job) break;
    const vr = await sb(env, `videos?select=${PIPELINE_VIDEO_COLS}&id=eq.${encodeURIComponent(job.video_id)}&limit=1`);
    if (!vr.ok) { const st = await markAiJobResult(env, job, new Error('Video read failed')); results.push({ job_id: job.id, video_id: job.video_id, status: st }); continue; }
    const videos = await vr.json();
    if (!videos.length) { const st = await markAiJobResult(env, job, new Error('Video not found')); results.push({ job_id: job.id, video_id: job.video_id, status: st }); continue; }
    try {
      const out = await runAiPipeline(env, videos[0]);
      const st = await markAiJobResult(env, job, null);
      results.push({ job_id: job.id, video_id: job.video_id, status: st, provider: out.provider, became_ready: out.became_ready, auto_approved: out.auto_approved, auto_scheduled: out.auto_scheduled });
    } catch (e) {
      await logError(env, 'ai', e.message, job.video_id, job.user_id);
      const st = await markAiJobResult(env, job, e);
      results.push({ job_id: job.id, video_id: job.video_id, status: st, error: e.message });
    }
  }
  return { ok: true, processed: results.length, results };
}


// ---------- AI provider manager: Gemini + OpenAI, transcript is source of truth ----------
// HONEST LIMIT, documented in README: a Worker cannot run ffmpeg, so it cannot
// extract audio from a video binary by itself. /ai-generate therefore requires
// a transcript (pasted, or produced by an external transcription step) and
// generates title/description/caption/hashtags FROM THAT TRANSCRIPT ONLY.
// It never invents content and never returns success without a real API reply.
// If no provider key is set, it returns a clear 503 — never fake content.
async function aiConfig(env, userId) {
  // V4: userId is kept ONLY for api_usage_logs attribution. AI settings
  // resolution is GLOBAL only (global row > env); per-profile rows ignored.
  const s = await resolveAiSettings(env, null);
  const c = s[s.active_provider];
  return { provider: s.active_provider, key: c.key, model: c.model, base_url: c.base_url, selected_models: selectedModelsFor(c), resolved: s };
}
function aiPrompt(transcript, language) {
  return `You are writing Facebook publishing copy from a real video transcript. Use ONLY facts present in the transcript. Do not invent people, places, events, or claims not in the transcript. If the transcript is unclear, keep the copy generic about the visible topic, never fabricated. Language preference: ${language || 'English only'}. Transcript:\n"""${transcript.slice(0, 12000)}"""\n\nReturn ONLY valid JSON with keys: title (short, natural, no clickbait), description (2-4 sentences: hook + what the viewer will see/learn + call to action), caption (1-2 engaging sentences for Facebook), hashtags (array of 3-6 relevant tags without the # sign). No markdown, no extra keys.`;
}
// Single model attempt (internal). Throws on any API/parse failure so
// the fallback wrapper can move to the next selected model.
async function aiGenerateOnce(env, transcript, language, provider, key, base_url, model) {
  const prompt = aiPrompt(transcript, language);
  if (provider === 'openai') {
    const r = await fetch(openaiUrl(base_url, '/v1/chat/completions'), { method:'POST', headers:{ Authorization:`Bearer ${key}`, 'Content-Type':'application/json' }, body: JSON.stringify({ model, messages:[{role:'user', content: prompt}], temperature: 0.7 }) });
    const j = await r.json().catch(() => ({})); const text = j.choices && j.choices[0] && j.choices[0].message && j.choices[0].message.content;
    if (!r.ok || !text) throw new Error('OpenAI generation failed (' + model + '): ' + JSON.stringify(j).slice(0,300));
    return { parsed: JSON.parse(text.replace(/^```json|```$/gm,'').trim()), usage: j.usage || null };
  }
  const r = await fetch(`${geminiUrl(base_url, '/v1beta')}/models/${encodeURIComponent(model)}:generateContent?key=${encodeURIComponent(key)}`, { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ contents:[{ parts:[{ text: prompt }] }] }) });
  const j = await r.json().catch(() => ({})); const text = j.candidates && j.candidates[0] && j.candidates[0].content && j.candidates[0].content.parts && j.candidates[0].content.parts[0] && j.candidates[0].content.parts[0].text;
  if (!r.ok || !text) throw new Error('Gemini generation failed (' + model + '): ' + JSON.stringify(j).slice(0,300));
  return { parsed: JSON.parse(text.replace(/^```json|```$/gm,'').trim()), usage: j.usageMetadata || null };
}
// v3.2 fallback: try ONLY the selected models of the active provider, in
// order, waiting MODEL_FALLBACK_DELAY_MS (3500ms) between attempts. Every
// attempt is logged to api_usage_logs (provider, model, success, truncated
// error, NO key). On success the successful model is returned and the
// caller saves it; on total failure a clear error lists every model tried
// and no fake content is saved by any caller.
async function aiGenerate(env, transcript, language, userId) {
  const cfg = await aiConfig(env, userId);
  if (!cfg.key) throw new Error('AI provider not configured: set a key in AI Settings, or set GEMINI_API_KEY / OPENAI_API_KEY in Worker secrets. No content was generated.');
  if (!transcript || !transcript.trim()) throw new Error('AI generation needs a transcript first. Paste a transcript or run transcription, then regenerate.');
  const models = selectedModelsFor({ model: cfg.model, selected_models: cfg.selected_models });
  const tried = [];
  let lastErr = null;
  for (let i = 0; i < models.length; i++) {
    const model = models[i];
    try {
      const { parsed, usage } = await aiGenerateOnce(env, transcript, language, cfg.provider, cfg.key, cfg.base_url, model);
      await logUsage(env, cfg.provider, 'content_generate', model, true, null, userId);
      tried.push({ model, ok: true });
      return { cfg: { provider: cfg.provider, key: cfg.key, model, base_url: cfg.base_url, selected_models: models }, parsed, usage, tried, successful_model: model };
    } catch (e) {
      lastErr = e;
      await logUsage(env, cfg.provider, 'content_generate', model, false, e.message, userId);
      tried.push({ model, ok: false, error: truncateErr(e, 300) });
      if (i < models.length - 1) await sleepMs(MODEL_FALLBACK_DELAY_MS);
    }
  }
  const triedList = tried.map(t => `${t.model} (${t.error || 'failed'})`).join('; ');
  throw new Error(`All selected models failed for ${cfg.provider} (${models.length} tried): ${triedList}. No content was generated.`);
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
async function transcribeWithGemini(env, bytes, mimeType, cfg) {
  const key = cfg ? cfg.key : env.GEMINI_API_KEY;
  const base = cfg ? cfg.base_url : AI_DEFAULTS.gemini.base_url;
  const model = cfg ? cfg.model : 'gemini-2.5-flash';
  // Step 1: resumable upload session to Gemini Files API.
  const start = await fetch(`${stripTrailingSlash(base)}/upload/v1beta/files?key=${encodeURIComponent(key)}`, {
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
    const chk = await fetch(`${geminiUrl(base, '/v1beta')}/${fileName}?key=${encodeURIComponent(key)}`);
    const cj = await chk.json().catch(() => ({}));
    state = cj.state || state; fileUri = cj.uri || fileUri;
    if (cj.state === 'FAILED') throw new Error('Gemini file processing failed for transcription.');
  }
  if (state !== 'ACTIVE' || !fileUri) throw new Error('Gemini file did not become ACTIVE within the polling cap — retry /transcribe (the file may still activate).');
  // Step 4: ask for a VERBATIM transcript, auto-detecting language.
  const prompt = 'Transcribe ALL speech in this video verbatim, word for word. Do not summarise, do not translate, do not add words that were not spoken. Detect the spoken language automatically: it may be Bengali, English, or mixed Bengali-English — report it exactly. If speech is unclear or inaudible at any point, write [inaudible] there instead of guessing. Return ONLY valid JSON with keys: transcript (the full verbatim text), language (one of: bn, en, bn-en, other), confidence_note (one short sentence, e.g. clear / some parts unclear). No markdown, no extra keys.';
  const gen = await fetch(`${geminiUrl(base, '/v1beta')}/models/${encodeURIComponent(model)}:generateContent?key=${encodeURIComponent(key)}`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ contents: [{ parts: [{ fileData: { mimeType: mimeType || 'video/mp4', fileUri } }, { text: prompt }] }] })
  });
  const gj = await gen.json().catch(() => ({}));
  const text = gj.candidates && gj.candidates[0] && gj.candidates[0].content && gj.candidates[0].content.parts && gj.candidates[0].content.parts[0] && gj.candidates[0].content.parts[0].text;
  if (!gen.ok || !text) throw new Error('Gemini transcription failed: ' + JSON.stringify(gj).slice(0, 300));
  try { const parsed = JSON.parse(text.replace(/^```json|```$/gm, '').trim()); return { transcript: String(parsed.transcript || ''), language: String(parsed.language || 'auto'), confidence_note: String(parsed.confidence_note || ''), segments: null }; }
  catch { return { transcript: text, language: 'auto', confidence_note: '', segments: null }; } // model returned raw text, not JSON — still a real transcript
}
// v3.2 Gemini transcription fallback: upload + poll ACTIVE once (file
// handling is not model-specific), then try generateContent with ONLY
// the selected Gemini models, in order, 3500ms apart. Each model attempt
// is logged. Upload/poll failures are NOT model failures and throw at
// once (no point retrying the same file upload per model).
async function transcribeWithGeminiFallback(env, bytes, mimeType, cfg) {
  const key = cfg ? cfg.key : env.GEMINI_API_KEY;
  const base = cfg ? cfg.base_url : AI_DEFAULTS.gemini.base_url;
  const models = cfg ? selectedModelsFor(cfg) : ['gemini-2.5-flash'];
  const start = await fetch(`${stripTrailingSlash(base)}/upload/v1beta/files?key=${encodeURIComponent(key)}`, {
    method: 'POST',
    headers: { 'X-Goog-Upload-Protocol': 'resumable', 'X-Goog-Upload-Command': 'start', 'X-Goog-Upload-Header-Content-Length': String(bytes.byteLength), 'X-Goog-Upload-Header-Content-Type': mimeType || 'video/mp4', 'Content-Type': 'application/json' },
    body: JSON.stringify({ file: { displayName: 'autopublish-transcribe' } })
  });
  const uploadUrl = start.headers.get('X-Goog-Upload-URL');
  if (!uploadUrl) throw new Error('Gemini Files upload session failed: ' + (await start.text()).slice(0, 300));
  const up = await fetch(uploadUrl, { method: 'POST', headers: { 'X-Goog-Upload-Command': 'upload, finalize', 'Content-Length': String(bytes.byteLength), 'Content-Type': mimeType || 'video/mp4' }, body: bytes });
  const upJson = await up.json().catch(() => ({}));
  if (!up.ok || !upJson.file || !upJson.file.name) throw new Error('Gemini Files upload failed: ' + JSON.stringify(upJson).slice(0, 300));
  const fileName = upJson.file.name;
  let fileUri = upJson.file.uri || null, state = upJson.file.state || 'PROCESSING';
  for (let i = 0; i < 30 && state !== 'ACTIVE'; i++) {
    await sleep(2000);
    const chk = await fetch(`${geminiUrl(base, '/v1beta')}/${fileName}?key=${encodeURIComponent(key)}`);
    const cj = await chk.json().catch(() => ({}));
    state = cj.state || state; fileUri = cj.uri || fileUri;
    if (cj.state === 'FAILED') throw new Error('Gemini file processing failed for transcription.');
  }
  if (state !== 'ACTIVE' || !fileUri) throw new Error('Gemini file did not become ACTIVE within the polling cap — retry /transcribe (the file may still activate).');
  const prompt = 'Transcribe ALL speech in this video verbatim, word for word. Do not summarise, do not translate, do not add words that were not spoken. Detect the spoken language automatically: it may be Bengali, English, or mixed Bengali-English — report it exactly. If speech is unclear or inaudible at any point, write [inaudible] there instead of guessing. Return ONLY valid JSON with keys: transcript (the full verbatim text), language (one of: bn, en, bn-en, other), confidence_note (one short sentence, e.g. clear / some parts unclear). No markdown, no extra keys.';
  const tried = [];
  for (let i = 0; i < models.length; i++) {
    const model = models[i];
    try {
      const gen = await fetch(`${geminiUrl(base, '/v1beta')}/models/${encodeURIComponent(model)}:generateContent?key=${encodeURIComponent(key)}`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ contents: [{ parts: [{ fileData: { mimeType: mimeType || 'video/mp4', fileUri } }, { text: prompt }] }] })
      });
      const gj = await gen.json().catch(() => ({}));
      const text = gj.candidates && gj.candidates[0] && gj.candidates[0].content && gj.candidates[0].content.parts && gj.candidates[0].content.parts[0] && gj.candidates[0].content.parts[0].text;
      if (!gen.ok || !text) throw new Error('Gemini transcription failed (' + model + '): ' + JSON.stringify(gj).slice(0, 300));
      tried.push({ model, ok: true });
      try { const parsed = JSON.parse(text.replace(/^```json|```$/gm, '').trim()); return { transcript: String(parsed.transcript || ''), language: String(parsed.language || 'auto'), confidence_note: String(parsed.confidence_note || ''), segments: null, provider: 'gemini', model, tried }; }
      catch { return { transcript: text, language: 'auto', confidence_note: '', segments: null, provider: 'gemini', model, tried }; }
    } catch (e) {
      tried.push({ model, ok: false, error: truncateErr(e, 300) });
      if (i < models.length - 1) await sleepMs(MODEL_FALLBACK_DELAY_MS);
      else { const err = new Error(`All selected Gemini models failed for transcription (${models.length} tried): ${tried.map(t => `${t.model} (${t.error || 'failed'})`).join('; ')}`); err.tried = tried; throw err; }
    }
  }
  throw new Error('Gemini transcription: no models selected.');
}
// v3.2 transcription fallback across providers:
//   Stage 1: active provider — Gemini: selected-model chain above;
//            OpenAI: single Whisper attempt (whisper-1, NOT a chat model).
//   Stage 2 (only if stage 1 failed AND the other provider is configured):
//            the other provider, same rules. 3500ms between stages.
// Every attempt is logged (provider, model, success, truncated error, NO
// key) by the caller via logTranscriptionAttempts() below, except the
// Gemini chain which logs its own attempts through that same helper —
// callers must NOT log a second success/failure entry for the same attempt.
async function transcribeWithFallback(env, bytes, mimeType, resolved, userId) {
  const stages = [];
  const active = resolved.active_provider;
  const other = active === 'gemini' ? 'openai' : 'gemini';
  const activeCfg = resolved[active];
  if (activeCfg && activeCfg.key) stages.push(active);
  const otherCfg = resolved[other];
  if (otherCfg && otherCfg.key) stages.push(other);
  if (!stages.length) throw new Error('AI not configured: set a Gemini or OpenAI key in AI Settings, or set GEMINI_API_KEY / OPENAI_API_KEY in Worker secrets. No transcript was generated.');
  const allTried = [];
  let lastErr = null;
  for (let si = 0; si < stages.length; si++) {
    const prov = stages[si];
    const cfg = resolved[prov];
    try {
      if (prov === 'gemini') {
        const res = await transcribeWithGeminiFallback(env, bytes, mimeType, cfg);
        for (const t of res.tried) await logUsage(env, 'gemini', 'transcribe', t.model, t.ok, t.ok ? null : t.error, userId);
        return { ...res, tried: res.tried, stages_tried: stages.slice(0, si + 1) };
      }
      // OpenAI Whisper: one transcription model, not a chat-model list.
      const wModel = env.OPENAI_TRANSCRIPTION_MODEL || 'whisper-1';
      try {
        const res = await transcribeWithOpenAI(env, bytes, mimeType, cfg);
        await logUsage(env, 'openai', 'transcribe', wModel, true, null, userId);
        return { ...res, provider: 'openai', model: wModel, tried: [{ model: wModel, ok: true }], stages_tried: stages.slice(0, si + 1) };
      } catch (e) {
        await logUsage(env, 'openai', 'transcribe', wModel, false, e.message, userId);
        allTried.push({ provider: 'openai', model: wModel, ok: false, error: truncateErr(e, 300) });
        lastErr = e;
      }
    } catch (e) {
      if (prov === 'gemini' && Array.isArray(e.tried) && e.tried.length) {
        for (const t of e.tried) { await logUsage(env, 'gemini', 'transcribe', t.model, t.ok, t.ok ? null : t.error, userId); allTried.push({ provider: 'gemini', model: t.model, ok: t.ok, error: t.error || null }); }
      } else {
        await logUsage(env, prov, 'transcribe', prov === 'gemini' ? cfg.model : (env.OPENAI_TRANSCRIPTION_MODEL || 'whisper-1'), false, e.message, userId);
        allTried.push({ provider: prov, model: prov === 'gemini' ? cfg.model : (env.OPENAI_TRANSCRIPTION_MODEL || 'whisper-1'), ok: false, error: truncateErr(e, 300) });
      }
      lastErr = e;
    }
    if (si < stages.length - 1) await sleepMs(MODEL_FALLBACK_DELAY_MS);
  }
  throw new Error(`Transcription failed on all configured providers (${stages.join(', ')}): ${truncateErr(lastErr, 300)}. No transcript was generated.`);
}
async function transcribeWithOpenAI(env, bytes, mimeType, cfg) {
  const base = cfg ? cfg.base_url : AI_DEFAULTS.openai.base_url;
  // Whisper accepts mp4 directly; omitting `language` = auto-detect.
  const form = new FormData();
  form.append('file', new Blob([bytes], { type: mimeType || 'video/mp4' }), 'video.mp4');
  form.append('model', env.OPENAI_TRANSCRIPTION_MODEL || 'whisper-1');
  // verbose_json returns segments with real start/end timestamps, used
  // for 'real' SRT timing. Plain json would give text only.
  form.append('response_format', 'verbose_json');
  const r = await fetch(openaiUrl(base, '/v1/audio/transcriptions'), { method: 'POST', headers: { Authorization: `Bearer ${cfg ? cfg.key : env.OPENAI_API_KEY}` }, body: form });
  const j = await r.json().catch(() => ({}));
  if (!r.ok || !j.text) throw new Error('OpenAI Whisper transcription failed: ' + JSON.stringify(j).slice(0, 300));
  const segments = Array.isArray(j.segments) ? j.segments.map(x => ({ start: x.start, end: x.end, text: x.text })) : null;
  return { transcript: String(j.text), language: String(j.language || 'auto'), confidence_note: '', segments };
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
    await sb(env, 'heartbeat', { method:'POST', body: JSON.stringify({ checked_at: now, due_count: Array.isArray(due)?due.length:0, note:'publisher tick (Pro v4)' }) });
    await sb(env, `heartbeat?checked_at=lt.${new Date(Date.now()-7*864e5).toISOString()}`, { method:'DELETE' });
  } catch (e) { console.log('heartbeat error', e.message); }
  // V4 migration FIRST (bounded, best-effort, never breaks the tick):
  // legacy pending/ready videos get one ai_process job, ready videos with
  // content get auto-scheduled. Newly created jobs may then be picked up
  // by processAiJobs below (1 job/tick, as before).
  let migrationResult = null;
  try { migrationResult = await migrateLegacyVideos(env); } catch (e) { migrationResult = { ok: false, error: e.message }; await logError(env, 'migration', e.message); }
  // v3.1: AI processing runs on every tick, even when publishing is paused
  // (pause stops Facebook publishing, not AI preparation). ONE AI job per
  // tick — transcription + generation can take a minute, so bulk queues
  // drain gradually, one job per Cron tick, never in parallel here.
  let aiResult = null;
  try { aiResult = await processAiJobs(env, 1); } catch (e) { aiResult = { ok: false, error: e.message }; await logError(env, 'ai', e.message); }
  // v3.1 isolation: pause is PER PROFILE — a paused (or deactivated)
  // profile's jobs are skipped, other profiles still publish on this tick.
  const nowIso = new Date().toISOString();
  // 2. Claim ONE due queue row. The status filter IS the job lock: a second
  // run cannot claim a row already moved to 'processing'.
  const qRes = await sb(env, `publishing_queue?select=*,videos(*)&status=in.(scheduled,retry_pending)&scheduled_at=lte.${nowIso}&order=scheduled_at.asc&limit=10`);
  if (!qRes.ok) { await logError(env, 'scheduler', 'publishing_queue read failed: ' + (await qRes.text()).slice(0,300)); return { ok:false }; }
  const jobs = await qRes.json(); if (!jobs.length) return { ok:true, due:0, ai: aiResult, migration: migrationResult };
  let job = null, video = {};
  for (const candidate of jobs) {
    if (candidate.user_id && !(await isProfileActive(env, candidate.user_id))) continue; // deactivated profile: never publish
    if (candidate.user_id && await schedulerPausedFor(env, candidate.user_id)) continue; // paused profile: skip, others still publish
    const claim = await sb(env, `publishing_queue?id=eq.${candidate.id}&status=in.(scheduled,retry_pending)`, { method:'PATCH', body: JSON.stringify({ status:'processing', updated_at: nowIso }) });
    const claimed = await claim.json().catch(()=>[]);
    if (Array.isArray(claimed) && claimed.length) { job = candidate; video = candidate.videos || {}; break; }
  }
  if (!job) return { ok:true, due:0, ai: aiResult, migration: migrationResult, skipped:'paused, deactivated, or already claimed' };
  await sb(env, `videos?id=eq.${video.id}`, { method:'PATCH', body: JSON.stringify({ status:'publishing' }) });
  try {
    // 3. Resolve page token from pages (v1) or connected_facebook_pages (v2).
    let page = null;
    if (job.page_id) { const pr = await sb(env, `pages?select=*&id=eq.${job.page_id}`); const ps = pr.ok ? await pr.json() : []; page = ps[0] || null; }
    if (!page && video.page_id) { const pr = await sb(env, `pages?select=*&id=eq.${video.page_id}`); const ps = pr.ok ? await pr.json() : []; page = ps[0] || null; }
    if (page && page.user_id && page.user_id !== job.user_id) page = null; // a page from another profile must never publish this video
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
    return { ok:true, published: video.id, facebook_post_id: fbId, ai: aiResult, migration: migrationResult };
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
    return { ok:false, error: e.message, ai: aiResult, migration: migrationResult };
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
    if (url.pathname === '/auth-me' && request.method === 'GET') { const me = await verifySession(env, request); if (!me) return json({ error:'login required' }, 401); if (!(await isProfileActive(env, me.id))) return json({ error:'account deactivated by admin' }, 401); return json({ user: me, role: await profileRole(env, me.id) }); }
    if (url.pathname === '/auth-logout' && request.method === 'POST') return json({ ok:true });

    // ---- Drive OAuth (Mother Admin, admin-only in V4) ----
    // /drive-auth-callback MUST stay public: it is Google's redirect
    // target and carries no Bearer token. It only shows the refresh
    // token to the browser that just completed Admin consent; the token
    // itself is never stored by the Worker — Admin saves it as a
    // Cloudflare secret. /drive-auth-start and /drive-auth-status are
    // admin-only and live AFTER the auth check below (V4 gate).
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
    // ---- All remaining endpoints require login ----
    const me = await verifySession(env, request);
    if (!me) {
      // Public root message only; every data endpoint below needs auth.
      if (url.pathname === '/' || url.pathname === '') return new Response('AutoPublish Studio Pro worker — runs on hourly Cron, not browser visits.');
      return json({ error:'login required' }, 401);
    }
    const isAdmin = (await profileRole(env, me.id)) === 'admin';
    // v3.1 isolation: DEFAULT workspace = the caller's OWN user_id, for
    // everyone including Admin. Admin management view = an explicit
    // ?profile_id= (validated against profiles; invalid -> 404 for every
    // eff-scoped endpoint). Clients' profile_id params are ignored.
    if (!(await isProfileActive(env, me.id))) return json({ error:'account deactivated by admin' }, 401);
    const effParam = url.searchParams.get('profile_id');
    let eff = me.id, effInvalid = false;
    if (isAdmin && effParam) {
      const r = await profileRole(env, effParam);
      if (r) eff = effParam; else effInvalid = true;
    }
    const effCheck = () => effInvalid ? json({ error:'profile not found' }, 404) : null;


    // ---- V4 role gating (backend-enforced; frontend hiding is NOT enough) ----
    // Normal users get 403 on every path below, whatever the frontend shows.
    // /admin/* (incl. the new /admin/video) is admin-only by prefix check.
    const V4_ADMIN_ONLY_PATHS = new Set(['/ai-settings', '/pipeline', '/srt', '/transcripts', '/ai-content', '/ai-generate', '/transcribe', '/ai-retry', '/ai-process', '/errors', '/drive-auth-status', '/drive-auth-start', '/publish-now', '/approve', '/scheduler', '/scheduler/pause', '/scheduler/resume']);
    if ((!isAdmin && V4_ADMIN_ONLY_PATHS.has(url.pathname)) || (!isAdmin && url.pathname.startsWith('/admin/'))) return json({ error: 'only Mother Admin can use this endpoint' }, 403);

    // ---- Drive OAuth start / status (Admin only in V4; global Drive) ----
    if (url.pathname === '/drive-auth-start' && request.method === 'GET') {
      if (!env.GOOGLE_OAUTH_CLIENT_ID) return new Response('Google OAuth client not configured: set GOOGLE_OAUTH_CLIENT_ID and GOOGLE_OAUTH_CLIENT_SECRET in Worker secrets first.', { status: 500 });
      const redirect = `${url.origin}/drive-auth-callback`;
      const auth = new URL('https://accounts.google.com/o/oauth2/v2/auth');
      auth.searchParams.set('client_id', env.GOOGLE_OAUTH_CLIENT_ID); auth.searchParams.set('redirect_uri', redirect);
      auth.searchParams.set('response_type', 'code'); auth.searchParams.set('scope', 'https://www.googleapis.com/auth/drive.file');
      auth.searchParams.set('access_type', 'offline'); auth.searchParams.set('prompt', 'consent');
      return Response.redirect(auth.toString(), 302);
    }
    if (url.pathname === '/drive-auth-status' && request.method === 'GET') {
      // V4: ai_configured reflects GLOBAL AI row + env only (per-profile
      // rows are not used for AI in V4, so they must not flip this flag).
      let dbAi = false, dbProvider = null;
      try { dbAi = await globalAiKeyPresent(env); const row = await readAiSettingsRow(env); if (row && (row.active_provider === 'gemini' || row.active_provider === 'openai')) dbProvider = row.active_provider; } catch {}
      return json({ oauth_client_configured: Boolean(env.GOOGLE_OAUTH_CLIENT_ID && env.GOOGLE_OAUTH_CLIENT_SECRET), oauth_refresh_token_configured: Boolean(env.GOOGLE_OAUTH_REFRESH_TOKEN), drive_folder_configured: Boolean(env.DRIVE_FOLDER_ID), service_account_configured: Boolean(env.GOOGLE_SERVICE_ACCOUNT_JSON), ai_configured: Boolean(env.GEMINI_API_KEY || env.OPENAI_API_KEY || dbAi), ai_provider: dbProvider || (env.AI_PROVIDER || 'gemini'), token_encryption_configured: Boolean(env.TOKEN_KEY) });
    }

    // ---- V4: Scheduler info for normal users (view-only; NO keys, NO system settings) ----
    if (url.pathname === '/scheduler-info' && request.method === 'GET') {
      try {
        if (effInvalid) return json({ error: 'profile not found' }, 404);
        const gap = await readGlobalSchedulerGap(env);
        const r = await sb(env, `publishing_queue?select=id&user_id=eq.${encodeURIComponent(eff)}&status=eq.scheduled`);
        const count = r.ok ? (await r.json()).length : 0;
        return json({ gap_minutes: gap, scheduled_count: count, paused: await schedulerPausedFor(env, eff) });
      } catch (e) { return json({ error: e.message }, 500); }
    }

    // ---- V4: Admin video detail (admin only; full row incl. transcript) ----
    // Normal users NEVER get this endpoint (V4 gate above returns 403).
    // Admin management view: video must belong to the selected profile,
    // otherwise 404 (never leak another profile's transcript/logs).
    if (url.pathname === '/admin/video' && request.method === 'GET') {
      try {
        if (effInvalid) return json({ error: 'profile not found' }, 404);
        const vid = url.searchParams.get('id') || url.searchParams.get('video_id');
        if (!vid) return json({ error: 'id (or video_id) is required' }, 400);
        const rows = await sbList(`videos?select=${VIDEO_COLS}&id=eq.${encodeURIComponent(vid)}&limit=1`);
        if (!rows.length || rows[0].user_id !== eff) return json({ error: 'video not found' }, 404);
        const video = rows[0];
        const transcripts = await sbList(`video_transcripts?select=transcript,language,confidence_flag,source,created_at&video_id=eq.${encodeURIComponent(vid)}&limit=1`).catch(() => []);
        const contents = await sbList(`ai_generated_contents?select=title,description,caption,hashtags,provider,model,approved&video_id=eq.${encodeURIComponent(vid)}&limit=1`).catch(() => []);
        const jobs = await sbList(`background_jobs?select=id,job_type,status,attempt_count,last_error,created_at&job_type=eq.ai_process&video_id=eq.${encodeURIComponent(vid)}&order=created_at.desc&limit=5`).catch(() => []);
        const errs = await sbList(`error_logs?select=source,message,created_at&video_id=eq.${encodeURIComponent(vid)}&order=created_at.desc&limit=20`).catch(() => []);
        let srtTiming = null;
        try { const meta = await readSystemValue(env, 'srt_meta:' + vid); if (meta) srtTiming = meta.timing || null; } catch {}
        return json({ video, transcript: transcripts[0] || null, ai_content: contents[0] || null, ai_jobs: jobs, errors: errs, srt_timing: srtTiming });
      } catch (e) { return json({ error: e.message }, 500); }
    }

    // ---- Videos: all / queue / history (real DB rows, token-free) ----
    if (url.pathname === '/videos' && request.method === 'GET') {
      try {
        const status = url.searchParams.get('status'); const pageId = url.searchParams.get('page_id'); const q = url.searchParams.get('q');
        if (effInvalid) return json({ error:'profile not found' }, 404);
        let path = `videos?select=${VIDEO_COLS}&user_id=eq.${encodeURIComponent(eff)}&order=created_at.desc&limit=1000`;
        if (status) path = `videos?select=${VIDEO_COLS}&user_id=eq.${encodeURIComponent(eff)}&status=eq.${encodeURIComponent(status)}&order=created_at.desc&limit=1000`;
        if (pageId) path += `&page_id=eq.${encodeURIComponent(pageId)}`;
        const rows = await sbList(path);
        const filtered = q ? rows.filter(r => (r.file_name||'').toLowerCase().includes(q.toLowerCase()) || (r.title||'').toLowerCase().includes(q.toLowerCase())) : rows;
        return json({ videos: isAdmin ? filtered : filtered.map(normalVideoView) });
      } catch (e) { return json({ error: e.message }, 500); }
    }
    if (url.pathname === '/queue' && request.method === 'GET') {
      try { if (effInvalid) return json({ error:'profile not found' }, 404); const rows = await sbList(`videos?select=${VIDEO_COLS}&user_id=eq.${encodeURIComponent(eff)}&status=in.(pending,ready,approved)&order=created_at.asc&limit=500`); return json({ videos: isAdmin ? rows : rows.map(normalVideoView) }); } catch (e) { return json({ error: e.message }, 500); }
    }
    if (url.pathname === '/publishing-queue' && request.method === 'GET') {
      try { if (effInvalid) return json({ error:'profile not found' }, 404); const rows = await sbList(`publishing_queue?select=*,videos(file_name,title,status)&user_id=eq.${encodeURIComponent(eff)}&order=scheduled_at.asc&limit=1000`); return json({ queue: rows }); } catch (e) { return json({ error: e.message }, 500); }
    }
    if (url.pathname === '/history' && request.method === 'GET') {
      try { if (effInvalid) return json({ error:'profile not found' }, 404); const rows = await sbList(`publishing_history?select=*&user_id=eq.${encodeURIComponent(eff)}&order=created_at.desc&limit=1000`); return json({ history: rows }); } catch (e) { return json({ error: e.message }, 500); }
    }
    if (url.pathname === '/analytics' && request.method === 'GET') {
      try {
        if (effInvalid) return json({ error:'profile not found' }, 404);
        const counts = {};
        for (const s of ['pending','ready','approved','published','failed','publishing']) {
          const r = await sb(env, `videos?select=id&user_id=eq.${encodeURIComponent(eff)}&status=eq.${s}`); counts[s] = r.ok ? (await r.json()).length : 0;
        }
        const totalRes = await sb(env, `videos?select=id&user_id=eq.${encodeURIComponent(eff)}`); const total = totalRes.ok ? (await totalRes.json()).length : 0;
        const pagesRes = await sb(env, `pages?select=id&user_id=eq.${encodeURIComponent(eff)}`); const pages = pagesRes.ok ? (await pagesRes.json()).length : 0;
        const aiRes = await sb(env, `ai_generated_contents?select=id&user_id=eq.${encodeURIComponent(eff)}`); const aiDone = aiRes.ok ? (await aiRes.json()).length : 0;
        const hbRes = await sb(env, 'heartbeat?select=checked_at&order=checked_at.desc&limit=1'); const hb = hbRes.ok ? await hbRes.json() : [];
        const errRes = await sb(env, `error_logs?select=source,message,created_at&user_id=eq.${encodeURIComponent(eff)}&order=created_at.desc&limit=20`); const errors = errRes.ok ? await errRes.json() : [];
        return json({ total, uploaded: total, published: counts.published||0, failed: counts.failed||0, scheduled: counts.approved||0, pending: counts.pending||0, ready: counts.ready||0, ai_completed: aiDone, pages, last_heartbeat: hb[0] ? hb[0].checked_at : null, ...(isAdmin ? { recent_errors: errors } : {}), success_rate: total ? Math.round((counts.published||0)/total*100) : 0 });
      } catch (e) { return json({ error: e.message }, 500); }
    }

    // ---- Pages: list (no token) / add (admin only) ----
    if (url.pathname === '/pages' && request.method === 'GET') {
      try { if (effInvalid) return json({ error:'profile not found' }, 404); const rows = await sbList(`pages?select=id,user_id,page_name,facebook_page_id,drive_folder_id,status,created_at&user_id=eq.${encodeURIComponent(eff)}&order=created_at.asc&limit=500`); return json({ pages: rows }); } catch (e) { return json({ error: e.message }, 500); }
    }
    if (url.pathname === '/pages' && request.method === 'POST') {
      if (effInvalid) return json({ error:'profile not found' }, 404); // any profile adds ONLY its own page (token encrypted as before)
      const body = await bodyJson(); if (!body || !body.facebook_page_id || !body.page_token) return json({ error:'facebook_page_id and page_token are required (token is stored server-side only, never returned)' }, 400);
      // v3: token is AES-GCM encrypted at rest (enc:v1:...) before storage.
      // If TOKEN_KEY is missing, encryptPageToken throws and NOTHING is stored.
      let encrypted;
      try { encrypted = await encryptPageToken(env, body.page_token); } catch (e) { return json({ error: e.message }, 500); }
      const ins = await sb(env, 'pages', { method:'POST', body: JSON.stringify({ user_id: eff, page_name: body.page_name || body.facebook_page_id, facebook_page_id: body.facebook_page_id, page_token_encrypted: encrypted, drive_folder_id: body.drive_folder_id || null, status:'connected' }) });
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
        // v3.1 isolation: uploads go ONLY to this profile's folder.
        try { const pf = await profileDriveFolderId(env, me.id); if (pf) meta.parents = [pf]; else if (env.DRIVE_FOLDER_ID) meta.parents = [env.DRIVE_FOLDER_ID]; }
        catch { if (env.DRIVE_FOLDER_ID) meta.parents = [env.DRIVE_FOLDER_ID]; }
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
        let durationSeconds = null;
        const durRaw = form.get('duration_seconds');
        if (durRaw !== null && durRaw !== '') { const d = Number(durRaw); if (Number.isFinite(d) && d > 0 && d < 100000) durationSeconds = d; }
        const row = { user_id: me.id, drive_file_id: created.id, file_name: file.name, title: file.name.replace(/\.(mp4|mov|mkv|webm)$/i,''), status:'pending', ai_status:'pending', file_hash: fileHash, file_size_bytes: bytes.byteLength, mime_type: file.type || 'video/mp4', duration_seconds: durationSeconds };
        const pid = form.get('page_id');
        if (pid) {
          try { const pr = await sb(env, `pages?select=id&user_id=eq.${me.id}&id=eq.${encodeURIComponent(String(pid))}&limit=1`); const ps = pr.ok ? await pr.json() : []; if (Array.isArray(ps) && ps.length) row.page_id = pid; } catch {}
        }
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
      if (effInvalid) return json({ error:'profile not found' }, 404); // approve ONLY this profile's own queue
      const body = await bodyJson(); if (!body) return json({ error:'invalid JSON body' }, 400);
      const items = Array.isArray(body) ? body : (Array.isArray(body?.items) ? body.items : null);
      const gap = body.gap_minutes && Number(body.gap_minutes) > 0 ? Number(body.gap_minutes) : await readGlobalSchedulerGap(env); // V4: Admin manual override only (normal users are 403'd by the V4 gate); explicit gap wins, else GLOBAL gap.
      let approved = 0, skipped = 0; const results = [];
      // Append-after-last: base = max existing scheduled_at, else now.
      let base = new Date();
      try { const last = await sbList(`publishing_queue?select=scheduled_at&user_id=eq.${encodeURIComponent(eff)}&order=scheduled_at.desc&limit=1`); if (last.length && last[0].scheduled_at) base = new Date(last[0].scheduled_at); } catch {}
      // Also respect explicit items with scheduled_at when the caller provides them.
      const list = items || [];
      if (!list.length && body.video_ids && Array.isArray(body.video_ids)) {
        for (const vid of body.video_ids) list.push({ video_id: vid });
      }
      if (!list.length) return json({ error:'no videos to approve: send {items:[{video_id, scheduled_at?}]} or {video_ids:[...]}' }, 400);
      for (const it of list) {
        const video_id = it?.video_id || it?.id; if (!video_id) { skipped++; continue; }
        // Ownership is enforced by the user_id filter on the PATCH itself:
        // a video from another profile simply matches zero rows.
        let when = it?.scheduled_at && !isNaN(Date.parse(it.scheduled_at)) ? new Date(it.scheduled_at) : new Date(base.getTime() + gap*60000);
        if (!it?.scheduled_at) base = when;
        const idem = `pub-${video_id}-${when.toISOString()}`;
        const r = await sb(env, `videos?id=eq.${encodeURIComponent(video_id)}&user_id=eq.${encodeURIComponent(eff)}&status=in.(pending,ready)`, { method:'PATCH', body: JSON.stringify({ status:'approved', scheduled_at: when.toISOString() }) });
        if (!r.ok) { skipped++; results.push({ video_id, status:'skipped', reason:'Supabase update failed' }); continue; }
        const changed = await r.json();
        if (!Array.isArray(changed) || !changed.length) { skipped++; results.push({ video_id, status:'skipped', reason:'row not pending/ready (already approved, publishing, published, failed, or not found)' }); continue; }
        const v = changed[0];
        await sb(env, 'publishing_queue', { method:'POST', headers:{Prefer:'return=minimal,resolution=ignore-duplicates'}, body: JSON.stringify({ video_id, user_id: v.user_id || eff, page_id: v.page_id || null, scheduled_at: when.toISOString(), status:'scheduled', idempotency_key: idem }) });
        approved++; results.push({ video_id, status:'approved', scheduled_at: when.toISOString() });
      }
      return json({ approved, skipped, results, gap_minutes: gap });
    }

    // ---- Scheduler: pause / resume / status (admin only for writes) ----
    if (url.pathname === '/scheduler' && request.method === 'GET') {
      try { if (effInvalid) return json({ error:'profile not found' }, 404); const rows = await sbList(`publishing_schedules?select=*&user_id=eq.${encodeURIComponent(eff)}&order=created_at.desc&limit=100`); return json({ schedules: rows, paused: await schedulerPausedFor(env, eff), gap_minutes: await readGlobalSchedulerGap(env) }); } catch (e) { return json({ error: e.message }, 500); }
    }
    if (url.pathname === '/scheduler/pause' && request.method === 'POST') {
      if (effInvalid) return json({ error:'profile not found' }, 404); // pause ONLY this profile's publishing
      await upsertSettings(env, eff, { scheduler_paused:true });
      return json({ paused:true });
    }
    if (url.pathname === '/scheduler/resume' && request.method === 'POST') {
      if (effInvalid) return json({ error:'profile not found' }, 404);
      await upsertSettings(env, eff, { scheduler_paused:false });
      return json({ paused:false });
    }
    if (url.pathname === '/scheduler' && request.method === 'POST') {
      if (effInvalid) return json({ error:'profile not found' }, 404); // schedule ONLY for this profile
      const body = await bodyJson(); if (!body) return json({ error:'invalid JSON body' }, 400);
      const ins = await sb(env, 'publishing_schedules', { method:'POST', body: JSON.stringify({ user_id: eff, page_id: body.page_id || null, name: body.name || 'Default schedule', interval_minutes: Number(body.interval_minutes) || 60, start_at: body.start_at || null, specific_times: body.specific_times ? JSON.stringify(body.specific_times) : null, mode: body.mode || 'interval' }) });
      if (!ins.ok) return json({ error:'schedule insert failed: ' + (await ins.text()).slice(0,300) }, 500);
      if (body.interval_minutes) await upsertSettings(env, eff, { schedule_gap_minutes: Number(body.interval_minutes) });
      return json({ schedule: await ins.json() });
    }

    // ---- AI Content Studio: list / edit / regenerate / approve ----
    if (url.pathname === '/ai-content' && request.method === 'GET') {
      try { if (effInvalid) return json({ error:'profile not found' }, 404); const rows = await sbList(`ai_generated_contents?select=*,videos(file_name,status)&user_id=eq.${encodeURIComponent(eff)}&order=generated_at.desc&limit=1000`); return json({ contents: rows }); } catch (e) { return json({ error: e.message }, 500); }
    }
    if (url.pathname === '/ai-content' && request.method === 'PATCH') {
      const body = await bodyJson(); if (!body || !body.video_id) return json({ error:'video_id is required' }, 400);
      if (effInvalid) return json({ error:'profile not found' }, 404);
      try { const vr = await sbList(`videos?select=id,user_id&id=eq.${encodeURIComponent(body.video_id)}&limit=1`); if (!vr.length || vr[0].user_id !== eff) return json({ error:'video not found' }, 404); } catch { return json({ error:'video not found' }, 404); }
      const patch = {}; for (const k of ['title','description','caption','hashtags']) if (body[k] !== undefined) patch[k] = body[k];
      if (body.approved !== undefined) patch.approved = !!body.approved;
      if (body.rejected !== undefined) patch.rejected = !!body.rejected;
      const r = await sb(env, `ai_generated_contents?video_id=eq.${encodeURIComponent(body.video_id)}&user_id=eq.${encodeURIComponent(eff)}`, { method:'PATCH', body: JSON.stringify(patch) });
      if (!r.ok) return json({ error:'AI content update failed: ' + (await r.text()).slice(0,300) }, 500);
      const changed = await r.json();
      if (Array.isArray(changed) && changed.length && patch.title) await sb(env, `videos?id=eq.${encodeURIComponent(body.video_id)}&user_id=eq.${encodeURIComponent(eff)}`, { method:'PATCH', body: JSON.stringify({ title: patch.title, description: patch.description || undefined, caption: patch.caption || undefined, hashtags: patch.hashtags || undefined, status:'ready' }) });
      return json({ updated: changed });
    }
    if (url.pathname === '/ai-generate' && request.method === 'POST') {
      const body = await bodyJson(); if (!body || !body.video_id) return json({ error:'video_id is required' }, 400);
      if (effInvalid) return json({ error:'profile not found' }, 404);
      try { const vr = await sbList(`videos?select=id,user_id&id=eq.${encodeURIComponent(body.video_id)}&limit=1`); if (!vr.length || vr[0].user_id !== eff) return json({ error:'video not found' }, 404); } catch { return json({ error:'video not found' }, 404); }
      // Transcript source: body.transcript, else stored transcript row, else error (never fake).
      let transcript = (body.transcript || '').trim();
      if (!transcript) { try { const t = await sbList(`video_transcripts?select=transcript,language&video_id=eq.${encodeURIComponent(body.video_id)}&limit=1`); if (t.length) transcript = t[0].transcript || ''; } catch {} }
      try {
        const { cfg, parsed, usage, tried } = await aiGenerate(env, transcript, body.language, eff);
        const valid = validateAi(parsed);
        // Never reuse another video's content: uniqueness check against stored contents.
        try { const all = await sbList(`ai_generated_contents?select=description&user_id=eq.${encodeURIComponent(eff)}&limit=1000`); const dup = all.some(c => c.description && c.description === valid.description); if (dup) throw new Error('Generated description duplicates an existing video — regenerate instead of reusing.'); } catch (e) { if (/duplicates/.test(e.message)) throw e; }
        const upsert = await sb(env, 'ai_generated_contents', { method:'POST', headers:{ Prefer:'resolution=merge-duplicates' }, body: JSON.stringify({ video_id: body.video_id, user_id: eff, title: valid.title, description: valid.description, caption: valid.caption, hashtags: valid.hashtags, provider: cfg.provider, model: cfg.model, approved:false }) });
        if (!upsert.ok) return json({ error:'AI content save failed: ' + (await upsert.text()).slice(0,300) }, 500);
        await sb(env, `videos?id=eq.${encodeURIComponent(body.video_id)}&user_id=eq.${encodeURIComponent(eff)}`, { method:'PATCH', body: JSON.stringify({ title: valid.title, description: valid.description, caption: valid.caption, hashtags: valid.hashtags, ai_status:'completed', status:'ready' }) });
        return json({ content: valid, provider: cfg.provider, model: cfg.model, attempted_models: tried || null, successful_model: cfg.model });
      } catch (e) {
        await logError(env, 'ai', e.message, body.video_id, eff);
        // (generation attempts already logged by aiGenerate fallback chain)
        const status = /not configured|needs a transcript/.test(e.message) ? 503 : 500;
        return json({ error: e.message }, status);
      }
    }
    if (url.pathname === '/transcripts' && request.method === 'POST') {
      const body = await bodyJson(); if (!body || !body.video_id || !body.transcript) return json({ error:'video_id and transcript are required' }, 400);
      if (effInvalid) return json({ error:'profile not found' }, 404);
      try { const vr = await sbList(`videos?select=id,user_id&id=eq.${encodeURIComponent(body.video_id)}&limit=1`); if (!vr.length || vr[0].user_id !== eff) return json({ error:'video not found' }, 404); } catch { return json({ error:'video not found' }, 404); }
      const low = /unclear|inaudible|\[inaudible\]/i.test(body.transcript) || body.transcript.trim().length < 20;
      const ins = await sb(env, 'video_transcripts', { method:'POST', headers:{ Prefer:'resolution=merge-duplicates' }, body: JSON.stringify({ video_id: body.video_id, user_id: eff, transcript: body.transcript, language: body.language || 'auto', confidence: body.confidence ?? null, confidence_flag: low ? 'low' : 'high', source: body.source || 'manual' }) });
      if (!ins.ok) return json({ error:'transcript save failed: ' + (await ins.text()).slice(0,300) }, 500);
      await sb(env, `videos?id=eq.${encodeURIComponent(body.video_id)}&user_id=eq.${encodeURIComponent(eff)}`, { method:'PATCH', body: JSON.stringify({ transcript: body.transcript, ai_status:'transcribed' }) });
      return json({ transcript: await ins.json(), low_confidence: low });
    }

    // ---- v3: Real transcription (Gemini Files / Whisper, no ffmpeg) ----
    if (url.pathname === '/transcribe' && request.method === 'POST') {
      const body = await bodyJson(); if (!body || !body.video_id) return json({ error:'video_id is required' }, 400);
      if (effInvalid) return json({ error:'profile not found' }, 404);
      try {
        const rows = await sbList(`videos?select=${VIDEO_COLS}&id=eq.${encodeURIComponent(body.video_id)}&limit=1`);
        if (!rows.length) return json({ error:'video not found' }, 404);
        const video = rows[0];
        if (video.user_id !== eff) return json({ error:'video not found' }, 404);
        if (!video.drive_file_id) return json({ error:'video has no Drive file ID — upload it first' }, 400);
        const resolved = await resolveAiSettings(env, null); // V4: GLOBAL AI only (this endpoint is admin-only in V4).
        const { bytes } = await fetchDriveVideo(env, video.drive_file_id); // throws Drive-not-connected error (503 below) if no OAuth/service token
        // v3.2 fallback chain: active provider, then other provider if configured.
        const res = await transcribeWithFallback(env, bytes, video.mime_type || 'video/mp4', resolved, eff);
        const provider = res.provider || resolved.active_provider;
        const transcript = (res.transcript || '').trim();
        const low = transcript.length < 20 || /inaudible|unclear|\[inaudible\]/i.test(transcript);
        await sb(env, 'video_transcripts', { method:'POST', headers:{ Prefer:'resolution=merge-duplicates' }, body: JSON.stringify({ video_id: video.id, user_id: eff, transcript, language: res.language || 'auto', confidence: null, confidence_flag: low ? 'low' : 'high', source: provider }) });
        await sb(env, `videos?id=eq.${encodeURIComponent(video.id)}&user_id=eq.${encodeURIComponent(eff)}`, { method:'PATCH', body: JSON.stringify({ transcript, ai_status:'transcribed' }) });
        let srtInfo = { available: false, timing: null, stored: false };
        try { const sr = await buildSrtForVideo(env, video, transcript, res.segments || null); if (sr) srtInfo = sr; } catch {}
        return json({ video_id: video.id, provider, model: res.model || null, transcript, language: res.language || 'auto', confidence_note: res.confidence_note || null, low_confidence: low, ai_status:'transcribed', srt_available: srtInfo.available, srt_timing: srtInfo.timing, attempted_models: res.tried || null, stages_tried: res.stages_tried || null });
      } catch (e) {
        await logError(env, 'transcribe', e.message, body.video_id, eff);
        const status = /not configured|Drive not connected|Drive OAuth|Drive token/i.test(e.message) ? 503 : 500;
        return json({ error: e.message }, status);
      }
    }
    if (url.pathname === '/transcripts' && request.method === 'GET') {
      try { if (effInvalid) return json({ error:'profile not found' }, 404); const rows = await sbList(`video_transcripts?select=*,videos(file_name)&user_id=eq.${encodeURIComponent(eff)}&order=created_at.desc&limit=1000`); return json({ transcripts: rows }); } catch (e) { return json({ error: e.message }, 500); }
    }

    // ---- Settings (admin writes; everyone reads own/global view) ----
    if (url.pathname === '/settings' && request.method === 'GET') {
      // v3.1: the 'ai_settings' row is NEVER returned here (its encrypted
      // key blob must not leak through this general endpoint). Any other
      // system row is returned with api_key-like fields stripped.
      try {
        if (effInvalid) return json({ error:'profile not found' }, 404);
        if (!isAdmin) {
          // V4 normal view: own language + global gap ONLY. No system
          // settings, no keys, no AI config, no auto-approve flag.
          const own = await sbList(`settings?select=language,ai_language&user_id=eq.${encodeURIComponent(eff)}&limit=1`).catch(() => []);
          const lang = own[0] ? (own[0].language || own[0].ai_language || null) : null;
          return json({ language: lang, global_gap_minutes: await readGlobalSchedulerGap(env), settings: { language: lang } });
        }
        const rows = await sbList(`settings?select=*&user_id=eq.${encodeURIComponent(eff)}&limit=1`); const sys = await sbList('system_settings?select=*&limit=100');
        const filtered = sys.filter(r => !isInternalSystemKey(r.key)).map(r => {
          if (!r.value || typeof r.value !== 'object') return r;
          const v = {}; for (const k of Object.keys(r.value)) if (!/api_key/i.test(k)) v[k] = r.value[k];
          return { ...r, value: v };
        });
        let aiProvider = (env.AI_PROVIDER || 'gemini'), aiConfigured = Boolean(env.GEMINI_API_KEY || env.OPENAI_API_KEY);
        try { const s = await resolveAiSettings(env, null); aiProvider = s.active_provider; aiConfigured = s.gemini.configured || s.openai.configured; } catch {} // V4: GLOBAL AI only, even for Admin display.
        const autoApprove = await autoApproveEnabled(env, eff);
        return json({ settings: rows[0] || null, system: filtered, ai: aiProvider, ai_configured: aiConfigured, auto_approve_ready: autoApprove, global_gap_minutes: await readGlobalSchedulerGap(env) });
      } catch (e) { return json({ error: e.message }, 500); }
    }
    // ---- v3.1: AI Settings (Mother Admin only) ----
    // GET returns ONLY base_url, model and configured per provider — never
    // a key, key fragment, or encrypted blob. POST merges per provider:
    // base_url/model replace when supplied; api_key replaces ONLY when a
    // non-empty value is supplied, otherwise the existing key is kept.
    // New keys are encrypted with TOKEN_KEY BEFORE anything is stored; if
    // TOKEN_KEY is missing the request fails closed and nothing is stored.
    if (url.pathname === '/ai-settings' && request.method === 'GET') {
      const scope = url.searchParams.get('scope');
      if (!isAdmin) return json({ error:'only Mother Admin can view AI settings' }, 403);
      if (scope !== 'global') return json({ error:'V4: AI settings are global only — use ?scope=global (per-profile AI settings are not used in V4)' }, 400);
      try {
        const s = await resolveAiSettings(env, null);
        return json({ ...aiSettingsSafeView(s), scope: 'global', effective_user_id: null });
      }
      catch (e) { return json({ error: e.message }, 500); }
    }
    if (url.pathname === '/ai-settings' && request.method === 'POST') {
      const scope = url.searchParams.get('scope'); // POST scope comes from the query string only (body is read once below)
      if (!isAdmin) return json({ error:'only Mother Admin can change AI settings' }, 403);
      if (scope !== 'global') return json({ error:'V4: AI settings are global only — use ?scope=global (per-profile AI settings are not used in V4)' }, 400);
      const body = await bodyJson(); if (!body || typeof body !== 'object') return json({ error:'invalid JSON body' }, 400);
      if (body.active_provider !== undefined && body.active_provider !== 'gemini' && body.active_provider !== 'openai') return json({ error:'active_provider must be gemini or openai' }, 400);
      // V4: storage key is ALWAYS the global row 'ai_settings'.
      // Per-profile rows from V3.1/V3.2 are never read or written here.
      const storeKey = 'ai_settings'; // V4: global row ONLY. Per-profile rows are never written here.
      const storeUser = null;
      try {
        const existing = await readSystemValue(env, storeKey);
        const merged = { active_provider: (existing && (existing.active_provider === 'gemini' || existing.active_provider === 'openai')) ? existing.active_provider : (((env.AI_PROVIDER || 'gemini').toLowerCase() === 'openai') ? 'openai' : 'gemini'), gemini: {}, openai: {} };
        if (body.active_provider) merged.active_provider = body.active_provider;
        for (const prov of ['gemini', 'openai']) {
          const prev = (existing && existing[prov] && typeof existing[prov] === 'object') ? existing[prov] : {};
          const next = { base_url: prev.base_url || null, model: prev.model || null, api_key_enc: prev.api_key_enc || null, selected_models: (prev.selected_models && Array.isArray(prev.selected_models)) ? prev.selected_models : null };
          const inp = body[prov];
          if (inp !== undefined) {
            if (typeof inp !== 'object' || inp === null) return json({ error: prov + ' must be an object' }, 400);
            if (inp.base_url !== undefined) { const v = validBaseUrl(inp.base_url); if (inp.base_url !== '' && !v) return json({ error: 'invalid base_url for ' + prov + ' (must be an http(s) URL without credentials)' }, 400); next.base_url = v; }
            if (inp.model !== undefined) { const v = validModel(inp.model); if (inp.model !== '' && !v) return json({ error: 'invalid model for ' + prov }, 400); next.model = v; }
            if (inp.selected_models !== undefined) { const v = validSelectedModels(inp.selected_models); if (v === null) return json({ error: 'invalid selected_models for ' + prov + ' (must be an array of valid model names)' }, 400); next.selected_models = v; }
            if (inp.api_key !== undefined && inp.api_key !== null && String(inp.api_key) !== '') {
              const k = String(inp.api_key).trim();
              if (k.length < 8 || k.length > 500) return json({ error: 'api_key for ' + prov + ' looks invalid' }, 400);
              // Encrypt FIRST, fail closed on missing TOKEN_KEY — nothing
              // is stored if encryption throws.
              next.api_key_enc = await encryptPageToken(env, k);
            } else if (inp.api_key !== undefined) {
              // Empty/omitted key = keep existing. If there is no existing
              // key AND none can be decrypted, fail rather than dropping it.
              if (next.api_key_enc) {
                try { await decryptPageToken(env, next.api_key_enc); } catch { return json({ error: 'Stored AI key for ' + prov + ' cannot be decrypted — TOKEN_KEY may have changed. Enter a new key.' }, 500); }
              }
            }
          }
          merged[prov] = next;
        }
        const up = await sb(env, 'system_settings', { method:'POST', headers:{ Prefer:'resolution=merge-duplicates' }, body: JSON.stringify({ key: storeKey, value: merged, updated_by: me.id }) });
        if (!up.ok) return json({ error:'AI settings save failed' }, 500);
        const s = await resolveAiSettings(env, null);
        return json({ ...aiSettingsSafeView(s), scope: 'global', effective_user_id: null });
      } catch (e) { return json({ error: e.message }, 500); }
    }
    if (url.pathname === '/settings' && request.method === 'POST') {
      if (effInvalid) return json({ error:'profile not found' }, 404); // own settings only (Admin: own or managed profile)
      const body = await bodyJson(); if (!body) return json({ error:'invalid JSON body' }, 400);
      if (!isAdmin) {
        // V4: normal users may change ONLY their own language. Any other
        // field (gap, Drive folder, AI provider/model, limits, auto-approve)
        // is 403 — Scheduler/AI changes are Admin-only in V4.
        const allowedLang = new Set(['language', 'ai_language']);
        const bad = Object.keys(body).filter(k => !allowedLang.has(k));
        if (bad.length) return json({ error: 'only language can be changed by a normal user; forbidden field(s): ' + bad.join(', ') }, 403);
        const langPatch = {}; if (body.language !== undefined) langPatch.language = body.language; if (body.ai_language !== undefined) langPatch.ai_language = body.ai_language;
        const langOk = await upsertSettings(env, eff, langPatch);
        return json({ ok: langOk, language: langPatch.language !== undefined ? langPatch.language : langPatch.ai_language });
      }
      const patch = {}; for (const k of ['drive_folder_id','schedule_gap_minutes','language','ai_provider','ai_model','ai_language','max_concurrent_uploads','max_retries']) if (body[k] !== undefined) patch[k] = body[k];
      const ok = await upsertSettings(env, eff, patch);
      // V4 NOTE: the auto-approve toggle below is kept for Admin display
      // only. The V4 pipeline does NOT consult it — ready videos are
      // always auto-scheduled (see autoScheduleVideo).
      if (body.auto_approve_ready !== undefined) {
        const up = await sb(env, 'system_settings', { method:'POST', headers:{ Prefer:'resolution=merge-duplicates' }, body: JSON.stringify({ key:'pipeline_settings:' + eff, value: { auto_approve_ready: !!body.auto_approve_ready }, updated_by: me.id }) });
        if (!up.ok) return json({ error:'auto-approve setting save failed' }, 500);
      }
      return json({ ok, auto_approve_ready: body.auto_approve_ready !== undefined ? !!body.auto_approve_ready : undefined });
    }
    // ---- v3.1: Pipeline monitor (authenticated) ----
    // One call for the Studio monitor: videos + transcript + generated
    // content + AI job + stored SRT timing, with a derived pipeline status.
    // This follows the existing /videos pattern (all rows, no per-user
    // filter in v3); the stricter owner check applies to /srt downloads.
    if (url.pathname === '/pipeline' && request.method === 'GET') {
      try {
        if (effInvalid) return json({ error:'profile not found' }, 404);
        const videos = await sbList(`videos?select=${VIDEO_COLS}&user_id=eq.${encodeURIComponent(eff)}&order=created_at.desc&limit=500`);
        const transcripts = await sbList(`video_transcripts?select=video_id,transcript,language,confidence_flag,source&user_id=eq.${encodeURIComponent(eff)}&limit=1000`).catch(() => []);
        const contents = await sbList(`ai_generated_contents?select=video_id,title,description,caption,hashtags,provider,model,approved&user_id=eq.${encodeURIComponent(eff)}&limit=1000`).catch(() => []);
        const jobs = await sbList(`background_jobs?select=id,video_id,job_type,status,attempt_count,last_error,payload&job_type=eq.ai_process&user_id=eq.${encodeURIComponent(eff)}&limit=1000`).catch(() => []);
        // SRT timing comes from srt_meta rows (Drive file + timing;
        // there is NO video_srts table in V3.1). Only this profile's
        // meta rows are used.
        let srts = [];
        try {
          const metaRows = await sbList(`system_settings?select=key,value&key=like.${encodeURIComponent('srt_meta:%')}`);
          srts = metaRows.filter(r => r.value && r.value.user_id === eff && r.value.video_id).map(r => ({ video_id: r.value.video_id, timing: r.value.timing || null }));
        } catch { srts = []; }
        const tMap = new Map(transcripts.map(t => [t.video_id, t]));
        const cMap = new Map(contents.map(c => [c.video_id, c]));
        const jMap = new Map(jobs.map(j => [j.video_id, j]));
        const sMap = new Map(srts.map(x => [x.video_id, x.timing]));
        const out = videos.map(v => {
          const t = tMap.get(v.id) || null, c = cMap.get(v.id) || null, j = jMap.get(v.id) || null, srtTiming = sMap.get(v.id) || null;
          let pipeline_status = 'uploaded';
          if (v.status === 'published') pipeline_status = 'published';
          else if (v.status === 'approved' || v.status === 'scheduled') pipeline_status = 'scheduled';
          else if (v.status === 'ready') pipeline_status = 'ready';
          else if (c) pipeline_status = 'generated';
          else if (srtTiming) pipeline_status = 'srt ready';
          else if (t) pipeline_status = 'transcribed';
          else if (j && j.status === 'running') pipeline_status = 'transcribing';
          else if (j && j.status === 'failed' && j.payload && j.payload.dead_letter) pipeline_status = 'failed (dead-letter)';
          else if (j && j.status === 'failed') pipeline_status = 'failed (retry pending)';
          else if (j) pipeline_status = 'uploaded (AI queued)';
          return { ...v, transcript: t ? t.transcript : (v.transcript || null), transcript_language: t ? t.language : null, transcript_confidence_flag: t ? t.confidence_flag : null, ai_content: c, ai_job: j ? { id: j.id, status: j.status, attempt_count: j.attempt_count, last_error: j.last_error } : null, srt_available: Boolean(srtTiming) || (Boolean(t) && Number(v.duration_seconds) > 0), srt_timing: srtTiming, pipeline_status };
        });
        // v3.2 model-switch status: selected models (never 'available'),
        // last successful model, and recent failures — from usage logs.
        let aiModelStatus = null;
        try {
          const usage = await sbList(`api_usage_logs?select=provider,operation,model,success,error,created_at&user_id=eq.${encodeURIComponent(eff)}&order=created_at.desc&limit=100`).catch(() => []);
          const resolvedForStatus = await resolveAiSettings(env, null).catch(() => null); // V4: GLOBAL AI only.
          const perProv = {};
          for (const prov of ['gemini', 'openai']) {
            const rows = usage.filter(u => u.provider === prov);
            const lastOk = rows.find(u => u.success === true);
            const failures = rows.filter(u => u.success === false).slice(0, 5).map(u => ({ model: u.model || null, operation: u.operation || null, error: u.error ? String(u.error).slice(0, 300) : null, created_at: u.created_at || null }));
            perProv[prov] = {
              selected_models: resolvedForStatus ? selectedModelsFor(resolvedForStatus[prov]) : [],
              last_successful_model: lastOk ? lastOk.model : null,
              last_failures: failures,
            };
          }
          aiModelStatus = { active_provider: resolvedForStatus ? resolvedForStatus.active_provider : null, providers: perProv, note: 'Models are shown as selected / last successful / last tried-failed only — never as available. Availability depends on your API key/account.' };
        } catch { aiModelStatus = null; }
        return json({ videos: out, auto_approve_ready: await autoApproveEnabled(env, eff), auto_schedule_v4: true, ai_model_status: aiModelStatus });
      } catch (e) { return json({ error: e.message }, 500); }
    }
    // ---- v3.1: SRT download (authenticated, owner or Mother Admin) ----
    if (url.pathname === '/srt' && request.method === 'GET') {
      const videoId = url.searchParams.get('video_id');
      if (!videoId) return json({ error:'video_id is required' }, 400);
      try {
        const rows = await sbList(`videos?select=${VIDEO_COLS}&id=eq.${encodeURIComponent(videoId)}&limit=1`);
        if (!rows.length) return json({ error:'video not found' }, 404);
        const video = rows[0];
        if (effInvalid) return json({ error:'profile not found' }, 404);
        if (video.user_id !== eff) return json({ error:'video not found' }, 404);
        const stored = await getStoredSrt(env, video.id);
        if (stored && stored.srt_text) {
          const base = String(video.file_name || 'video').replace(/\.[^.]+$/, '').replace(/[^A-Za-z0-9._-]+/g, '_') || 'video';
          return new Response(stored.srt_text, { headers: { ...cors, 'Content-Type': 'text/plain; charset=utf-8', 'Content-Disposition': `attachment; filename="${base}.srt"` } });
        }
        let transcript = video.transcript || '';
        if (!transcript) { try { const t = await sbList(`video_transcripts?select=transcript&video_id=eq.${encodeURIComponent(video.id)}&limit=1`); if (t.length) transcript = t[0].transcript || ''; } catch {} }
        const dur = Number(video.duration_seconds);
        if (transcript.trim() && dur > 0) {
          const srt = srtEstimated(transcript, dur);
          if (srt) {
            const base = String(video.file_name || 'video').replace(/\.[^.]+$/, '').replace(/[^A-Za-z0-9._-]+/g, '_') || 'video';
            return new Response(srt, { headers: { ...cors, 'Content-Type': 'text/plain; charset=utf-8', 'Content-Disposition': `attachment; filename="${base}.srt"` } });
          }
        }
        return json({ error:'SRT not available for this video yet: no stored SRT and no usable duration for estimated timing. Run transcription again, or download the transcript instead.' }, 503);
      } catch (e) { return json({ error: e.message }, 500); }
    }
    // ---- v3.1: Run one AI job now (Admin) / Retry a video's AI job ----
    if (url.pathname === '/ai-process' && request.method === 'POST') {
      if (!isAdmin) return json({ error:'only Mother Admin can run AI processing now' }, 403);
      if (effInvalid) return json({ error:'profile not found' }, 404);
      const body = await bodyJson() || {};
      if (body.video_id) {
        try {
          const rows = await sbList(`videos?select=${PIPELINE_VIDEO_COLS}&id=eq.${encodeURIComponent(body.video_id)}&limit=1`);
          if (!rows.length) return json({ error:'video not found' }, 404);
          if (rows[0].user_id !== eff) return json({ error:'video not found' }, 404);
          const out = await runAiPipeline(env, rows[0]);
          try { const js = await sbList(`background_jobs?select=id&job_type=eq.ai_process&video_id=eq.${encodeURIComponent(body.video_id)}&limit=10`); for (const j of js) await sb(env, `background_jobs?id=eq.${j.id}`, { method:'PATCH', body: JSON.stringify({ status:'done', updated_at: new Date().toISOString() }) }); } catch {}
          return json({ video_id: body.video_id, provider: out.provider, became_ready: out.became_ready, auto_approved: out.auto_approved, srt: out.srt });
        } catch (e) { await logError(env, 'ai', e.message, body.video_id, me.id); return json({ error: e.message }, 500); }
      }
      const res = await processAiJobs(env, 1, eff); // Admin default = own jobs; management view = selected profile's jobs
      return json(res);
    }
    if (url.pathname === '/ai-retry' && request.method === 'POST') {
      const body = await bodyJson(); if (!body || !body.video_id) return json({ error:'video_id is required' }, 400);
      try {
        const rows = await sbList(`videos?select=id,user_id&id=eq.${encodeURIComponent(body.video_id)}&limit=1`);
        if (!rows.length) return json({ error:'video not found' }, 404);
        if (effInvalid) return json({ error:'profile not found' }, 404);
        if (rows[0].user_id !== eff) return json({ error:'video not found' }, 404);
        const js = await sbList(`background_jobs?select=*&job_type=eq.ai_process&video_id=eq.${encodeURIComponent(body.video_id)}&order=created_at.desc&limit=1`).catch(() => []);
        if (js.length && js[0].status === 'running') return json({ error:'AI job already running for this video' }, 409);
        if (js.length) {
          const r = await sb(env, `background_jobs?id=eq.${js[0].id}`, { method:'PATCH', body: JSON.stringify({ status:'pending', attempt_count: 0, last_error: null, payload: {}, updated_at: new Date().toISOString() }) });
          return json({ video_id: body.video_id, status:'pending', job_id: js[0].id, queued: r.ok });
        }
        const ins = await sb(env, 'background_jobs', { method:'POST', body: JSON.stringify({ job_type:'ai_process', video_id: body.video_id, user_id: rows[0].user_id, status:'pending' }) });
        const created = ins.ok ? await ins.json().catch(() => []) : [];
        return json({ video_id: body.video_id, status:'pending', job_id: Array.isArray(created) && created[0] ? created[0].id : null });
      } catch (e) { return json({ error: e.message }, 500); }
    }
    if (url.pathname === '/errors' && request.method === 'GET') {
      try { if (effInvalid) return json({ error:'profile not found' }, 404); const rows = await sbList(`error_logs?select=*&user_id=eq.${encodeURIComponent(eff)}&order=created_at.desc&limit=100`); return json({ errors: rows }); } catch (e) { return json({ error: e.message }, 500); }
    }
    // ---- Admin > Users (Admin only; first profile is Admin by the
    // existing first-profile-is-admin rule — NO email is hardcoded here.
    // Whoever created the first account on the live site IS the Admin.)
    // POST creates a CLIENT account via the Supabase Auth Admin API
    // (service key, server-side only) with email_confirm:true, so the
    // client can log in immediately without a confirmation email, then
    // creates the profiles row with role='client'. The temporary password
    // is NEVER returned or logged. If email confirmation is still
    // required (email_confirmed_at null), the response says so clearly.
    if (url.pathname === '/admin/users' && request.method === 'GET') {
      if (!isAdmin) return json({ error:'only Mother Admin can list users' }, 403);
      try {
        const rows = await sbList('profiles?select=user_id,email,display_name,role,created_at&order=created_at.asc&limit=500');
        let activeMap = {};
        try {
          const st = await sbList(`system_settings?select=key,value&key=like.${encodeURIComponent('profile_status:%')}`);
          for (const r of st) { const uid = String(r.key).replace('profile_status:', ''); if (r.value && typeof r.value.active === 'boolean') activeMap[uid] = r.value.active; }
        } catch {}
        return json({ profiles: rows.map(r => ({ id: r.user_id, email: r.email, display_name: r.display_name || null, role: r.role, created_at: r.created_at, active: activeMap[r.user_id] !== false })) });
      } catch (e) { return json({ error: e.message }, 500); }
    }
    if (url.pathname === '/admin/users' && request.method === 'POST') {
      if (!isAdmin) return json({ error:'only Mother Admin can create users' }, 403);
      const body = await bodyJson(); if (!body) return json({ error:'invalid JSON body' }, 400);
      const email = String(body.email || '').trim();
      const password = String(body.password || '');
      const displayName = String(body.display_name || body.profile_name || '').trim() || null;
      if (!/.+@.+\..+/.test(email)) return json({ error:'enter a valid email address' }, 400);
      if (password.length < 8) return json({ error:'temporary password must be at least 8 characters' }, 400);
      if (!env.SUPABASE_URL || !env.SUPABASE_SERVICE_KEY) return json({ error:'Supabase service key is not configured' }, 500);
      try {
        const ar = await fetch(`${env.SUPABASE_URL}/auth/v1/admin/users`, { method:'POST', headers:{ apikey: env.SUPABASE_SERVICE_KEY, Authorization:`Bearer ${env.SUPABASE_SERVICE_KEY}`, 'Content-Type':'application/json' }, body: JSON.stringify({ email, password, email_confirm: true, user_metadata: displayName ? { display_name: displayName } : {} }) });
        let aj = null; try { aj = await ar.json(); } catch {}
        if (!ar.ok) return json({ error: 'account creation failed: ' + String((aj && (aj.msg || aj.message)) || ('HTTP ' + ar.status)).slice(0, 300) }, ar.status === 422 ? 400 : ar.status);
        const uid = aj && aj.id; if (!uid) return json({ error:'account creation failed: no user returned' }, 500);
        const ins = await sb(env, 'profiles', { method:'POST', headers:{ Prefer:'resolution=merge-duplicates' }, body: JSON.stringify({ user_id: uid, email, role:'client', display_name: displayName }) });
        if (!ins.ok) return json({ warning:'Auth account was created, but the profiles row could not be saved — check the Users list and retry if the profile is missing.', user: { id: uid, email, display_name: displayName, role:'client', email_confirmed: Boolean(aj.email_confirmed_at) } }, 500);
        const confirmed = Boolean(aj.email_confirmed_at);
        return json({ user: { id: uid, email, display_name: displayName, role:'client', email_confirmed: confirmed }, message: confirmed ? 'Client account created and confirmed — the client can log in now.' : 'Client account created, but email confirmation is still required — the client must confirm via email before login.' });
      } catch (e) { return json({ error: e.message }, 500); }
    }
    // Activate / deactivate: stores profile_status:<id> {active:bool}.
    // There is NO profiles.status column in v1/v2 schema, so this flag is
    // enforced in the Worker (deactivated = 401 on every data call).
    // LIMIT, documented honestly: it does NOT revoke an already-issued
    // Supabase Auth token at the Auth server itself; for a full ban use
    // Supabase Dashboard > Authentication > Users. Admin accounts and
    // the caller's own account cannot be deactivated here.
    if (url.pathname.startsWith('/admin/users/') && url.pathname.endsWith('/status') && request.method === 'POST') {
      if (!isAdmin) return json({ error:'only Mother Admin can change user status' }, 403);
      const targetId = url.pathname.split('/')[3];
      if (!targetId) return json({ error:'user id is required' }, 400);
      if (targetId === me.id) return json({ error:'cannot change your own status' }, 400);
      const targetRole = await profileRole(env, targetId);
      if (!targetRole) return json({ error:'user not found' }, 404);
      if (targetRole === 'admin') return json({ error:'cannot deactivate an admin account' }, 403);
      const body = await bodyJson(); if (!body || body.active === undefined) return json({ error:'active (true/false) is required' }, 400);
      const up = await sb(env, 'system_settings', { method:'POST', headers:{ Prefer:'resolution=merge-duplicates' }, body: JSON.stringify({ key:'profile_status:' + targetId, value:{ active: !!body.active } }) });
      if (!up.ok) return json({ error:'status save failed' }, 500);
      return json({ id: targetId, active: !!body.active });
    }
    if (url.pathname === '/publish-now' && request.method === 'POST') {
      if (!isAdmin) return json({ error:'only Mother Admin can trigger a manual tick' }, 403);
      const result = await publisherTick(env); return json(result);
    }
    return new Response('AutoPublish Studio Pro worker — unknown endpoint.', { status: 404, headers: cors });
  }
};
