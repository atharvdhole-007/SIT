/* FeedSentinel dashboard. Vanilla JS, no build step. Contract: docs/API.md
   - WebSocket /ws: `hello` (full re-init, may arrive at any time) then `tick` at ~4 Hz.
   - Every server string goes into the DOM via textContent (never innerHTML).
   - Every field is treated as optional: missing / null values render as "–". */
(() => {
  'use strict';

  // ================================================================ helpers
  const $ = (sel, root = document) => root.querySelector(sel);
  const arr = (v) => (Array.isArray(v) ? v : []);
  const obj = (v) => (v && typeof v === 'object' && !Array.isArray(v) ? v : null);
  const isNum = (v) => typeof v === 'number' && Number.isFinite(v);
  const str = (v, dflt = '') => (v == null ? dflt : String(v));

  function h(tag, attrs, ...kids) {
    const el = document.createElement(tag);
    if (attrs) {
      for (const [k, v] of Object.entries(attrs)) {
        if (v == null || v === false) continue;
        if (k === 'class') el.className = v;
        else if (k.startsWith('on') && typeof v === 'function') el.addEventListener(k.slice(2), v);
        else el.setAttribute(k, v === true ? '' : String(v));
      }
    }
    for (const kid of kids.flat(Infinity)) {
      if (kid == null || kid === false) continue;
      el.append(kid instanceof Node ? kid : document.createTextNode(String(kid)));
    }
    return el;
  }
  const SVGNS = 'http://www.w3.org/2000/svg';
  function svg(tag, attrs) {
    const el = document.createElementNS(SVGNS, tag);
    for (const [k, v] of Object.entries(attrs || {})) el.setAttribute(k, String(v));
    return el;
  }
  function setText(el, v) {
    const s = str(v);
    if (el && el.textContent !== s) el.textContent = s;
  }
  function setAttr(el, k, v) {
    const s = str(v);
    if (el && el.getAttribute(k) !== s) el.setAttribute(k, s);
  }
  function safe(fn, ...args) {
    try { return fn(...args); } catch (e) { console.error('[FeedSentinel]', fn.name, e); return undefined; }
  }

  // ---------------------------------------------------------------- formatting
  const nfCache = new Map();
  function nf(minD, maxD = minD) {
    const key = minD + ':' + maxD;
    if (!nfCache.has(key)) nfCache.set(key, new Intl.NumberFormat('en-US', { minimumFractionDigits: minD, maximumFractionDigits: maxD }));
    return nfCache.get(key);
  }
  const fmtInt = (v) => (isNum(v) ? nf(0).format(Math.round(v)) : '–');
  const fmtFix = (v, d = 1) => (isNum(v) ? nf(d).format(v) : '–');
  const fmtPct = (v, d = 0) => (isNum(v) ? nf(d).format(v * 100) + '%' : '–');
  const fmtSec = (v, d = 1) => (isNum(v) ? nf(d).format(v) + ' s' : '–');
  const fmtMs = (v) => (!isNum(v) ? '–' : Math.abs(v) >= 100 ? nf(0).format(v) : nf(1).format(v));
  function fmtPrice(v) {
    if (!isNum(v)) return '–';
    const a = Math.abs(v);
    return nf(a >= 1 ? 2 : a >= 0.01 ? 4 : 6).format(v);
  }
  function fmtCompact(v) {
    if (!isNum(v)) return '–';
    const a = Math.abs(v);
    if (a >= 1e6) return nf(1).format(v / 1e6) + 'M';
    if (a >= 1e4) return nf(1).format(v / 1e3) + 'K';
    return nf(0).format(v);
  }
  function fmtDuration(s) {
    if (!isNum(s)) return '–';
    s = Math.max(0, Math.floor(s));
    const hh = Math.floor(s / 3600), mm = Math.floor((s % 3600) / 60), ss = s % 60;
    const p = (n) => String(n).padStart(2, '0');
    return hh ? `${hh}h ${p(mm)}m ${p(ss)}s` : mm ? `${mm}m ${p(ss)}s` : `${ss}s`;
  }
  const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
  function fmtDate(d) {
    const m = /^(\d{4})-(\d{2})-(\d{2})/.exec(str(d));
    if (!m) return str(d, '—') || '—';
    return `${+m[3]} ${MONTHS[+m[2] - 1] || m[2]} ${m[1]}`;
  }
  function makeClockFmt(tz) {
    const o = { hour: '2-digit', minute: '2-digit', second: '2-digit', hourCycle: 'h23' };
    try { return new Intl.DateTimeFormat('en-GB', { ...o, timeZone: tz || 'UTC' }); } catch (e) { return new Intl.DateTimeFormat('en-GB', { ...o, timeZone: 'UTC' }); }
  }
  const fmtClock = (t) => (isNum(t) ? S.clockFmt.format(t) : '–');
  const fmtClockMs = (t) => (isNum(t) ? `${S.clockFmt.format(t)}.${String(Math.floor(((t % 1000) + 1000) % 1000)).padStart(3, '0')}` : '–');
  function tzShort(t) {
    try {
      const parts = new Intl.DateTimeFormat('en-US', { timeZone: S.tz || 'UTC', timeZoneName: 'short' }).formatToParts(new Date(isNum(t) ? t : Date.now()));
      return (parts.find((p) => p.type === 'timeZoneName') || {}).value || S.tz;
    } catch (e) { return S.tz || ''; }
  }

  // ---------------------------------------------------------------- colours (read from CSS tokens)
  const rootStyle = getComputedStyle(document.documentElement);
  const cssv = (n, d) => rootStyle.getPropertyValue(n).trim() || d;
  const COL = {
    grid: cssv('--grid', '#2c2c2a'), axis: cssv('--axis', '#383835'), axisText: cssv('--text-axis', '#898781'),
    text1: cssv('--text-1', '#fff'), text2: cssv('--text-2', '#c3c2b7'), text3: cssv('--text-3', '#a3a29b'),
    surface1: cssv('--surface-1', '#1a1a19'), surface2: cssv('--surface-2', '#242423'),
    good: cssv('--good', '#0ca30c'), warning: cssv('--warning', '#fab219'), critical: cssv('--critical', '#d03b3b'),
    other: cssv('--series-other', '#9298AA'), consensus: 'rgba(48,58,86,0.42)', criticalStrong: cssv('--critical-strong', '#8E4E53'),
    tooltipBg: '#FFFFFF', tooltipBorder: 'rgba(80,88,120,0.18)', crosshair: 'rgba(48,58,86,0.45)',
    series: [1, 2, 3, 4, 5, 6, 7, 8].map((i) => cssv(`--series-${i}`, '#888')),
  };
  const FONT = cssv('--font', 'system-ui, sans-serif');
  const STATES = new Set(['HEALTHY', 'DEGRADED', 'CRITICAL']);
  const ICON = { HEALTHY: '●', DEGRADED: '⚠︎', CRITICAL: '✖︎' };
  const sevIcon = (s) => ICON[s] || '•';
  const sevColor = (s) => (s === 'CRITICAL' ? COL.critical : s === 'DEGRADED' ? COL.warning : COL.text2);

  // ================================================================ state
  const S = {
    hello: null, tz: 'UTC', clockFmt: makeClockFmt('UTC'), tzName: '', tzAt: 0,
    feeds: [], slot: new Map(),              // feed id -> categorical slot (1..8, 0 = other)
    paused: false, mode: 'replay', demoOn: false,
    cards: new Map(), recoItems: new Map(), recoPrev: new Map(),
    incRows: new Map(), seenInc: new Set(), incPrimed: false,
    chart: null, chartMarkers: [], xTicks: null, lastChart: null, tableView: false,
    chipsSig: null, chipRefs: new Map(), swSig: null,
    ws: null, retry: 0, connected: false, reconnectTimer: null, lastMsgAt: 0, helloRequested: false,
    drawerId: null, drawerTimer: null, drawerSig: '', drawerBusy: false, drawerErr: false, drawerReturn: null,
    tab: 'monitor', metrics: undefined, metricsBusy: false,
    page: 'login', seq: null, pollTimer: null, resyncing: false, tlSig: null, trCards: new Map(), trAlertSig: null, chatBusy: false,
  };
  const feedSlot = (id) => (S.slot.has(str(id)) ? S.slot.get(str(id)) : 0);
  const feedVar = (id) => { const n = feedSlot(id); return n ? `var(--series-${n})` : 'var(--series-other)'; };
  const feedHex = (id) => { const n = feedSlot(id); return n ? COL.series[n - 1] : COL.other; };
  const feedName = (id) => { const f = S.feeds.find((x) => x.id === str(id)); return f ? str(f.name, f.id) : str(id); };
  function feedBadge(id, extraTitle) {
    return h('span', { class: 'feed-badge', style: `--feed-color: ${feedVar(id)}`, title: `Feed ${str(id)} · ${feedName(id)}${extraTitle ? ' · ' + extraTitle : ''}` }, str(id, '?'));
  }

  // ================================================================ REST + toasts
  function toast(msg, kind = 'info', ms) {
    const box = $('#toasts');
    const t = h('div', { class: `toast ${kind}`, role: kind === 'error' ? 'alert' : 'status' }, msg);
    box.append(t);
    while (box.children.length > 4) box.firstElementChild.remove();
    setTimeout(() => t.remove(), ms || (kind === 'error' ? 6500 : 2600));
  }
  // ================================================================ auth (token in sessionStorage, refresh cookie is httpOnly)
  const store = {
    get(k) { try { return sessionStorage.getItem(k); } catch (e) { return null; } },
    set(k, v) { try { if (v == null) sessionStorage.removeItem(k); else sessionStorage.setItem(k, v); } catch (e) { /* storage blocked */ } },
  };
  const AUTH = {
    get token() { return store.get('fs_token') || ''; },
    set token(v) { store.set('fs_token', v || null); },
    get user() { try { return obj(JSON.parse(store.get('fs_user') || 'null')); } catch (e) { return null; } },
    set user(u) { store.set('fs_user', obj(u) ? JSON.stringify(u) : null); },
  };
  const HOMES = ['/ops', '/trader'];
  const homeOf = (u) => (u && HOMES.includes(u.home) ? u.home : u && u.role === 'trader' ? '/trader' : '/ops');
  let refreshing = null;
  function refreshToken() {
    if (!refreshing) {
      refreshing = (async () => {
        try {
          const r = await fetch('/api/auth/refresh', { method: 'POST', credentials: 'include', headers: { Accept: 'application/json' } });
          if (!r.ok) return false;
          const d = await r.json();
          if (!d || !d.access_token) return false;
          AUTH.token = d.access_token;
          if (obj(d.user)) AUTH.user = d.user;
          return true;
        } catch (e) { return false; }
      })();
      refreshing.finally(() => setTimeout(() => { refreshing = null; }, 0));
    }
    return refreshing;
  }
  function goLogin(msg) {
    AUTH.token = null;
    AUTH.user = null;
    if (msg) store.set('fs_msg', msg);
    if (location.pathname !== '/login') location.assign('/login');
  }
  /** fetch with the bearer token; a 401 triggers one refresh + retry, then /login. */
  async function authFetch(path, opts = {}, retried = false) {
    const o = { ...opts, credentials: 'include', headers: { Accept: 'application/json', ...(opts.headers || {}) } };
    if (AUTH.token) o.headers.Authorization = `Bearer ${AUTH.token}`;
    const res = await fetch(path, o);
    if (res.status === 401) {
      if (!retried && (await refreshToken())) return authFetch(path, opts, true);
      goLogin('Your session has expired. Please sign in again.');
      const err = new Error('not signed in');
      err.silent = true;
      throw err;
    }
    return res;
  }
  /** fetch JSON; on failure show a toast and resolve to null (never throws). */
  async function send(method, path, body, { quiet = false } = {}) {
    const opts = { method, headers: {} };
    if (body !== undefined) { opts.headers['Content-Type'] = 'application/json'; opts.body = JSON.stringify(body); }
    try {
      const res = await authFetch(path, opts);
      if (res.status === 403) { const err = new Error('Not permitted for your role/region'); err.status = 403; throw err; }
      const text = await res.text();
      let data = null;
      if (text) { try { data = JSON.parse(text); } catch (e) { data = null; } }
      if (!res.ok) {
        let detail = data && data.detail != null ? data.detail : res.statusText;
        if (typeof detail !== 'string') detail = JSON.stringify(detail);
        const err = new Error(`${res.status} ${str(detail).slice(0, 220)}`.trim());
        err.status = res.status;
        throw err;
      }
      return data === null ? {} : data;
    } catch (e) {
      if (e.silent) return null;
      if (e.status === 403) { toast('Not permitted for your role/region', 'error'); return null; }
      if (!quiet) toast(`${method} ${path} failed: ${e.message || e}`, 'error');
      return null;
    }
  }
  const control = (body) => send('POST', '/api/control', body);

  // ================================================================ WebSocket
  function connect() {
    clearTimeout(S.reconnectTimer);
    if (!AUTH.token) { goLogin(); return; }
    const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
    let ws;
    try { ws = new WebSocket(`${proto}//${location.host}/ws?token=${encodeURIComponent(AUTH.token)}`); } catch (e) { scheduleReconnect(); return; }
    S.ws = ws;
    ws.onopen = () => {
      if (S.ws !== ws) return;
      S.connected = true; S.retry = 0; S.seq = null;
      stopPolling();
      updateConn();
    };
    ws.onmessage = (ev) => {
      if (S.ws !== ws) return;
      let msg;
      try { msg = JSON.parse(ev.data); } catch (e) { return; }
      if (obj(msg) && isNum(msg.seq)) {
        if (S.seq != null && msg.seq > S.seq + 1) resync(); // missed messages: take a full snapshot
        S.seq = msg.seq;
      }
      S.lastMsgAt = Date.now();
      handleMessage(msg);
    };
    ws.onclose = (ev) => {
      if (S.ws !== ws) return;
      S.connected = false; S.ws = null;
      startPolling();
      updateConn();
      if (ev.code === 4401) {
        refreshToken().then((ok) => (ok ? connect() : goLogin('Your session has expired. Please sign in again.')));
        return;
      }
      scheduleReconnect();
    };
    ws.onerror = () => { try { ws.close(); } catch (e) { /* ignore */ } };
  }
  function scheduleReconnect() {
    S.retry += 1;
    const delay = Math.min(10000, 500 * 2 ** Math.min(S.retry - 1, 5)) * (0.8 + Math.random() * 0.4);
    clearTimeout(S.reconnectTimer);
    S.reconnectTimer = setTimeout(connect, delay);
  }
  function setBanner(text, kind) {
    const b = $('#conn-banner');
    b.hidden = !text;
    b.classList.toggle('warn', kind === 'warn');
    if (text) setText($('#conn-text'), text);
    document.body.classList.toggle('disconnected', !!text && kind !== 'warn');
  }
  const withType = (st) => (obj(st) && !st.type ? { ...st, type: S.page === 'trader' ? 'trader_tick' : 'tick' } : st);
  function startPolling() {
    if (S.pollTimer) return;
    S.pollTimer = setInterval(poll, 2000);
    poll();
  }
  function stopPolling() { clearInterval(S.pollTimer); S.pollTimer = null; }
  async function poll() {
    if (S.connected) { stopPolling(); return; }
    if (!S.hello) { const hl = await send('GET', '/api/hello', undefined, { quiet: true }); if (hl && !S.hello) handleMessage({ ...hl, type: 'hello' }); }
    const st = await send('GET', '/api/state', undefined, { quiet: true });
    if (st && !S.connected) { S.lastMsgAt = Date.now(); handleMessage(withType(st)); }
  }
  async function resync() {
    if (S.resyncing) return;
    S.resyncing = true;
    const st = await send('GET', '/api/state', undefined, { quiet: true });
    S.resyncing = false;
    if (st) handleMessage(withType(st));
  }
  /** live = socket streaming; polling = socket down, REST every 2 s; stale = nothing new for > 5 s. */
  function updateConn() {
    if (S.page === 'login') return;
    const age = S.lastMsgAt ? (Date.now() - S.lastMsgAt) / 1000 : Infinity;
    const state = !S.lastMsgAt ? 'connecting' : age > 5 && !S.paused ? 'stale' : S.connected ? 'live' : 'polling';
    const pill = $('#conn-pill');
    if (pill.dataset.state !== state) {
      pill.dataset.state = state;
      setText($('#conn-label'), state);
      pill.title = { live: 'Live: streaming over WebSocket', polling: 'Live stream down: polling every 2 s', stale: 'No new data for more than 5 s', connecting: 'Connecting…' }[state];
    }
    if (state === 'stale') setBanner(`No updates from FeedSentinel for ${Math.round(age)} s. Reconnecting…`);
    else if (state === 'polling') setBanner('Live stream interrupted. Polling every 2 s while reconnecting…', 'warn');
    else setBanner(null);
  }

  function handleMessage(msg) {
    if (!obj(msg)) return;
    if (msg.type === 'hello') {
      if (HOMES.includes('/' + msg.view) && msg.view !== S.page) { location.replace('/' + msg.view); return; }
      if (obj(msg.user)) { AUTH.user = msg.user; renderUser(); }
      safe(applyHello, msg);
    } else if (msg.type === 'tick') {
      if (S.page !== 'ops') return;
      if (!S.hello) requestHello();
      safe(applyTick, msg);
    } else if (msg.type === 'trader_tick') {
      if (S.page !== 'trader') return;
      safe(applyTraderTick, msg);
    }
  }
  async function requestHello() {
    if (S.helloRequested) return;
    S.helloRequested = true;
    const hl = await send('GET', '/api/hello', undefined, { quiet: true });
    if (hl && !S.hello) handleMessage({ ...hl, type: 'hello' });
  }

  // ================================================================ hello: full re-initialisation
  function applyHello(hl) {
    const prevMode = S.hello && S.hello.mode;
    S.hello = hl;
    S.tz = str(hl.tz, 'UTC');
    S.clockFmt = makeClockFmt(S.tz);
    S.tzName = '';
    S.mode = str(hl.mode, 'replay');
    if (prevMode && prevMode !== S.mode) S.demoOn = false;
    if (S.page !== 'ops') return; // the trader view only needs the clock settings
    S.feeds = [];
    S.slot = new Map();
    for (const f of arr(hl.feeds)) addFeed(f);

    setText($('#source'), str(hl.source));
    $('#source').title = str(hl.source);
    if (hl.product && hl.product !== 'FeedSentinel') setText($('.logo'), hl.product);
    document.title = `${str(hl.product, 'FeedSentinel')} · ${S.mode.toUpperCase()}`;

    const sel = $('#speed-select');
    sel.textContent = '';
    for (const sp of arr(hl.speeds)) sel.append(h('option', { value: sp }, `${sp}×`));
    if (isNum(hl.speed)) sel.value = String(hl.speed);
    renderModeControls(S.mode, hl.speed);

    const ssel = $('#symbol-select');
    ssel.textContent = '';
    for (const s of arr(hl.symbols)) ssel.append(h('option', { value: s }, s));

    buildFeedCards();
    $('#reco-list').textContent = '';
    S.recoItems.clear(); S.recoPrev.clear();
    buildChartDatasets();
    buildChaos();
    $('#incident-list').textContent = '';
    S.incRows.clear(); S.incPrimed = false;
    S.chipsSig = null; S.swSig = null; // force a redraw even when the next list is empty
    renderModels();
    renderDemoBtn();
  }

  function addFeed(f) {
    const fo = obj(f);
    if (!fo || fo.id == null) return null;
    const id = String(fo.id);
    if (S.slot.has(id)) return S.feeds.find((x) => x.id === id);
    const feed = { id, name: str(fo.name, id), role: str(fo.role) };
    S.feeds.push(feed);
    S.slot.set(id, S.feeds.length <= 8 ? S.feeds.length : 0);
    return feed;
  }

  function renderModels() {
    const hl = S.hello || {};
    const m = obj(hl.models) || {};
    const llm = obj(hl.llm) || {};
    const yn = (v) => (v === true ? '✓' : v === false ? 'not loaded' : '–');
    const expl = llm.enabled ? `LLM (${str(llm.provider, 'on')})` : 'template';
    setText($('#models-status'), `AI models: anomaly ${yn(m.anomaly)} · classifier ${yn(m.classifier)} · explanations: ${expl}`);
  }

  // ================================================================ tick
  function applyTick(t) {
    safe(renderHeader, t);
    safe(renderFeeds, arr(t.feeds));
    safe(renderReco, obj(t.recommended));
    if (S.tab === 'monitor' && !document.hidden) safe(renderChart, obj(t.chart));
    else S.lastChart = obj(t.chart);
    safe(renderIncidents, arr(t.incidents));
    const chaos = obj(t.chaos) || {};
    safe(renderActiveFaults, arr(chaos.active));
    safe(renderStopwatch, obj(chaos.stopwatch));
    safe(renderSelf, obj(t.self));
    safe(renderTimeline, arr(t.timeline));
    safe(renderBandStats, t);
  }

  // ---------------------------------------------------------------- header
  function renderHeader(t) {
    setText($('#clock'), t.clock || fmtClock(t.t_ms));
    setText($('#clock-date'), fmtDate(t.date));
    if (!S.tzName || (isNum(t.t_ms) && Math.abs(t.t_ms - S.tzAt) > 3600e3)) { S.tzName = tzShort(t.t_ms); S.tzAt = t.t_ms; }
    setText($('#clock-tz'), S.tzName);
    setText($('#session'), str(t.session, '—'));
    if (t.mode) S.mode = String(t.mode);
    S.paused = t.paused === true;
    renderModeControls(S.mode, t.speed);
  }
  function renderModeControls(mode, speed) {
    const mb = $('#mode-badge');
    setText(mb, str(mode, '—').toUpperCase());
    mb.classList.toggle('live', mode === 'live');
    const speeds = arr(S.hello && S.hello.speeds);
    const sb = $('#speed-badge');
    sb.hidden = !(isNum(speed) && mode !== 'live');
    setText(sb, isNum(speed) ? `${speed}×` : '');
    const sel = $('#speed-select');
    sel.disabled = speeds.length <= 1;
    if (isNum(speed) && document.activeElement !== sel && sel.value !== String(speed)) {
      if (![...sel.options].some((o) => o.value === String(speed))) sel.append(h('option', { value: speed }, `${speed}×`));
      sel.value = String(speed);
    }
    $('#paused-badge').hidden = !S.paused;
    const pb = $('#pause-btn');
    setText(pb, S.paused ? '▶︎ Resume' : '❚❚ Pause');
    setAttr(pb, 'aria-label', S.paused ? 'Resume replay' : 'Pause replay');
    for (const b of document.querySelectorAll('.seg-btn')) setAttr(b, 'aria-pressed', b.dataset.mode === mode ? 'true' : 'false');
  }
  function renderDemoBtn() {
    const b = $('#demo-btn');
    setAttr(b, 'aria-pressed', S.demoOn ? 'true' : 'false');
    setText(b, S.demoOn ? 'Stop autopilot' : 'Demo autopilot');
  }

  // ---------------------------------------------------------------- feed cards
  function buildFeedCards() {
    const wrap = $('#feed-cards');
    wrap.textContent = '';
    S.cards.clear();
    wrap.style.setProperty('--n-feeds', String(Math.max(1, Math.min(S.feeds.length, 4))));
    for (const f of S.feeds) wrap.append(makeCard(f));
  }
  function makeCard(f) {
    const R = {};
    const r = (name, el) => (R[name] = el);
    const spark = svg('svg', { class: 'spark', viewBox: '0 0 120 40', 'aria-hidden': 'true', focusable: 'false' });
    r('line', svg('polyline', { class: 'spark-line', points: '' }));
    r('dot', svg('circle', { class: 'spark-dot', r: 3.5, cx: -10, cy: -10 }));
    spark.append(svg('line', { class: 'spark-ref', x1: 0, x2: 120, y1: 39, y2: 39 }), R.line, R.dot);
    const card = h('article', { class: 'feed-card', 'data-feed': f.id, style: `--feed-color: ${feedVar(f.id)}`, 'aria-label': `Feed ${f.id}, ${f.name}` },
      h('div', { class: 'fc-head' },
        h('span', { class: 'feed-badge' }, f.id),
        h('span', { class: 'fc-name' }, f.name),
        h('span', { class: 'fc-role', title: f.role }, f.role),
        h('span', { class: 'fc-trust', title: 'Consensus trust weight, 0 to 1' }, 'trust ', r('trust', h('b', { class: 'num' }, '–')))),
      h('div', { class: 'fc-main' },
        h('div', { class: 'fc-health', title: 'Health score, 0 to 100' },
          r('health', h('span', { class: 'fc-health-num num' }, '–')),
          h('span', { class: 'fc-health-lbl' }, 'health')),
        h('div', { class: 'fc-status' },
          r('pill', h('span', { class: 'state-pill' }, '…')),
          r('headline', h('div', { class: 'fc-headline' }, ''))),
        h('div', { class: 'fc-spark', title: 'Health over the last 120 market seconds (scale 0 to 100)' }, spark, h('div', { class: 'spark-cap' }, 'health · 2 min'))),
      h('dl', { class: 'fc-stats' },
        h('div', null, h('dt', null, 'msg/s'), r('rate', h('dd', { class: 'num' }, '–'))),
        h('div', null, h('dt', null, 'latency p50 / p99'), h('dd', { class: 'num' }, r('lat', h('span', null, '–')), h('span', { class: 'unit' }, ' ms'))),
        h('div', null, h('dt', null, 'gaps · dups · quarantined'),
          h('dd', { class: 'num' }, r('gaps', h('span', null, '–')), ' · ', r('dups', h('span', null, '–')), ' · ', r('quar', h('span', null, '–'))))),
      h('div', { class: 'fc-ai' },
        h('div', { class: 'fc-ml', title: 'Isolation Forest anomaly score (bar) vs its alert threshold (white tick)' },
          h('span', { class: 'k' }, 'AI anomaly'),
          r('meter', h('div', { class: 'meter', 'aria-hidden': 'true' }, r('mfill', h('div', { class: 'meter-fill' })), r('mthr', h('div', { class: 'meter-thr' })))),
          r('mlv', h('span', { class: 'v num' }, '–'))),
        h('div', { class: 'fc-diag', title: 'Fault classifier: most likely fault and its probability' },
          h('span', { class: 'k' }, 'Diagnosis'), r('diag', h('span', { class: 'v' }, '–')))));
    R.card = card;
    R.state = '';
    S.cards.set(f.id, R);
    return card;
  }

  function renderFeeds(list) {
    for (const f of list) {
      if (!obj(f) || f.id == null) continue;
      const id = String(f.id);
      let R = S.cards.get(id);
      if (!R) { // feed not announced in hello: add it rather than drop it
        const feed = addFeed({ id, name: f.name });
        if (!feed) continue;
        $('#feed-cards').append(makeCard(feed));
        $('#feed-cards').style.setProperty('--n-feeds', String(Math.max(1, Math.min(S.feeds.length, 4))));
        R = S.cards.get(id);
        buildChartDatasets();
        buildChaos();
      }
      updateCard(R, f);
    }
  }

  function updateCard(R, f) {
    const state = STATES.has(f.state) ? f.state : 'UNKNOWN';
    if (R.state !== state) {
      if (R.state) R.card.classList.remove(`state-${R.state}`);
      R.card.classList.add(`state-${state}`);
      R.state = state;
      setText(R.pill, `${sevIcon(state)} ${state === 'UNKNOWN' ? str(f.state, 'UNKNOWN') : state}`);
    }
    setText(R.health, isNum(f.health) ? Math.round(f.health) : '–');
    const codes = arr(f.codes).map(String);
    const headline = str(f.headline) || (state === 'HEALTHY' ? 'All checks passing' : codes.join(' · '));
    setText(R.headline, headline);
    setAttr(R.headline, 'title', headline);
    setText(R.trust, fmtFix(f.trust, 2));
    setText(R.rate, fmtInt(f.msg_rate));
    setText(R.lat, `${fmtMs(f.lat_p50_ms)} / ${fmtMs(f.lat_p99_ms)}`);
    for (const [k, el] of [['gaps', R.gaps], ['dups', R.dups], ['quarantined', R.quar]]) {
      setText(el, fmtInt(f[k]));
      el.classList.toggle('nz', isNum(f[k]) && f[k] > 0);
    }
    // AI anomaly score vs threshold
    const ml = obj(f.ml);
    if (ml && isNum(ml.score)) {
      const thr = isNum(ml.threshold) ? ml.threshold : null;
      const scale = Math.max(1, thr != null ? thr * 1.6 : 0, ml.score);
      R.mfill.style.width = `${Math.max(0, Math.min(100, (ml.score / scale) * 100))}%`;
      R.mthr.hidden = thr == null;
      if (thr != null) R.mthr.style.left = `calc(${(thr / scale) * 100}% - 1px)`;
      const flag = ml.flag === true || (ml.flag == null && thr != null && ml.score > thr);
      R.meter.classList.toggle('flag', flag);
      setText(R.mlv, `${fmtFix(ml.score, 2)}${thr != null ? ' / ' + fmtFix(thr, 2) : ''}${flag ? ' ▲' : ''}`);
      setAttr(R.mlv, 'title', flag ? 'Above threshold: flagged as anomalous' : 'Below threshold');
    } else {
      R.mfill.style.width = '0%';
      R.mthr.hidden = true;
      R.meter.classList.remove('flag');
      const loaded = obj(S.hello && S.hello.models);
      setText(R.mlv, loaded && loaded.anomaly === false ? 'no model' : '–');
    }
    const dg = obj(f.diagnosis);
    if (dg && dg.label != null) {
      setText(R.diag, `${str(dg.label)}${isNum(dg.p) ? ' ' + fmtPct(dg.p) : ''}`);
    } else {
      const loaded = obj(S.hello && S.hello.models);
      setText(R.diag, loaded && loaded.classifier === false ? 'no model' : '–');
    }
    updateSpark(R, arr(f.history));
  }

  function updateSpark(R, hist) {
    const vals = hist.map((v) => (isNum(v) ? Math.max(0, Math.min(100, v)) : null));
    const n = vals.length;
    const W = 120, dx = W / 119;
    const y = (v) => 39 - (v / 100) * 37;
    let pts = '', last = null;
    for (let i = 0; i < n; i++) {
      if (vals[i] == null) continue;
      const x = W - (n - 1 - i) * dx;
      const p = `${x.toFixed(1)},${y(vals[i]).toFixed(1)}`;
      pts += (pts ? ' ' : '') + p;
      last = [x, y(vals[i])];
    }
    if (R.line.getAttribute('points') === pts) return;
    R.line.setAttribute('points', pts);
    R.dot.setAttribute('cx', last ? last[0].toFixed(1) : -10);
    R.dot.setAttribute('cy', last ? last[1].toFixed(1) : -10);
  }

  // ---------------------------------------------------------------- recommended source
  function renderReco(rec) {
    const list = $('#reco-list');
    const r = rec || {};
    const syms = arr(S.hello && S.hello.symbols).map(String);
    for (const k of Object.keys(r)) if (!syms.includes(k)) syms.push(k);
    const keep = new Set();
    for (const sym of syms) {
      if (!(sym in r)) continue;
      keep.add(sym);
      let it = S.recoItems.get(sym);
      if (!it) {
        it = { el: h('span', { class: 'reco-item' }, h('span', null, sym), h('span', { class: 'arrow', 'aria-hidden': 'true' }, '→')) };
        it.badgeHost = h('span');
        it.el.append(it.badgeHost);
        S.recoItems.set(sym, it);
        list.append(it.el);
      }
      const fid = r[sym] == null ? '' : String(r[sym]);
      if (it.fid !== fid) {
        it.badgeHost.textContent = '';
        it.badgeHost.append(fid ? feedBadge(fid) : h('span', { class: 'muted' }, '—'));
        setAttr(it.el, 'title', fid ? `${sym}: use feed ${fid} (${feedName(fid)})` : `${sym}: no healthy source`);
        if (it.fid !== undefined) {
          it.el.classList.remove('changed');
          void it.el.offsetWidth; // restart the flash animation
          it.el.classList.add('changed');
          clearTimeout(it.timer);
          it.timer = setTimeout(() => it.el.classList.remove('changed'), 3000);
        }
        it.fid = fid;
      }
    }
    for (const [sym, it] of S.recoItems) if (!keep.has(sym)) { it.el.remove(); S.recoItems.delete(sym); }
    const placeholder = list.querySelector(':scope > .muted');
    if (!S.recoItems.size && !placeholder) list.append(h('span', { class: 'muted' }, '—'));
    else if (S.recoItems.size && placeholder) placeholder.remove();
  }

  // ================================================================ price chart
  const markersPlugin = {
    id: 'fsMarkers',
    afterDatasetsDraw(chart) {
      const ms = S.chartMarkers;
      const a = chart.chartArea, x = chart.scales.x;
      if (!ms || !ms.length || !a || !x) return;
      const ctx = chart.ctx;
      ctx.save();
      ctx.font = `700 11px ${FONT}`;
      ctx.textBaseline = 'middle';
      const rows = [];
      for (const m of ms) {
        const px = x.getPixelForValue(m.t_ms);
        if (!(px >= a.left - 1 && px <= a.right + 1)) continue;
        const col = sevColor(m.severity);
        ctx.strokeStyle = col;
        ctx.lineWidth = 2;
        ctx.setLineDash(m.severity === 'CRITICAL' ? [] : [5, 4]);
        ctx.beginPath(); ctx.moveTo(px, a.top); ctx.lineTo(px, a.bottom); ctx.stroke();
        ctx.setLineDash([]);
        const text = `${sevIcon(m.severity)} ${str(m.feed)} ${str(m.code)}`.trim();
        const w = ctx.measureText(text).width + 10, hh = 16;
        const lx = Math.max(a.left, Math.min(px - 1, a.right - w));
        let row = 0;
        while (row < 3 && rows[row] != null && lx < rows[row]) row++;
        if (row >= 3) continue;
        rows[row] = lx + w + 3;
        const ly = a.top + 2 + row * (hh + 2);
        ctx.fillStyle = m.severity === 'CRITICAL' ? COL.criticalStrong : col;
        ctx.beginPath();
        if (ctx.roundRect) ctx.roundRect(lx, ly, w, hh, 8); else ctx.rect(lx, ly, w, hh);
        ctx.fill();
        ctx.fillStyle = m.severity === 'CRITICAL' ? '#ffffff' : '#172038';
        ctx.fillText(text, lx + 5, ly + hh / 2 + 0.5);
      }
      ctx.restore();
    },
  };
  const crosshairPlugin = {
    id: 'fsCrosshair',
    afterDatasetsDraw(chart) {
      const act = chart.tooltip && chart.tooltip.getActiveElements ? chart.tooltip.getActiveElements() : [];
      if (!act || !act.length) return;
      const a = chart.chartArea;
      const x = Math.round(act[0].element.x) + 0.5;
      const ctx = chart.ctx;
      ctx.save();
      ctx.strokeStyle = COL.crosshair;
      ctx.lineWidth = 1;
      ctx.beginPath(); ctx.moveTo(x, a.top); ctx.lineTo(x, a.bottom); ctx.stroke();
      ctx.restore();
    },
  };

  function initChart() {
    const canvas = $('#price-chart');
    if (typeof window.Chart === 'undefined') {
      const e = $('#chart-empty');
      e.hidden = false;
      e.textContent = 'Chart library failed to load (/static/vendor/chart.umd.min.js)';
      return;
    }
    const box = h('div', { class: 'chart-box' });
    canvas.replaceWith(box);
    box.append(canvas);
    const Chart = window.Chart;
    Chart.defaults.font.family = FONT;
    Chart.defaults.color = COL.axisText;
    S.chart = new Chart(canvas, {
      type: 'line',
      data: { datasets: [] },
      options: {
        animation: false,
        responsive: true,
        maintainAspectRatio: false,
        parsing: false,
        normalized: true,
        spanGaps: false,
        interaction: { mode: 'index', intersect: false },
        elements: {
          point: { radius: 0, hitRadius: 8, hoverRadius: 4.5, hoverBorderWidth: 2 },
          line: { tension: 0, borderJoinStyle: 'round', borderCapStyle: 'round' },
        },
        scales: {
          x: {
            type: 'linear',
            grid: { color: COL.grid, lineWidth: 1, drawTicks: false },
            border: { color: COL.axis },
            ticks: { color: COL.axisText, maxRotation: 0, autoSkip: true, autoSkipPadding: 16, padding: 6, includeBounds: false, font: { size: 12 }, callback: (v) => fmtClock(v) },
            afterBuildTicks: (sc) => { if (S.xTicks && S.xTicks.length) sc.ticks = S.xTicks.map((v) => ({ value: v })); },
          },
          y: {
            type: 'linear',
            position: 'right',
            grid: { color: COL.grid, lineWidth: 1, drawTicks: false },
            border: { display: false },
            ticks: { color: COL.axisText, padding: 8, maxTicksLimit: 6, includeBounds: false, font: { size: 12 }, callback: (v) => fmtPrice(v) },
          },
        },
        plugins: {
          legend: { display: false },
          decimation: { enabled: false },
          tooltip: {
            backgroundColor: COL.tooltipBg,
            borderColor: COL.tooltipBorder,
            borderWidth: 1,
            titleColor: COL.text1,
            bodyColor: COL.text1,
            padding: 10,
            boxWidth: 16,
            boxHeight: 3,
            boxPadding: 6,
            titleFont: { weight: '700', size: 13 },
            bodyFont: { size: 13 },
            filter: (it) => isNum(it.parsed && it.parsed.y),
            itemSort: (a, b) => a.datasetIndex - b.datasetIndex,
            callbacks: {
              title: (items) => (items.length ? fmtClockMs(items[0].parsed.x) : ''),
              label: (it) => `${fmtPrice(it.parsed.y)}   ${it.dataset.label}`,
              labelColor: (it) => ({ borderColor: it.dataset.borderColor, backgroundColor: it.dataset.borderColor, borderWidth: 0, borderRadius: 1 }),
            },
          },
        },
      },
      plugins: [markersPlugin, crosshairPlugin],
    });
  }

  const DASHES = [[], [], [7, 4], [2, 3], [10, 3, 2, 3]];
  function makeDataset(key, i) {
    if (key === 'consensus') {
      return { _key: key, label: 'Consensus', data: [], borderColor: COL.consensus, backgroundColor: COL.consensus, borderWidth: 4, order: 100,
        pointHoverBackgroundColor: COL.text1, pointHoverBorderColor: COL.surface1 };
    }
    const known = S.slot.has(key);
    const color = known ? feedHex(key) : COL.other;
    return { _key: key, label: known ? `${key} · ${feedName(key)}` : key, data: [], borderColor: color, backgroundColor: color, borderWidth: 2,
      borderDash: DASHES[Math.min(i, DASHES.length - 1)] || [], order: i,
      pointHoverBackgroundColor: color, pointHoverBorderColor: COL.surface1 };
  }
  function buildChartDatasets(extraKeys) {
    const keys = ['consensus', ...S.feeds.map((f) => f.id)];
    for (const k of arr(extraKeys)) if (!keys.includes(k)) keys.push(k);
    if (S.chart) {
      S.chart.data.datasets = keys.map((k, i) => makeDataset(k, k === 'consensus' ? 0 : i - 1));
      S.chart.update('none');
    }
    // HTML legend (line keys mirror the marks)
    const lg = $('#chart-legend');
    lg.textContent = '';
    for (const [i, k] of keys.entries()) {
      if (k === 'consensus') {
        lg.append(h('span', { class: 'legend-item' }, h('span', { class: 'legend-key thick', style: `--c: ${COL.consensus}` }), 'Consensus'));
      } else {
        const dash = (DASHES[Math.min(i - 1, DASHES.length - 1)] || []).length > 0;
        lg.append(h('span', { class: 'legend-item' },
          h('span', { class: `legend-key${dash ? ' dashed' : ''}`, style: `--c: ${S.slot.has(k) ? feedVar(k) : 'var(--series-other)'}` }),
          S.slot.has(k) ? `${k} ${feedName(k)}` : k));
      }
    }
    lg.append(h('span', { class: 'legend-sep', 'aria-hidden': 'true' }),
      h('span', { class: 'legend-item', title: 'Vertical lines mark incidents: solid red = CRITICAL, dashed amber = DEGRADED' },
        h('span', { class: 'legend-key marker' }), 'incident'));
    S.chartKeys = keys;
  }

  function niceXTicks(t0, t1) {
    const width = S.chart && S.chart.chartArea ? S.chart.chartArea.width : 900;
    const target = Math.max(2, Math.floor(width / 90));
    const spanS = (t1 - t0) / 1000;
    const steps = [1, 2, 5, 10, 15, 30, 60, 120, 300, 600, 900, 1800, 3600, 7200, 14400];
    const step = (steps.find((s) => spanS / s <= target) || 14400) * 1000;
    const out = [];
    for (let v = Math.ceil(t0 / step) * step; v <= t1 && out.length < 60; v += step) out.push(v);
    return out;
  }

  function renderChart(ch) {
    S.lastChart = ch;
    const empty = $('#chart-empty');
    const t = ch ? arr(ch.t_ms) : [];
    const series = (ch && obj(ch.series)) || {};
    // keep the symbol selector in sync with the server
    const ssel = $('#symbol-select');
    if (ch && ch.symbol != null && document.activeElement !== ssel && ssel.value !== String(ch.symbol)) {
      if (![...ssel.options].some((o) => o.value === String(ch.symbol))) ssel.append(h('option', { value: ch.symbol }, ch.symbol));
      ssel.value = String(ch.symbol);
    }
    empty.hidden = t.length > 0 || S.tableView;
    if (S.tableView) renderChartTable(t, series);
    if (!S.chart) return;
    const extra = Object.keys(series).filter((k) => !S.chartKeys.includes(k));
    if (extra.length) buildChartDatasets(extra);

    const n = t.length;
    // y range: anchored on consensus (fallback: all series); feed values far outside are flagged off-scale
    const vals = (k) => arr(series[k]).filter(isNum);
    let base = vals('consensus');
    if (!base.length) base = Object.keys(series).flatMap(vals);
    let lo = Infinity, hi = -Infinity;
    for (const v of base) { if (v < lo) lo = v; if (v > hi) hi = v; }
    const off = [];
    if (base.length) {
      const mid = (lo + hi) / 2;
      const tol = Math.max(Math.abs(mid) * 0.05, (hi - lo) * 2);
      const aLo = lo - tol, aHi = hi + tol;
      for (const k of Object.keys(series)) {
        if (k === 'consensus') continue;
        let worst = null;
        for (const v of vals(k)) {
          if (v >= aLo && v <= aHi) { if (v < lo) lo = v; if (v > hi) hi = v; } else if (worst == null || Math.abs(v - mid) > Math.abs(worst - mid)) worst = v;
        }
        if (worst != null) off.push([k, worst]);
      }
      const minSpan = Math.max(Math.abs(mid) * 0.0006, 0.02);
      if (hi - lo < minSpan) { const c = (hi + lo) / 2; lo = c - minSpan / 2; hi = c + minSpan / 2; }
      const pad = (hi - lo) * 0.08;
      S.chart.options.scales.y.min = lo - pad;
      S.chart.options.scales.y.max = hi + pad;
    } else {
      S.chart.options.scales.y.min = undefined;
      S.chart.options.scales.y.max = undefined;
    }
    for (const ds of S.chart.data.datasets) {
      const src = arr(series[ds._key]);
      const pts = new Array(n);
      for (let i = 0; i < n; i++) pts[i] = { x: t[i], y: isNum(src[i]) ? src[i] : null };
      ds.data = pts;
    }
    if (n >= 2 && isNum(t[0]) && isNum(t[n - 1]) && t[n - 1] > t[0]) {
      S.chart.options.scales.x.min = t[0];
      S.chart.options.scales.x.max = t[n - 1];
      S.xTicks = niceXTicks(t[0], t[n - 1]);
    } else {
      S.chart.options.scales.x.min = undefined;
      S.chart.options.scales.x.max = undefined;
      S.xTicks = null;
    }
    S.chartMarkers = arr(ch && ch.markers).filter((m) => obj(m) && isNum(m.t_ms)).sort((a, b) => a.t_ms - b.t_ms);
    // off-scale note (a decimal-shifted feed must not flatten everyone else)
    const note = $('#chart-note');
    const sig = off.map(([k, v]) => `${k}:${fmtPrice(v)}`).join('|');
    if (note.dataset.sig !== sig) {
      note.dataset.sig = sig;
      note.textContent = '';
      for (const [k, v] of off) {
        note.append(h('span', { class: 'off', title: `Feed ${k} prices are far outside the consensus range and are clipped from the chart` },
          feedBadge(k), `off-scale ${v > hi ? '▲' : '▼'} ${fmtPrice(v)}`));
      }
    }
    S.chart.update('none');
  }

  function renderChartTable(t, series) {
    const host = $('#chart-table');
    const keys = S.chartKeys || ['consensus'];
    const rows = [];
    for (let i = t.length - 1; i >= 0 && rows.length < 20; i--) rows.push(i);
    const table = h('table', { class: 'table' },
      h('caption', { class: 'sr-only' }, 'Latest price samples, newest first'),
      h('thead', null, h('tr', null, h('th', { scope: 'col' }, `Time (${S.tzName || S.tz})`),
        keys.map((k) => h('th', { scope: 'col', class: 'n' }, k === 'consensus' ? 'Consensus' : `${k} ${feedName(k)}`)))),
      h('tbody', null, rows.map((i) => h('tr', null, h('td', { class: 'num' }, fmtClockMs(t[i])),
        keys.map((k) => h('td', { class: 'n' }, fmtPrice(arr(series[k])[i])))))));
    host.textContent = '';
    host.append(table);
  }

  // ================================================================ incidents
  function shortAction(a) {
    const s = str(a).split(/;|\. /)[0].trim();
    return s.length > 90 ? s.slice(0, 88) + '…' : s;
  }
  function makeIncRow(id) {
    const R = {};
    const r = (n, e) => (R[n] = e);
    R.el = h('button', { type: 'button', class: 'inc-row', 'data-id': id, onclick: () => openDrawer(id) },
      r('sev', h('span', { class: 'inc-sev', 'aria-hidden': 'true' }, '')),
      r('feedHost', h('span', { class: 'inc-feed' })),
      r('code', h('span', { class: 'inc-code' }, '')),
      h('span', { class: 'inc-stat' }, r('st', h('span', { class: 'st' }, '')), ' · ', r('opened', h('span', { class: 'num' }, '')), r('ack', h('span', { class: 'inc-fb inc-ack', hidden: true }, '')), r('fb', h('span', { class: 'inc-fb', hidden: true }, ''))),
      r('head', h('span', { class: 'inc-head' }, '')),
      r('act', h('span', { class: 'inc-act' }, '')),
      r('ttd', h('span', { class: 'inc-ttd num' }, '')),
      r('dur', h('span', { class: 'inc-dur num' }, '')));
    return R;
  }
  function updateIncRow(R, inc) {
    const sev = str(inc.severity, 'DEGRADED');
    const open = str(inc.status, 'OPEN') === 'OPEN';
    const cls = `inc-row sev-${sev} ${open ? 'open' : 'resolved'}${R.el.classList.contains('fresh') ? ' fresh' : ''}`;
    if (R.el.className !== cls) R.el.className = cls;
    setText(R.sev, sevIcon(sev));
    const fid = str(inc.feed);
    if (R.fid !== fid) { R.feedHost.textContent = ''; R.feedHost.append(feedBadge(fid)); R.fid = fid; }
    setText(R.code, str(inc.code, '?'));
    setText(R.st, open ? 'OPEN' : 'RESOLVED');
    setText(R.opened, str(inc.opened_clock) || fmtClock(inc.opened_ms));
    const fb = inc.feedback === 'confirmed' ? '✓ confirmed' : inc.feedback === 'false_positive' ? '✗ false positive' : '';
    R.fb.hidden = !fb;
    setText(R.fb, fb);
    R.ack.hidden = inc.acked_by == null;
    setText(R.ack, inc.acked_by != null ? '✓ ACK' : '');
    setAttr(R.ack, 'title', inc.acked_by != null ? `Acknowledged by ${str(inc.acked_by)}` : '');
    const head = str(inc.headline) || str(inc.title);
    setText(R.head, head);
    setAttr(R.head, 'title', `${str(inc.title)}${inc.title && inc.headline ? ': ' : ''}${str(inc.headline)}`);
    const act = shortAction(inc.action);
    setText(R.act, act ? `→ ${act}` : '');
    setAttr(R.act, 'title', str(inc.action));
    setText(R.ttd, isNum(inc.ttd_s) ? `TTD ${fmtSec(inc.ttd_s)}` : '');
    setAttr(R.ttd, 'title', isNum(inc.ttd_s) ? 'Time to detect, market seconds from fault injection' : '');
    setText(R.dur, isNum(inc.duration_s) ? (open ? `open ${fmtSec(inc.duration_s)}` : `lasted ${fmtSec(inc.duration_s)}`) : '');
    setAttr(R.el, 'aria-label', `${sev} ${str(inc.code)} on feed ${fid}: ${head}. ${open ? 'Open' : 'Resolved'}. Show why.`);
  }
  function renderIncidents(list) {
    const host = $('#incident-list');
    const items = list.filter((i) => obj(i) && i.id != null);
    // open first, otherwise keep server order
    const ordered = items.filter((i) => i.status === 'OPEN').concat(items.filter((i) => i.status !== 'OPEN'));
    const seen = new Set();
    let prev = null;
    for (const inc of ordered) {
      const id = String(inc.id);
      if (seen.has(id)) continue;
      seen.add(id);
      let R = S.incRows.get(id);
      if (!R) {
        R = makeIncRow(id);
        S.incRows.set(id, R);
        if (S.incPrimed && !S.seenInc.has(id)) {
          R.el.classList.add('fresh');
          setTimeout(() => R.el.classList.remove('fresh'), 2600);
        }
      }
      S.seenInc.add(id);
      updateIncRow(R, inc);
      const want = prev ? prev.el.nextSibling : host.firstChild;
      if (want !== R.el) host.insertBefore(R.el, want);
      prev = R;
    }
    for (const [id, R] of S.incRows) if (!seen.has(id)) { R.el.remove(); S.incRows.delete(id); }
    S.incPrimed = true;
    $('#incident-empty').hidden = S.incRows.size > 0;
    const nOpen = ordered.filter((i) => i.status === 'OPEN').length;
    setText($('#inc-counts'), `${nOpen} open · ${S.incRows.size} shown`);
  }

  // ================================================================ "Why?" drawer
  function openDrawer(id) {
    if (id == null || id === '') return;
    if (!S.drawerId) S.drawerReturn = document.activeElement;
    S.drawerId = String(id);
    S.drawerSig = '';
    S.drawerErr = false;
    $('#drawer').hidden = false;
    $('#drawer-backdrop').hidden = false;
    setText($('#drawer-id'), S.drawerId);
    setText($('#drawer-title'), 'Loading…');
    const body = $('#drawer-body');
    body.textContent = '';
    body.scrollTop = 0;
    $('#drawer-close').focus();
    refreshDrawer();
    clearInterval(S.drawerTimer);
    S.drawerTimer = setInterval(refreshDrawer, 2000);
  }
  function closeDrawer() {
    if (!S.drawerId) return;
    clearInterval(S.drawerTimer);
    S.drawerId = null;
    $('#drawer').hidden = true;
    $('#drawer-backdrop').hidden = true;
    const back = S.drawerReturn;
    S.drawerReturn = null;
    if (back && document.contains(back) && typeof back.focus === 'function') back.focus();
  }
  async function refreshDrawer() {
    const id = S.drawerId;
    if (!id || S.drawerBusy) return;
    S.drawerBusy = true;
    try {
      const res = await authFetch(`/api/incidents/${encodeURIComponent(id)}`);
      if (id !== S.drawerId) return;
      if (res.status === 404) {
        if (S.drawerSig !== '404') {
          S.drawerSig = '404';
          setText($('#drawer-title'), 'Incident not found');
          const body = $('#drawer-body');
          body.textContent = '';
          body.append(h('p', { class: 'drawer-err' }, `The server no longer knows ${id} (it may have been reset or the mode changed).`));
        }
        return;
      }
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const d = await res.json();
      if (id !== S.drawerId) return;
      const sig = JSON.stringify(d);
      if (sig === S.drawerSig) return;
      S.drawerSig = sig;
      renderDrawer(obj(d) || {});
      S.drawerErr = false;
    } catch (e) {
      if (e.silent) return;
      if (!S.drawerErr) toast(`Could not load ${id}: ${e.message || e}`, 'error');
      S.drawerErr = true;
    } finally {
      S.drawerBusy = false;
    }
  }

  function kvTable(o) {
    const entries = Object.entries(obj(o) || {});
    if (!entries.length) return h('p', { class: 'muted' }, 'No evidence recorded.');
    const fmtV = (v) => (isNum(v) ? (Number.isInteger(v) ? fmtInt(v) : String(+v.toFixed(4))) : v == null ? '–' : typeof v === 'object' ? JSON.stringify(v) : String(v));
    return h('table', { class: 'kv-table' }, h('tbody', null, entries.map(([k, v]) => h('tr', null, h('th', { scope: 'row' }, k.replace(/_/g, ' ')), h('td', null, fmtV(v))))));
  }

  function renderDrawer(d) {
    const body = $('#drawer-body');
    const focusKey = document.activeElement && body.contains(document.activeElement) ? document.activeElement.dataset.key : null;
    const scroll = body.scrollTop;
    const sev = str(d.severity, 'DEGRADED');
    const open = str(d.status, 'OPEN') === 'OPEN';
    setText($('#drawer-id'), str(d.id, S.drawerId));
    setText($('#drawer-title'), str(d.title) || str(d.code, 'Incident'));

    const secs = [];
    // meta
    const kv = (k, v, title) => h('div', { class: 'kv', title }, h('span', { class: 'k' }, k), h('span', { class: 'v num' }, v));
    secs.push(h('section', { class: 'd-sec' },
      h('div', { class: 'd-meta' },
        h('span', { class: `sev-pill ${sev}` }, `${sevIcon(sev)} ${sev}`),
        h('b', null, open ? 'OPEN' : 'RESOLVED'),
        h('span', { class: 'kv' }, h('span', { class: 'k' }, 'feed'), h('span', { class: 'v' }, feedBadge(str(d.feed)), ' ', str(d.feed_name) || feedName(d.feed))),
        kv('code', str(d.code, '–')),
        kv('opened', str(d.opened_clock) || fmtClock(d.opened_ms)),
        kv(open ? 'last seen' : 'closed', fmtClock(open ? d.last_ms : d.closed_ms)),
        kv('duration', fmtSec(d.duration_s)),
        kv('TTD', isNum(d.ttd_s) ? fmtSec(d.ttd_s) : 'n/a', 'Time to detect in market seconds; n/a when no injected fault explains the incident')),
      str(d.headline) ? h('p', { style: 'margin:.6rem 0 0;font-size:1.05rem' }, str(d.headline)) : null,
      arr(d.symbols).length ? h('p', { class: 'muted', style: 'margin:.3rem 0 0' }, `Symbols: ${arr(d.symbols).join(', ')}`) : null));
    // root cause analysis (+ runbook action)
    const rca = obj(d.rca);
    if (rca) {
      secs.push(rcaSection(rca));
      if (str(d.action)) secs.push(h('section', { class: 'd-sec' }, h('h3', null, 'Runbook'), h('p', { class: 'm0' }, str(d.action))));
    } else {
      secs.push(h('section', { class: 'd-sec' }, h('h3', null, 'Recommended action'), h('div', { class: 'd-action' }, str(d.action, 'No action recorded.'))));
    }
    // acknowledge
    const acked = d.acked_by != null;
    secs.push(h('section', { class: 'd-sec' }, h('h3', null, 'Acknowledge'),
      acked ? h('p', { class: 'ack-done m0' }, h('span', { class: 'ok-ic', 'aria-hidden': 'true' }, '✓'), ` Acknowledged by ${str(d.acked_by)}${isNum(d.acked_ms) ? ' at ' + fmtClock(d.acked_ms) : ''}`)
        : h('div', { class: 'fb-row' }, h('button', { type: 'button', class: 'btn btn-primary', 'data-key': 'ack', onclick: () => ackIncident(d.id) }, 'Acknowledge'),
          h('span', { class: 'muted' }, 'Tells the team you own this incident.'))));
    // explanation
    const llm = d.explanation_source === 'llm';
    const lines = str(d.explanation).split('\n').map((s) => s.trim()).filter(Boolean);
    secs.push(h('section', { class: 'd-sec d-expl' },
      h('h3', null, 'Why', h('span', { class: `src-badge${llm ? ' llm' : ''}`, title: llm ? 'Written by an LLM from the measured evidence only' : 'Deterministic template filled from the evidence' }, llm ? 'AI-written' : 'template')),
      lines.length ? lines.map((l) => h('p', null, l)) : h('p', { class: 'muted' }, 'No explanation available.')));
    // feedback
    const fb = d.feedback;
    secs.push(h('section', { class: 'd-sec' }, h('h3', null, 'Operator feedback'),
      h('div', { class: 'fb-row' },
        h('button', { type: 'button', class: 'btn', 'data-key': 'fb-confirm', 'aria-pressed': fb === 'confirmed' ? 'true' : 'false', onclick: () => sendFeedback(d.id, 'confirmed') }, '✓ Confirm'),
        h('button', { type: 'button', class: 'btn', 'data-key': 'fb-fp', 'aria-pressed': fb === 'false_positive' ? 'true' : 'false', onclick: () => sendFeedback(d.id, 'false_positive') }, '✗ False positive'),
        h('span', { class: 'muted' }, fb === 'confirmed' ? 'Marked as a real problem.' : fb === 'false_positive' ? 'Marked as a false alarm.' : 'Not reviewed yet.'))));
    // evidence
    secs.push(h('section', { class: 'd-sec' }, h('h3', null, 'Evidence'), kvTable(d.evidence)));
    // top ML features
    const feats = arr(d.top_features).filter(obj);
    if (feats.length) {
      const zMax = Math.max(3, ...feats.map((f) => (isNum(f.z) ? Math.abs(f.z) : 0)));
      secs.push(h('section', { class: 'd-sec' }, h('h3', null, 'Top AI features', h('span', { class: 'src-badge', title: 'Bar length = |z|, how far the value sits from its learned baseline' }, 'bar = |z|')),
        feats.map((f) => h('div', { class: 'feat' },
          h('span', { class: 'feat-name', title: str(f.name) }, str(f.name, '?')),
          h('div', { class: 'hbar', 'aria-hidden': 'true' }, h('i', { style: `width:${isNum(f.z) ? Math.min(100, (Math.abs(f.z) / zMax) * 100) : 0}%` })),
          h('span', { class: 'feat-nums' }, `${fmtNum(f.value)} vs ${fmtNum(f.baseline)} · z ${isNum(f.z) ? (f.z > 0 ? '+' : '') + fmtFix(f.z, 1) : '–'}`)))));
    } else {
      secs.push(h('section', { class: 'd-sec' }, h('h3', null, 'Top AI features'), h('p', { class: 'muted' }, 'No anomaly-model features for this incident.')));
    }
    // classifier
    const dg = obj(d.diagnosis);
    secs.push(h('section', { class: 'd-sec' }, h('h3', null, 'Classifier diagnosis'),
      dg && dg.label != null
        ? [h('div', { class: 'diag-main' }, h('b', null, str(dg.label)), h('div', { class: 'hbar', 'aria-hidden': 'true' }, h('i', { style: `width:${isNum(dg.p) ? dg.p * 100 : 0}%` })), h('span', { class: 'num' }, fmtPct(dg.p))),
          arr(dg.alternatives).length ? h('ul', { class: 'diag-alts' }, h('li', null, 'Alternatives: ',
            arr(dg.alternatives).filter(obj).map((a) => `${str(a.label)} ${fmtPct(a.p)}`).join(' · '))) : null]
        : h('p', { class: 'muted' }, 'No classifier output (model not loaded).')));
    // injected fault
    const fault = obj(d.fault);
    secs.push(h('section', { class: 'd-sec' }, h('h3', null, 'Matched injected fault'),
      fault ? h('p', { style: 'margin:0' }, h('b', null, str(fault.label) || str(fault.scenario)), ` (${str(fault.scenario)}) injected at ${fmtClock(fault.started_ms)}`)
        : h('p', { class: 'muted', style: 'margin:0' }, 'None: no injected fault explains this incident (organic detection).')));
    // timeline
    const tl = arr(d.timeline).filter(obj);
    secs.push(h('section', { class: 'd-sec' }, h('h3', null, 'Timeline'),
      tl.length ? h('ol', { class: 'timeline' }, tl.map((e) => h('li', null, h('span', { class: 't' }, str(e.clock) || fmtClock(e.t_ms)), h('span', null, str(e.event))))) : h('p', { class: 'muted' }, 'No events.')));
    // raw samples
    const samples = arr(d.samples);
    secs.push(h('section', { class: 'd-sec' }, h('h3', null, 'Raw sample messages'),
      samples.length ? h('pre', { class: 'samples', tabindex: 0 }, samples.map((s) => JSON.stringify(s)).join('\n')) : h('p', { class: 'muted' }, 'No samples captured.')));

    body.textContent = '';
    body.append(...secs);
    body.scrollTop = scroll;
    if (focusKey) { const el = body.querySelector(`[data-key="${focusKey}"]`); if (el) el.focus(); }
  }
  function rcaSection(r) {
    const pc = (c) => (obj(c) ? `${str(c.cause, '–')}${isNum(c.confidence) ? ` (${Math.round(c.confidence)}%)` : ''}` : '–');
    const conf = obj(r.primary) && isNum(r.primary.confidence) ? Math.max(0, Math.min(100, r.primary.confidence)) : 0;
    const ev = arr(r.evidence).map(str).filter(Boolean);
    return h('section', { class: 'd-sec rca' },
      h('h3', null, 'Root cause analysis'),
      h('div', { class: 'rca-line rca-primary' }, h('span', { class: 'k' }, 'Primary'), h('b', null, pc(r.primary)),
        h('span', { class: 'hbar', 'aria-hidden': 'true' }, h('i', { style: `width:${conf}%` }))),
      h('div', { class: 'rca-line' }, h('span', { class: 'k' }, 'Secondary'), h('span', null, pc(r.secondary))),
      ev.length ? h('div', { class: 'rca-block' }, h('span', { class: 'k' }, 'Evidence'), h('ul', { class: 'rca-ev' }, ev.map((e) => h('li', null, e)))) : null,
      str(r.scope) ? h('div', { class: 'rca-line' }, h('span', { class: 'k' }, 'Scope'), h('span', null, str(r.scope))) : null,
      arr(r.affected).length ? h('div', { class: 'rca-line' }, h('span', { class: 'k' }, 'Likely affected'), h('span', null, arr(r.affected).map(str).join(', '))) : null,
      h('div', { class: 'rca-block' }, h('span', { class: 'k' }, 'Recommended action'), h('div', { class: 'd-action' }, str(r.action) || 'No action recorded.')));
  }
  async function ackIncident(id) {
    if (id == null) return;
    const res = await send('POST', `/api/incidents/${encodeURIComponent(id)}/ack`, { note: '' });
    if (res) { toast(`${id} acknowledged`); S.drawerSig = ''; refreshDrawer(); }
  }
  function fmtNum(v) {
    if (!isNum(v)) return '–';
    const a = Math.abs(v);
    return a >= 1000 ? fmtInt(v) : a >= 10 ? fmtFix(v, 1) : fmtFix(v, 2);
  }
  async function sendFeedback(id, label) {
    if (id == null) return;
    const res = await send('POST', `/api/incidents/${encodeURIComponent(id)}/feedback`, { label, note: '' });
    if (res) {
      toast(label === 'confirmed' ? `${id} confirmed` : `${id} marked as false positive`);
      S.drawerSig = '';
      refreshDrawer();
    }
  }

  // ================================================================ Chaos Lab
  const GROUPS = [
    ['fault', 'Faults'],
    ['incident', 'Replay real incidents'],
    ['novel', 'Novel faults: not in the AI’s training set'],
    ['market', 'Real market events: should NOT alert'],
  ];
  function buildChaos() {
    const hl = S.hello || {};
    const fsel = $('#chaos-feed');
    const prev = fsel.value;
    fsel.textContent = '';
    for (const f of S.feeds) fsel.append(h('option', { value: f.id }, `${f.id} · ${f.name}`));
    const ids = S.feeds.map((f) => f.id);
    fsel.value = ids.includes(prev) ? prev : ids.includes('C') ? 'C' : ids[ids.length - 1] || '';

    const ssel = $('#chaos-symbols');
    const prevS = ssel.value;
    ssel.textContent = '';
    ssel.append(h('option', { value: '' }, 'All symbols'));
    for (const s of arr(hl.symbols)) ssel.append(h('option', { value: s }, s));
    ssel.value = arr(hl.symbols).map(String).includes(prevS) ? prevS : '';

    const host = $('#scenario-groups');
    host.textContent = '';
    const scen = arr(hl.scenarios).filter((s) => obj(s) && s.id != null);
    const groups = GROUPS.map(([g]) => g);
    for (const s of scen) if (!groups.includes(str(s.group, 'fault'))) groups.push(str(s.group, 'fault'));
    for (const g of groups) {
      const list = scen.filter((s) => str(s.group, 'fault') === g);
      if (!list.length) continue;
      const title = (GROUPS.find(([k]) => k === g) || [g, g])[1];
      host.append(h('div', { class: `scn-group g-${g}` },
        h('h3', null, title),
        h('div', { class: 'scn-btns' }, list.map((s) => {
          const tip = `${str(s.description)}${s.accepts_symbols ? '' : ' (applies to all symbols)'}`.trim();
          const b = h('button', { type: 'button', class: `btn btn-sm scn g-${g}`, title: tip, 'aria-label': `Inject ${str(s.label, s.id)}. ${tip}` }, str(s.label, s.id));
          b.addEventListener('click', () => inject(s, b));
          return b;
        }))));
    }
    if (!scen.length) host.append(h('p', { class: 'muted' }, 'No scenarios available from the server.'));
  }
  async function inject(s, btn) {
    const feed = $('#chaos-feed').value;
    if (!feed) { toast('Pick a target feed first', 'error'); return; }
    const sym = $('#chaos-symbols').value;
    const symbols = s.accepts_symbols && sym ? [sym] : null;
    btn.disabled = true;
    btn.classList.add('busy');
    const res = await send('POST', '/api/chaos', { scenario: s.id, feed, symbols, params: {}, duration_s: null });
    btn.disabled = false;
    btn.classList.remove('busy');
    if (res) toast(`Injected “${str(s.label, s.id)}” on ${feed}${symbols ? ' · ' + symbols.join(', ') : ''}`);
  }
  function renderActiveFaults(active) {
    const host = $('#active-faults');
    const list = active.filter((f) => obj(f) && f.id != null);
    const sig = list.map((f) => `${f.id}|${f.label}|${f.feed}|${arr(f.symbols).join(',')}`).join(';');
    if (sig !== S.chipsSig) {
      S.chipsSig = sig;
      host.textContent = '';
      S.chipRefs.clear();
      if (!list.length) host.append(h('span', { class: 'muted' }, 'none'));
      for (const f of list) {
        const age = h('span', { class: 'num muted' }, '');
        const label = str(f.label) || str(f.scenario);
        const syms = arr(f.symbols).length ? arr(f.symbols).join(', ') : 'all';
        host.append(h('span', { class: 'chip', title: `${label} on ${str(f.feed)} (${syms}), id ${f.id}` },
          h('span', null, label), feedBadge(str(f.feed)), h('span', { class: 'muted' }, syms), age,
          h('button', { type: 'button', class: 'chip-x', 'aria-label': `Clear ${label} on ${str(f.feed)}`, title: 'Clear this fault', onclick: () => clearFault(f.id) }, '×')));
        S.chipRefs.set(String(f.id), age);
      }
    }
    for (const f of list) { const el = S.chipRefs.get(String(f.id)); if (el) setText(el, fmtSec(f.age_s)); }
    $('#chaos-clear').disabled = !list.length;
  }
  async function clearFault(id) {
    const res = await send('DELETE', `/api/chaos/${encodeURIComponent(id)}`);
    if (res) toast(`Cleared fault ${id}`);
  }
  function renderStopwatch(sw) {
    const box = $('#stopwatch');
    let state, value, sub, inc = null;
    if (!sw) {
      state = 'idle'; value = ['—']; sub = 'Inject a fault to time its detection';
    } else {
      const label = str(sw.label) || str(sw.scenario, 'fault');
      const on = sw.feed != null ? ` on ${sw.feed}` : '';
      if (sw.group === 'market') {
        const n = isNum(sw.false_alerts) ? sw.false_alerts : 0;
        state = n === 0 ? 'market-ok' : 'market-bad';
        value = n === 0 ? [h('span', { class: 'ok' }, '✓'), ' false alerts: 0'] : [h('span', { class: 'bad' }, '✖︎'), ` false alerts: ${n}`];
        sub = `No alert expected · ${label}${on} · ${fmtSec(sw.elapsed_s)}`;
      } else if (sw.detected) {
        state = 'detected';
        value = [`TTD ${fmtSec(sw.ttd_s)} `, h('span', { class: 'ok' }, '✓')];
        sub = `${label}${on} · detected`;
        inc = sw.incident_id != null ? String(sw.incident_id) : null;
      } else {
        state = 'running'; value = [fmtSec(sw.elapsed_s)]; sub = `Detecting… ${label}${on}`;
      }
    }
    const sig = `${state}|${value.map((v) => (v instanceof Node ? v.textContent : v)).join('')}|${sub}|${inc}`;
    if (sig === S.swSig) return;
    S.swSig = sig;
    box.dataset.state = state;
    const vEl = $('#sw-value');
    vEl.textContent = '';
    vEl.append(...value);
    setText($('#sw-sub'), sub);
    const b = $('#sw-inc');
    b.hidden = !inc;
    if (inc) { setText(b, `${inc} · Why? ›`); b.onclick = () => openDrawer(inc); }
  }

  // ================================================================ footer: detector self-health
  function renderSelf(s) {
    if (!s) return;
    const lag = s.lag_ms;
    const st = $('#self-status');
    const bad = isNum(lag) && lag > 500, slow = isNum(lag) && lag > 100;
    setText(st, !isNum(lag) ? '' : bad ? '⚠︎ lagging' : slow ? '⚠︎ slow' : '● OK');
    st.className = `self-status ${bad || slow ? 'warn' : 'ok'}`;
    setText($('#self-stats'), `${fmtInt(s.events_per_s)} events/s · lag ${fmtMs(lag)} ms · ${fmtInt(s.processed)} processed · up ${fmtDuration(s.uptime_s)}`);
  }

  // ================================================================ Evaluation tab
  async function loadMetrics() {
    if (S.metricsBusy) return;
    S.metricsBusy = true;
    const body = $('#eval-body');
    body.style.opacity = '0.5';
    const btn = $('#eval-refresh');
    btn.disabled = true;
    const m = await send('GET', '/api/metrics');
    S.metricsBusy = false;
    btn.disabled = false;
    body.style.opacity = '';
    if (m === null) {
      if (S.metrics === undefined) { body.textContent = ''; body.append(h('p', { class: 'muted' }, 'Could not load /api/metrics. Try Refresh.')); }
      return;
    }
    S.metrics = m;
    safe(renderMetrics, obj(m) || {});
  }

  const notRun = (what) => h('p', { class: 'not-run' }, `${what}: not run yet.`);
  function card(title, sub, content) {
    return h('section', { class: 'panel eval-card' }, h('div', { class: 'panel-head' }, h('h2', null, title), sub ? h('span', { class: 'panel-sub' }, sub) : null), h('div', { class: 'inner' }, content));
  }
  function tile(label, value, sub, hero) {
    return h('div', { class: `tile${hero ? ' hero' : ''}` }, h('div', { class: 'label' }, label), h('div', { class: 'value' }, value), sub ? h('div', { class: 'sub' }, sub) : null);
  }
  function rateCell(v) {
    return h('td', { class: 'n' }, h('span', { class: 'cell-bar' }, fmtPct(v), h('span', { class: 'hbar', 'aria-hidden': 'true' }, h('i', { style: `width:${isNum(v) ? Math.max(0, Math.min(1, v)) * 100 : 0}%` }))));
  }
  // sequential ramp (dark mode: near-zero recedes into the surface, high values get brighter)
  // light mode: near-zero recedes to the card, high values get darker
  const SEQ = ['#F5F5FA', '#cde2fb', '#b7d3f6', '#9ec5f4', '#86b6ef', '#6da7ec', '#5598e7', '#3987e5', '#2a78d6', '#256abf', '#1c5cab'];
  function seqCell(p) {
    if (!isNum(p) || p <= 0) return { bg: SEQ[0], fg: COL.text3 };
    // step 8 (#2a78d6) is skipped: neither white nor ink reaches 4.5:1 on it
    const i = [1, 2, 3, 4, 5, 6, 7, 9, 10][Math.min(8, Math.floor(p * 9))];
    return { bg: SEQ[i], fg: i >= 9 ? '#ffffff' : '#172038' };
  }

  function renderMetrics(m) {
    const body = $('#eval-body');
    body.textContent = '';
    const meta = [];
    if (m.generated_at) meta.push(`generated ${str(m.generated_at)}`);
    const data = obj(m.data);
    if (data && data.source) meta.push(str(data.source));
    setText($('#eval-meta'), meta.join(' · '));
    if (!Object.keys(m).length) {
      body.append(h('section', { class: 'panel', style: 'padding:1.2rem' },
        h('h2', null, 'No evaluation report yet'),
        h('p', { class: 'muted' }, 'Run ', h('code', null, 'python -m feedsentinel evaluate'), ' to write reports/metrics.json, then press Refresh.')));
      return;
    }
    const clean = obj(m.clean), inc = obj(m.incidents), cls = obj(m.classifier), an = obj(m.anomaly), thr = obj(m.throughput);
    // headline tiles
    const byFeed = clean && obj(clean.by_feed) ? Object.entries(clean.by_feed).map(([k, v]) => `${k}: ${fmtInt(v)}`).join(' · ') : '';
    body.append(h('div', { class: 'tiles' },
      clean ? tile('Clean-data false alarms per hour', fmtFix(clean.false_alarms_per_hour, 2),
        `${fmtInt(clean.false_alarms)} false alarms in ${fmtFix(clean.market_hours, 1)} market hours of clean data${byFeed ? ' (' + byFeed + ')' : ''}`, true)
        : tile('Clean-data false alarms per hour', '—', 'not run yet', true),
      clean ? tile('Real market events that alerted', `${fmtInt(clean.market_event_alerts)} / ${fmtInt(clean.market_events)}`, 'halts, fast moves, bursts: should be 0') : tile('Real market events that alerted', '—', 'not run yet'),
      inc ? tile('Incident precision', fmtPct(inc.precision, 1), `${fmtInt(inc.matched)} of ${fmtInt(inc.total)} incidents matched an injected fault`) : tile('Incident precision', '—', 'not run yet'),
      inc ? tile('Collateral alerts on healthy feeds', fmtInt(inc.collateral_on_healthy_feeds), 'alerts on feeds with no fault injected') : tile('Collateral alerts on healthy feeds', '—', 'not run yet'),
      cls ? tile('Classifier macro-F1', fmtFix(cls.macro_f1, 2), `accuracy ${fmtPct(cls.accuracy, 1)}`) : tile('Classifier macro-F1', '—', 'not run yet'),
      thr ? tile('Throughput', fmtCompact(thr.events_per_s), 'events per second, single process') : tile('Throughput', '—', 'not run yet')));

    // per-fault table
    const faults = arr(m.faults).filter(obj);
    body.append(card('Detection by fault', 'episodes injected into held-out test data; TTD in market seconds',
      faults.length ? h('table', { class: 'table' },
        h('thead', null, h('tr', null, ['Fault', 'Group', 'Episodes', 'Detected', 'Detection rate', 'Correct code', 'TTD p50', 'TTD p95'].map((c, i) => h('th', { scope: 'col', class: i >= 2 ? 'n' : null }, c)))),
        h('tbody', null, faults.map((f) => h('tr', null,
          h('th', { scope: 'row' }, str(f.label) || str(f.fault)),
          h('td', { class: 'muted' }, str(f.group)),
          h('td', { class: 'n' }, fmtInt(f.episodes)),
          h('td', { class: 'n' }, fmtInt(f.detected)),
          rateCell(f.detection_rate),
          h('td', { class: 'n' }, fmtPct(f.correct_code_rate)),
          h('td', { class: 'n' }, fmtSec(f.ttd_p50_s)),
          h('td', { class: 'n' }, fmtSec(f.ttd_p95_s))))))
        : notRun('Fault-injection evaluation')));

    // ablation + anomaly
    const ab = obj(m.ablation);
    let abContent = notRun('Ablation');
    if (ab && arr(ab.configs).length) {
      const cfgs = arr(ab.configs).map(String);
      abContent = h('table', { class: 'table' },
        h('thead', null, h('tr', null, h('th', { scope: 'col' }, 'Fault'), cfgs.map((c) => h('th', { scope: 'col', class: 'n' }, c)))),
        h('tbody', null,
          arr(ab.rows).filter(obj).map((r) => h('tr', null, h('th', { scope: 'row' }, str(r.label) || str(r.fault)), cfgs.map((_, i) => rateCell(arr(r.rates)[i])))),
          arr(ab.overall).length ? h('tr', { class: 'total' }, h('th', { scope: 'row' }, 'Overall detection'), cfgs.map((_, i) => rateCell(arr(ab.overall)[i]))) : null,
          arr(ab.false_alarms_per_hour).length ? h('tr', null, h('th', { scope: 'row' }, 'False alarms / hour'), cfgs.map((_, i) => h('td', { class: 'n' }, fmtFix(arr(ab.false_alarms_per_hour)[i], 2)))) : null));
    }
    const anContent = an ? kvTable({
      'alert threshold': isNum(an.threshold) ? +an.threshold.toFixed(4) : an.threshold,
      'threshold percentile (clean data)': isNum(an.percentile) ? `${an.percentile}th` : an.percentile,
      'clean training windows': an.clean_windows,
      'flag rate on clean data': isNum(an.flag_rate_clean) ? fmtPct(an.flag_rate_clean, 2) : an.flag_rate_clean,
    }) : notRun('Anomaly model calibration');
    const dataContent = data ? kvTable({ source: data.source, train: data.train, validation: data.validation, test: data.test }) : notRun('Data split');
    body.append(h('div', { class: 'eval-grid' },
      card('Ablation: what each layer adds', 'detection rate per configuration', abContent),
      h('div', { style: 'display:flex;flex-direction:column;gap:.9rem' },
        card('Anomaly model (Isolation Forest)', null, anContent),
        card('Data', 'time-split, no leakage between train and test', dataContent))));

    // classifier
    let perContent = notRun('Classifier evaluation');
    let confContent = notRun('Confusion matrix');
    if (cls) {
      const per = arr(cls.per_class).filter(obj);
      if (per.length) {
        perContent = h('table', { class: 'table' },
          h('thead', null, h('tr', null, ['Class', 'Precision', 'Recall', 'F1', 'Support'].map((c, i) => h('th', { scope: 'col', class: i ? 'n' : null }, c)))),
          h('tbody', null, per.map((p) => h('tr', null, h('th', { scope: 'row' }, str(p.label)),
            h('td', { class: 'n' }, fmtFix(p.precision, 2)), h('td', { class: 'n' }, fmtFix(p.recall, 2)), h('td', { class: 'n' }, fmtFix(p.f1, 2)), h('td', { class: 'n' }, fmtInt(p.support))))),
          h('tfoot', null, h('tr', { class: 'total' }, h('th', { scope: 'row' }, 'Macro-F1 / accuracy'), h('td', { class: 'n' }), h('td', { class: 'n' }), h('td', { class: 'n' }, fmtFix(cls.macro_f1, 2)), h('td', { class: 'n' }, fmtPct(cls.accuracy, 1)))));
      }
      const labels = arr(cls.labels).map(String);
      const conf = arr(cls.confusion);
      if (labels.length && conf.length) {
        confContent = h('div', null,
          h('div', { class: 'axis-cap' }, 'rows: true class · columns: predicted class · shade: share of the true class'),
          h('table', { class: 'confusion' },
            h('thead', null, h('tr', null, h('th', null, ''), labels.map((l) => h('th', { scope: 'col' }, l)))),
            h('tbody', null, labels.map((rl, i) => {
              const row = arr(conf[i]);
              const tot = row.reduce((a, v) => a + (isNum(v) ? v : 0), 0);
              return h('tr', null, h('th', { scope: 'row' }, rl), labels.map((cl, j) => {
                const v = row[j];
                const p = tot > 0 && isNum(v) ? v / tot : 0;
                const c = seqCell(p);
                return h('td', { class: i === j ? 'diag' : null, style: `background:${c.bg};color:${c.fg}`, title: `True ${rl} → predicted ${cl}: ${fmtInt(v)} (${fmtPct(p, 1)} of ${rl})` }, fmtInt(v));
              }));
            }))),
          h('div', { class: 'conf-legend' }, '0%', h('span', { class: 'ramp' }), '100% of the true class · outlined = correct'));
      }
    }
    body.append(h('div', { class: 'eval-grid' }, card('Fault classifier: per class', null, perContent), card('Confusion matrix', null, confContent)));
  }

  // ================================================================ tabs
  function showTab(name) {
    S.tab = name;
    for (const b of document.querySelectorAll('.tab')) {
      const on = b.dataset.tab === name;
      b.setAttribute('aria-selected', on ? 'true' : 'false');
      b.tabIndex = on ? 0 : -1;
    }
    $('#tab-monitor').hidden = name !== 'monitor';
    $('#tab-eval').hidden = name !== 'eval';
    $('#tab-about').hidden = name !== 'about';
    $('#tab-audit').hidden = name !== 'audit';
    if (name === 'eval' && S.metrics === undefined) loadMetrics();
    if (name === 'audit') loadAudit();
    if (name === 'monitor' && S.chart) { S.chart.resize(); safe(renderChart, S.lastChart); }
  }


  // ================================================================ ops: band stats + incident timeline
  function renderBandStats(t) {
    const feeds = arr(t.feeds).filter(obj);
    setText($('#st-healthy'), feeds.length ? `${feeds.filter((f) => f.state === 'HEALTHY').length}/${feeds.length}` : '–');
    setText($('#st-open'), fmtInt(arr(t.incidents).filter((i) => obj(i) && i.status === 'OPEN').length));
    setText($('#st-eps'), fmtCompact(obj(t.self) ? t.self.events_per_s : null));
  }
  const TL = { ok: ['●', 'ok'], warn: ['⚠\uFE0E', 'warn'], minor: ['▲', 'warn'], major: ['✖\uFE0E', 'bad'], incident: ['◆', 'bad'], action: ['↻', 'info'], info: ['i', 'info'] };
  function renderTimeline(list) {
    const items = list.filter(obj);
    const sig = items.map((e) => `${e.id}:${e.count}:${e.text}:${e.clock}`).join('|');
    if (sig === S.tlSig) return;
    S.tlSig = sig;
    const host = $('#tl-list');
    host.textContent = '';
    for (const e of items.slice().reverse()) { // newest first
      const [icon, tone] = TL[e.level] || ['•', 'info'];
      const inc = e.incident_id != null ? String(e.incident_id) : null;
      const text = str(e.text);
      const n = isNum(e.count) && e.count > 1 ? e.count : 0;
      const kids = [
        h('span', { class: 'tl-icon', 'aria-hidden': 'true' }, icon), h('span', { class: 'sr-only' }, `${str(e.level)}: `),
        h('span', { class: 'tl-time num' }, str(e.clock) || fmtClock(e.t_ms)),
        e.feed != null ? feedBadge(str(e.feed)) : h('span', { class: 'tl-nofeed', 'aria-hidden': 'true' }),
        h('span', { class: 'tl-text' }, text, n && !text.includes(`x${n}`) ? h('span', { class: 'tl-count num' }, `x${n}`) : null),
        inc ? h('span', { class: 'tl-link' }, `${inc} ›`) : null,
      ];
      host.append(inc
        ? h('li', null, h('button', { type: 'button', class: `tl-row tone-${tone}`, title: `Open ${inc}`, onclick: () => openDrawer(inc) }, kids))
        : h('li', null, h('div', { class: `tl-row tone-${tone}` }, kids)));
    }
    $('#tl-empty').hidden = items.length > 0;
    setText($('#tl-count'), items.length ? `${items.length} events` : '');
  }

  // ================================================================ admin: audit log
  async function loadAudit() {
    const rows = await send('GET', '/api/audit?limit=100');
    const body = $('#audit-body');
    if (!rows) return;
    body.textContent = '';
    const list = arr(rows).filter(obj);
    if (!list.length) { body.append(h('p', { class: 'muted' }, 'No audit entries yet.')); return; }
    const when = (t) => (isNum(t) ? new Date(t).toISOString().replace('T', ' ').slice(0, 19) + ' UTC' : '–');
    body.append(h('div', { class: 'panel table-wrap' }, h('table', { class: 'table' },
      h('thead', null, h('tr', null, ['#', 'Time', 'Actor', 'Action', 'Target', 'Outcome', 'Detail', 'Hash'].map((c) => h('th', { scope: 'col' }, c)))),
      h('tbody', null, list.map((e) => h('tr', null,
        h('td', { class: 'n' }, fmtInt(e.seq)), h('td', { class: 'num nowrap' }, when(e.t_ms)), h('td', null, str(e.actor)),
        h('td', null, h('code', null, str(e.action))), h('td', null, str(e.target)),
        h('td', null, h('span', { class: `outcome ${e.outcome === 'ok' ? 'ok' : 'bad'}` }, e.outcome === 'ok' ? '✓ ok' : `✖\uFE0E ${str(e.outcome)}`)),
        h('td', { class: 'muted' }, typeof e.detail === 'object' && e.detail ? JSON.stringify(e.detail) : str(e.detail)),
        h('td', { class: 'hash', title: str(e.hash) }, str(e.hash).slice(0, 12) + (str(e.hash).length > 12 ? '…' : ''))))))));
  }
  async function verifyAudit() {
    const out = $('#audit-result');
    setText(out, 'Verifying…');
    out.className = 'verify-result';
    const v = await send('GET', '/api/audit/verify');
    if (!v) { setText(out, ''); return; }
    const ok = v.ok === true;
    out.className = `verify-result ${ok ? 'ok' : 'bad'}`;
    setText(out, ok ? `✓ Hash chain intact (${fmtInt(v.entries)} entries)` : `✖\uFE0E Hash chain broken${v.bad_seq != null ? ' at entry #' + v.bad_seq : ''}`);
  }

  // ================================================================ trader view
  const TRUST = { VERIFIED: ['ok', '✓'], 'USE CAUTION': ['warn', '⚠\uFE0E'], 'DO NOT USE': ['bad', '✖\uFE0E'] };
  const ALERT = { critical: ['bad', '✖\uFE0E'], warn: ['warn', '⚠\uFE0E'], ok: ['ok', '✓'] };
  function applyTraderTick(t) {
    if (t.tz && t.tz !== S.tz) { S.tz = String(t.tz); S.clockFmt = makeClockFmt(S.tz); S.tzName = ''; }
    if (!S.tzName && isNum(t.t_ms)) S.tzName = tzShort(t.t_ms);
    setText($('#tr-clock'), t.clock || fmtClock(t.t_ms));
    setText($('#tr-tz'), S.tzName || S.tz);
    setText($('#tr-session'), str(t.session, '—'));
    setText($('#tr-date'), fmtDate(t.date));
    const wl = arr(t.watchlist).filter((w) => obj(w) && w.symbol != null);
    const ages = wl.map((w) => w.updated_s).filter(isNum);
    setText($('#tr-fresh'), ages.length ? fmtSec(Math.max(...ages)) : '–');
    setText($('#tr-verified'), wl.length ? `${wl.filter((w) => w.trust === 'VERIFIED').length}/${wl.length}` : '–');
    const notice = str(t.notice);
    $('#tr-notice').hidden = !notice;
    setText($('#tr-notice-text'), notice);
    // watchlist cards, keyed by symbol
    const host = $('#watchlist');
    const keep = new Set();
    for (const w of wl) {
      const sym = String(w.symbol);
      keep.add(sym);
      let R = S.trCards.get(sym);
      if (!R) {
        R = {};
        const r = (n, e) => (R[n] = e);
        R.el = h('article', { class: 'card wl-card' },
          h('div', { class: 'wl-top' }, h('div', null, h('div', { class: 'wl-sym' }, sym), r('name', h('div', { class: 'wl-name' }, ''))), r('badge', h('span', { class: 'trust-badge' }, ''))),
          r('price', h('div', { class: 'wl-price num' }, '–')),
          r('reason', h('div', { class: 'wl-reason' }, '')),
          h('div', { class: 'wl-foot' }, r('src', h('span', null, '')), r('upd', h('span', { class: 'num' }, ''))));
        S.trCards.set(sym, R);
        host.append(R.el);
      }
      const [tone, icon] = TRUST[w.trust] || ['warn', '?'];
      const cls = `card wl-card trust-${tone}`;
      if (R.el.className !== cls) R.el.className = cls;
      setText(R.name, str(w.name));
      setText(R.badge, `${icon} ${str(w.trust, 'UNKNOWN')}`);
      setText(R.price, fmtPrice(w.price));
      setText(R.reason, str(w.trust_reason));
      setText(R.src, w.source_name || w.source ? `via ${str(w.source_name) || str(w.source)} feed` : '');
      setText(R.upd, isNum(w.updated_s) ? `updated ${fmtFix(w.updated_s, 1)} s ago` : '');
    }
    for (const [sym, R] of S.trCards) if (!keep.has(sym)) { R.el.remove(); S.trCards.delete(sym); }
    // alerts (plain language)
    const alerts = arr(t.alerts).filter(obj);
    const sig = alerts.map((a) => `${a.level}|${a.clock}|${a.text}`).join('\n');
    if (sig !== S.trAlertSig) {
      S.trAlertSig = sig;
      const list = $('#tr-alerts');
      list.textContent = '';
      for (const a of alerts) {
        const [tone, icon] = ALERT[a.level] || ['warn', '•'];
        list.append(h('li', { class: `alert tone-${tone}` }, h('span', { class: 'al-icon', 'aria-hidden': 'true' }, icon),
          h('span', { class: 'sr-only' }, `${str(a.level)}: `), h('span', { class: 'al-time num' }, str(a.clock) || fmtClock(a.t_ms)), h('span', { class: 'al-text' }, str(a.text))));
      }
      $('#tr-alerts-empty').hidden = alerts.length > 0;
    }
  }

  // ================================================================ chat
  const SUGGEST = {
    ops: ['Why is Feed C red?', 'Which feed should I trust for AAPL?', 'What happened in the last 10 minutes?'],
    trader: ['Can I trust MSFT prices?', 'What are my regions?'],
  };
  function setChat(open) {
    $('#chat').hidden = !open;
    setAttr($('#chat-toggle'), 'aria-expanded', open ? 'true' : 'false');
    document.body.classList.toggle('chat-open', open);
    if (open) $('#chat-input').focus();
  }
  function addMsg(kind, text, sources) {
    const el = h('div', { class: `msg msg-${kind}` },
      kind === 'refused' ? h('div', { class: 'msg-tag' }, 'Outside your scope') : null,
      h('div', { class: 'msg-text' }, text),
      sources && sources.length ? h('div', { class: 'msg-sources' }, sources.map((x) => h('span', { class: 'src-chip' }, str(x)))) : null);
    const box = $('#chat-msgs');
    box.append(el);
    box.scrollTop = box.scrollHeight;
    return el;
  }
  async function ask(text) {
    const q = str(text).trim();
    if (!q || S.chatBusy) return;
    S.chatBusy = true;
    $('#chat-input').value = '';
    addMsg('user', q);
    const pending = addMsg('pending', 'Thinking…');
    const r = await send('POST', '/api/chat', { message: q }, { quiet: true });
    pending.remove();
    S.chatBusy = false;
    if (!r) { addMsg('error', 'Sorry, FeedSentinel could not answer right now. Please try again in a moment.'); return; }
    addMsg(r.refused === true ? 'refused' : 'bot', str(r.answer) || '(no answer)', arr(r.sources));
  }
  function initChat() {
    const sug = $('#chat-suggest');
    sug.textContent = '';
    for (const q of SUGGEST[S.page] || []) sug.append(h('button', { type: 'button', class: 'chip-btn', onclick: () => ask(q) }, q));
    $('#chat-toggle').hidden = false;
    $('#chat-toggle').addEventListener('click', () => setChat($('#chat').hidden));
    $('#chat-close').addEventListener('click', () => { setChat(false); $('#chat-toggle').focus(); });
    $('#chat-form').addEventListener('submit', (e) => { e.preventDefault(); ask($('#chat-input').value); });
  }

  // ================================================================ nav + login + router
  const ROLE_LABEL = { ops_analyst: 'Ops analyst', admin: 'Admin', trader: 'Trader' };
  function renderUser() {
    const u = AUTH.user;
    $('#user-chip').hidden = !u;
    if (!u) return;
    setText($('#user-name'), str(u.display_name) || str(u.username));
    setText($('#user-meta'), `${ROLE_LABEL[u.role] || str(u.role)} · ${arr(u.regions).join(', ') || 'no region'}`);
    $('#tabbtn-audit').hidden = u.role !== 'admin';
  }
  function showLoginErr(msg, info) {
    const e = $('#login-err');
    e.hidden = !msg;
    e.classList.toggle('info', !!info);
    setText(e, msg);
  }
  function initLogin() {
    const msg = store.get('fs_msg');
    if (msg) { store.set('fs_msg', null); showLoginErr(msg, true); }
    if (AUTH.token) { // already signed in: go straight home if the token still works
      fetch('/api/auth/me', { credentials: 'include', headers: { Accept: 'application/json', Authorization: `Bearer ${AUTH.token}` } })
        .then((r) => (r.ok ? r.json() : null)).then((u) => { if (obj(u)) { AUTH.user = u; location.replace(homeOf(u)); } }).catch(() => {});
    }
    for (const b of document.querySelectorAll('.demo-chip')) {
      b.addEventListener('click', () => { $('#login-user').value = b.dataset.user; $('#login-pass').value = 'demo123'; $('#login-submit').focus(); });
    }
    $('#login-form').addEventListener('submit', async (e) => {
      e.preventDefault();
      const btn = $('#login-submit');
      btn.disabled = true;
      setText(btn, 'Signing in…');
      showLoginErr('');
      try {
        const r = await fetch('/api/auth/login', { method: 'POST', credentials: 'include', headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
          body: JSON.stringify({ username: $('#login-user').value.trim(), password: $('#login-pass').value }) });
        let d = null;
        try { d = await r.json(); } catch (e2) { d = null; }
        if (r.ok && d && d.access_token) {
          AUTH.token = d.access_token;
          AUTH.user = obj(d.user);
          location.assign(homeOf(obj(d.user)));
          return;
        }
        showLoginErr(r.status === 429 ? 'Too many attempts. Please wait a moment and try again.'
          : d && typeof d.detail === 'string' ? d.detail : `Sign-in failed (HTTP ${r.status}).`);
      } catch (err) {
        showLoginErr('Cannot reach the FeedSentinel server.');
      }
      btn.disabled = false;
      setText(btn, 'Sign in');
    });
    $('#login-user').focus();
  }
  async function signOut() {
    await send('POST', '/api/auth/logout', {}, { quiet: true });
    AUTH.token = null;
    AUTH.user = null;
    location.assign('/login');
  }
  function boot() {
    const path = location.pathname.replace(/\/+$/, '') || '/';
    S.page = path === '/ops' ? 'ops' : path === '/trader' ? 'trader' : 'login';
    if (S.page === 'login' && path !== '/login') history.replaceState(null, '', '/login');
    for (const pg of ['login', 'ops', 'trader']) $(`#page-${pg}`).hidden = pg !== S.page;
    document.body.dataset.page = S.page;
    setText($('#view-name'), { login: '', ops: 'Operations', trader: 'Markets' }[S.page]);
    if (S.page === 'login') { $('#conn-pill').hidden = true; initLogin(); return; }
    if (!AUTH.token) { goLogin(); return; }
    const u = AUTH.user;
    if (u && u.role === 'trader' && S.page === 'ops') { location.replace('/trader'); return; }
    document.title = `FeedSentinel · ${S.page === 'ops' ? 'Operations' : 'Markets'}`;
    renderUser();
    $('#signout').hidden = false;
    $('#signout').addEventListener('click', signOut);
    initChat();
    document.addEventListener('keydown', (e) => { if (e.key === 'Escape' && !S.drawerId && !$('#chat').hidden) setChat(false); });
    if (S.page === 'ops') {
      wire();
      safe(initChart);
      buildChartDatasets();
    }
    connect();
    setInterval(updateConn, 1000);
    updateConn();
  }

  // ================================================================ wiring
  function wire() {
    $('#speed-select').addEventListener('change', (e) => { const v = Number(e.target.value); if (isNum(v)) control({ speed: v }); });
    $('#pause-btn').addEventListener('click', async () => {
      const next = !S.paused;
      const res = await control({ paused: next });
      if (res) { S.paused = next; renderModeControls(S.mode, Number($('#speed-select').value)); }
    });
    $('#demo-btn').addEventListener('click', async () => {
      const next = !S.demoOn;
      const res = await send('POST', '/api/demo', { action: next ? 'start' : 'stop' });
      if (res) { S.demoOn = next; renderDemoBtn(); toast(next ? 'Demo autopilot started' : 'Demo autopilot stopped'); }
    });
    for (const b of document.querySelectorAll('.seg-btn')) {
      b.addEventListener('click', async () => {
        const mode = b.dataset.mode;
        if (mode === S.mode) return;
        const res = await control({ mode });
        if (res) toast(`Switching to ${mode} mode…`);
      });
    }
    $('#symbol-select').addEventListener('change', (e) => { if (e.target.value) control({ symbol: e.target.value }); });
    $('#chart-table-toggle').addEventListener('click', (e) => {
      S.tableView = !S.tableView;
      e.currentTarget.setAttribute('aria-pressed', S.tableView ? 'true' : 'false');
      $('#chart-wrap').hidden = S.tableView;
      $('#chart-table').hidden = !S.tableView;
      safe(renderChart, S.lastChart);
    });
    $('#chaos-clear').addEventListener('click', async () => {
      const res = await send('POST', '/api/chaos/clear');
      if (res) toast(`Cleared ${isNum(res.cleared) ? res.cleared : 'all'} fault${res.cleared === 1 ? '' : 's'}`);
    });
    $('#eval-refresh').addEventListener('click', loadMetrics);
    const allTabs = [...document.querySelectorAll('.tab')];
    for (const b of allTabs) {
      b.addEventListener('click', () => showTab(b.dataset.tab));
      b.addEventListener('keydown', (e) => {
        if (e.key !== 'ArrowRight' && e.key !== 'ArrowLeft') return;
        const tabs = allTabs.filter((x) => !x.hidden);
        const i = tabs.indexOf(b);
        const nb = tabs[(i + (e.key === 'ArrowRight' ? 1 : tabs.length - 1)) % tabs.length];
        nb.focus();
        showTab(nb.dataset.tab);
      });
    }
    $('#audit-refresh').addEventListener('click', loadAudit);
    $('#audit-verify').addEventListener('click', verifyAudit);
    $('#drawer-close').addEventListener('click', closeDrawer);
    $('#drawer-backdrop').addEventListener('click', closeDrawer);
    document.addEventListener('keydown', (e) => { if (e.key === 'Escape' && S.drawerId) { e.preventDefault(); closeDrawer(); } });
    document.addEventListener('visibilitychange', () => { if (!document.hidden && S.tab === 'monitor') safe(renderChart, S.lastChart); });
  }

  boot();
  // Chart.js sizes itself on creation; if the page was hidden then, it keeps the 150 px default.
  // Re-measure periodically and resize when the container and the chart disagree.
  setInterval(() => {
    const wrap = document.getElementById('chart-wrap');
    if (S.chart && wrap && !wrap.hidden && wrap.clientHeight > 0 && Math.abs(S.chart.height - wrap.clientHeight) > 4) {
      try { S.chart.resize(); } catch (e) { /* ignore */ }
    }
  }, 1000);
})();
