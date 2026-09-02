// Interactive Telegram bot (#41) — Supabase Edge Function webhook.
//
// What it answers:
//   • a team abbreviation ("LAD")  → that team's offensive card: all 5 splits ×
//     all date ranges, per-cell color-graded, with ▲/▼ trends.
//   • LINEUPS / /lineups           → today's slate as tappable buttons; tapping
//     one returns both lineups with per-hitter wRC+ (or wOBA/AVG/K%) across
//     SZN/L30/L14/L7. Hitters are keyed by initials so the grid fits a phone.
//   • a partial team ("NY", "yank") → suggestion buttons for every match.
//   • inline mode: type "@<bot> ny" in ANY chat and matching teams pop up as
//     you type. Requires inline mode to be switched on once in @BotFather:
//     /setinline → pick the bot → placeholder e.g. "team abbr…".
//
// Deploy:
//   supabase functions deploy tg-bot --no-verify-jwt
//   supabase secrets set TELEGRAM_TOKEN=<botToken> WEBHOOK_SECRET=<anyRandomString>
//   curl "https://api.telegram.org/bot<botToken>/setWebhook?url=https://<ref>.functions.supabase.co/tg-bot&secret_token=<WEBHOOK_SECRET>&allowed_updates=%5B%22message%22,%22callback_query%22,%22inline_query%22%5D"
//
// Env: TELEGRAM_TOKEN, WEBHOOK_SECRET (both secrets). SUPABASE_URL /
// SUPABASE_ANON_KEY are provided by the platform (public read is fine).

const SUPABASE_URL = Deno.env.get('SUPABASE_URL') || 'https://mfliuasrygxkembqmrkr.supabase.co';
const SUPABASE_ANON = Deno.env.get('SUPABASE_ANON_KEY')
  || 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im1mbGl1YXNyeWd4a2VtYnFtcmtyIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzUyNTI2MzIsImV4cCI6MjA5MDgyODYzMn0.ecYwYajjAfKNl22ONtYp2i99U-ePEAZ6uHESPiiT8nw';
const TOKEN = Deno.env.get('TELEGRAM_TOKEN') || '';
const WEBHOOK_SECRET = Deno.env.get('WEBHOOK_SECRET') || '';

const STATS = [
  ['wrcPlus', 'wRC+', 'wrcplus', true, [120, 110, 100, 90]],
  ['ops', 'OPS', 'ops', true, [0.780, 0.750, 0.720, 0.690]],
  ['avg', 'AVG', 'avg', true, [0.270, 0.260, 0.245, 0.230]],
  ['obp', 'OBP', 'obp', true, [0.340, 0.325, 0.310, 0.295]],
  ['slg', 'SLG', 'slg', true, [0.450, 0.425, 0.400, 0.375]],
  ['woba', 'wOBA', 'woba', true, [0.340, 0.320, 0.310, 0.295]],
  ['iso', 'ISO', 'iso', true, [0.180, 0.165, 0.150, 0.135]],
  ['babip', 'BABIP', 'babip', true, [0.310, 0.300, 0.290, 0.280]],
  ['kPct', 'K%', 'k_pct', false, [19.0, 21.0, 23.0, 25.0]],
  ['bbPct', 'BB%', 'bb_pct', true, [10.5, 9.5, 8.5, 7.5]],
] as const;
const RANGES: [string, string][] = [['season', 'SZN'], ['last_30', 'L30'], ['last_14', 'L14'], ['last_7', 'L7']];
const SPLITS: [string, string][] = [['no_split', 'OVR'], ['vs_lhp', 'VLHP'], ['vs_rhp', 'VRHP'], ['home', 'HOME'], ['away', 'AWAY']];
const SQ: Record<string, string> = { elite: '🟩', good: '🟩', mid: '🟨', bad: '🟧', ass: '🟥' };

// Full names back the fuzzy match, so "yank" and "dodg" find NYY and LAD.
const TEAM_NAMES: Record<string, string> = {
  ARI: 'Arizona Diamondbacks', ATH: 'Athletics', ATL: 'Atlanta Braves',
  BAL: 'Baltimore Orioles', BOS: 'Boston Red Sox', CHC: 'Chicago Cubs',
  CHW: 'Chicago White Sox', CIN: 'Cincinnati Reds', CLE: 'Cleveland Guardians',
  COL: 'Colorado Rockies', DET: 'Detroit Tigers', HOU: 'Houston Astros',
  KCR: 'Kansas City Royals', LAA: 'Los Angeles Angels', LAD: 'Los Angeles Dodgers',
  MIA: 'Miami Marlins', MIL: 'Milwaukee Brewers', MIN: 'Minnesota Twins',
  NYM: 'New York Mets', NYY: 'New York Yankees', PHI: 'Philadelphia Phillies',
  PIT: 'Pittsburgh Pirates', SDP: 'San Diego Padres', SEA: 'Seattle Mariners',
  SFG: 'San Francisco Giants', STL: 'St. Louis Cardinals', TBR: 'Tampa Bay Rays',
  TEX: 'Texas Rangers', TOR: 'Toronto Blue Jays', WSN: 'Washington Nationals',
};

const num = (v: unknown) => parseFloat(String(v ?? '').replace('%', '')) || 0;
const esc = (s: unknown) => String(s ?? '').replace(/[&<>]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;' }[c] as string));
const fmt = (key: string, v: number) => key === 'wrcPlus' ? String(Math.round(v))
  : key === 'kPct' || key === 'bbPct' ? v.toFixed(1) + '%'
  : v.toFixed(3).replace(/^(-?)0\./, '$1.');

function tier(t: readonly number[], hb: boolean, v: number): string {
  const [a, b, c, d] = t;
  if (hb) return v >= a ? 'elite' : v >= b ? 'good' : v >= c ? 'mid' : v >= d ? 'bad' : 'ass';
  return v <= a ? 'elite' : v <= b ? 'good' : v <= c ? 'mid' : v <= d ? 'bad' : 'ass';
}
function rankOf(rows: any[], col: string, hb: boolean, tm: string): number | null {
  const s = rows.filter(r => r[col] != null).sort((a, b) => hb ? num(b[col]) - num(a[col]) : num(a[col]) - num(b[col]));
  const i = s.findIndex(r => r.tm === tm);
  return i >= 0 ? i + 1 : null;
}
function trend(cur: number, szn: number, hb: boolean): string {
  if (!szn) return '';
  const diff = cur - szn;
  if (Math.abs(diff / szn) < 0.03) return '';
  return (hb ? diff > 0 : diff < 0) ? '▲' : '▼';
}

// ── Supabase reads ──────────────────────────────────────────────────────────
async function sbGet(path: string): Promise<any[]> {
  const res = await fetch(`${SUPABASE_URL}/rest/v1/${path}`,
    { headers: { apikey: SUPABASE_ANON, Authorization: `Bearer ${SUPABASE_ANON}` } });
  if (!res.ok) throw new Error(`Supabase ${res.status}`);
  return await res.json();
}
// PostgREST caps a page at 1000 rows; player splits run well past that.
async function sbGetAll(table: string, params: string): Promise<any[]> {
  const PAGE = 1000;
  let all: any[] = [], pg = 0;
  while (true) {
    const page = await sbGet(`${table}?${params}&limit=${PAGE}&offset=${pg * PAGE}`);
    all = all.concat(page);
    if (page.length < PAGE) break;
    pg++;
  }
  return all;
}
const fetchAll = () => sbGet('fangraphs_splits?select=*&limit=1000');

// ── Telegram ────────────────────────────────────────────────────────────────
async function tg(method: string, body: unknown) {
  await fetch(`https://api.telegram.org/bot${TOKEN}/${method}`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body),
  });
}
const send = (chatId: number, text: string, reply_markup?: unknown) =>
  tg('sendMessage', { chat_id: chatId, text, parse_mode: 'HTML', disable_web_page_preview: true, ...(reply_markup ? { reply_markup } : {}) });

// ── Team offense card (unchanged format) ────────────────────────────────────
function buildCard(rows: any[], tm: string): string | null {
  const byKey: Record<string, any[]> = {};
  for (const r of rows) {
    const k = `${(r.split || '').trim()}-${(r.date_range || '').trim()}`;
    (byKey[k] ||= []).push(r);
  }
  const overall = byKey['no_split-season'] || [];
  if (!overall.find(r => r.tm === tm)) return null;
  const wrcRank = rankOf(overall, 'wrcplus', true, tm);

  const splits = SPLITS.map(([sv, slabel]) => {
    const seasonArr = byKey[`${sv}-season`] || [];
    const srow = seasonArr.find(r => r.tm === tm);
    if (!srow) return null;
    const rows2 = STATS.map(([key, label, col, hb, thr]) => {
      const cells = RANGES.map(([rv], ci) => {
        const arr = byKey[`${sv}-${rv}`] || [];
        const row = arr.find(r => r.tm === tm);
        if (!row || row[col] == null) return { text: '—', tier: '' };
        const tr = ci === 0 ? '' : trend(num(row[col]), num(srow[col]), hb as boolean);
        return { text: `${fmt(key, num(row[col]))}${tr}`, tier: tier(thr, hb as boolean, num(row[col])) };
      });
      return { label, cells };
    });
    return { label: slabel, rows: rows2 };
  }).filter(Boolean) as any[];

  // widths across all splits
  const allRows: string[][] = [];
  splits.forEach(sp => { allRows.push(['', ...RANGES.map(r => r[1])]); sp.rows.forEach((r: any) => allRows.push([r.label, ...r.cells.map((c: any) => c.text)])); });
  const w = [0, 0, 0, 0, 0].map((_, c) => Math.max(...allRows.map(r => (r[c] || '').length)));
  const line = (row: string[], tiers: (string | null)[] | null) => {
    const label = (row[0] || '').padEnd(w[0]);
    // tier square on the RIGHT of each value; cells run together (square separates), narrow
    const cells = row.slice(1).map((cell, i) => (cell || '').padStart(w[i + 1]) + (tiers ? (SQ[tiers[i] as string] || '⬜') : '⬜')).join('');
    return `${label} ${cells}`;
  };
  const WIDTH = line(['', ...RANGES.map(r => r[1])], null).length;
  const underline = (s: string) => [...s].map(c => c + '̲').join('');
  const center = (label: string) => ' '.repeat(Math.max(0, Math.floor((WIDTH - label.length) / 2))) + underline(label.toUpperCase());

  const out = [`${tm} #${wrcRank} offense`];
  splits.forEach(sp => {
    out.push(center(sp.label));
    out.push(line(['', ...RANGES.map(r => r[1])], null));
    sp.rows.forEach((r: any) => out.push(line([r.label, ...r.cells.map((c: any) => c.text)], r.cells.map((c: any) => c.tier))));
  });
  return `<b>${tm}</b>\n<i>great 🟩 · avg 🟨 · below 🟧 · poor 🟥 · vs season ▲▼</i>\n<pre>${out.join('\n')}</pre>`;
}

// ── Team matching (the "options pop up" half) ───────────────────────────────
// Ranked: exact abbr, abbr prefix, full-name word prefix, then any substring.
function matchTeams(q: string, teams: string[]): string[] {
  const s = q.trim().toUpperCase().replace(/[^A-Z0-9 ]/g, '');
  if (!s) return teams;
  const seen = new Set<string>();
  const out: string[] = [];
  const take = (t: string) => { if (!seen.has(t)) { seen.add(t); out.push(t); } };
  teams.forEach(t => { if (t === s) take(t); });
  teams.forEach(t => { if (t.startsWith(s)) take(t); });
  teams.forEach(t => {
    const words = (TEAM_NAMES[t] || '').toUpperCase().split(/\s+/);
    if (words.some(w => w.startsWith(s))) take(t);
  });
  teams.forEach(t => { if (t.includes(s) || (TEAM_NAMES[t] || '').toUpperCase().includes(s)) take(t); });
  return out;
}
const teamButtons = (abbrs: string[], perRow = 5) => {
  const rows: any[][] = [];
  abbrs.forEach((t, i) => {
    if (i % perRow === 0) rows.push([]);
    rows[rows.length - 1].push({ text: t, callback_data: `t:${t}` });
  });
  return rows;
};

// ── Lineup stats ────────────────────────────────────────────────────────────
// Leadoff sees ~4.6 PA/g down to ~3.8 for the 9-hole; only the relative
// weights matter for a weighted mean.
const PA_BY_SLOT = [4.6, 4.5, 4.4, 4.3, 4.2, 4.1, 4.0, 3.9, 3.8];
const LU_STAT_KEYS = ['wrcPlus', 'woba', 'avg', 'kPct'] as const;
const SUFFIX = /^(jr|sr|ii|iii|iv)$/i;

const words = (name: string) =>
  String(name || '').replace(/[.]/g, ' ').trim().split(/\s+/).filter(w => w && !SUFFIX.test(w));

// "Kyle Schwarber" → KS, "J.T. Realmuto" → JR, "Bryan De La Cruz" → BC
function initials(name: string): string {
  const p = words(name);
  if (!p.length) return '??';
  return ((p[0][0] || '') + (p[p.length - 1][0] || '')).toUpperCase();
}
// Two hitters in one lineup can share initials — break the tie with the second
// letter of the last name rather than printing the same key twice.
function initialsForLineup(names: string[]): string[] {
  const base = names.map(initials);
  const groups: Record<string, number[]> = {};
  base.forEach((b, i) => (groups[b] ||= []).push(i));
  const out = [...base];
  for (const [b, idx] of Object.entries(groups)) {
    if (idx.length < 2) continue;
    idx.forEach(i => {
      const p = words(names[i]);
      const last = p[p.length - 1] || '';
      out[i] = b + (last[1] || '').toLowerCase();
    });
  }
  return out;
}

const normLast = (name: string) => {
  const p = words(name);
  return (p[p.length - 1] || '').toLowerCase().replace(/[^a-z]/g, '');
};
const firstInit = (name: string) => (words(name)[0] || '')[0]?.toLowerCase() || '';

// Same match order as the site: team + last name + first initial, then any team.
function findPlayer(rows: any[], fullName: string, tm: string) {
  const ln = normLast(fullName), fi = firstInit(fullName);
  return rows.find(p => normLast(p.name) === ln && firstInit(p.name) === fi && p.tm === tm)
    || rows.find(p => normLast(p.name) === ln && firstInit(p.name) === fi)
    || null;
}

const parseOrder = (rec: any): any[] => {
  const bo = rec?.batting_order;
  if (Array.isArray(bo)) return bo;
  try { return bo ? JSON.parse(bo) : []; } catch { return []; }
};
const etToday = () => new Date().toLocaleDateString('en-CA', { timeZone: 'America/New_York' });

async function fetchLineups(): Promise<any[]> {
  const today = await sbGet(`projected_lineups?select=*&game_date=eq.${etToday()}&order=game_time.asc`);
  if (today.length) return today;
  // Off-hours / pre-scrape: fall back to the most recent slate on file.
  const last = await sbGet('projected_lineups?select=game_date&order=game_date.desc&limit=1');
  const d = last[0]?.game_date;
  return d ? await sbGet(`projected_lineups?select=*&game_date=eq.${d}&order=game_time.asc`) : [];
}
const fetchPlayerSplits = (split: string) => sbGetAll('fangraphs_player_splits',
  `select=name,tm,split,date_range,pa,wrcplus,woba,avg,k_pct&split=eq.${split}&order=name.asc`);

// Pair the slate off each row's stored opponent; a row whose partner was never
// scraped still shows on its own rather than vanishing.
function pairGames(rows: any[]) {
  const used = new Set<any>(), pairs: { away: any; home: any }[] = [];
  for (const r of rows) {
    if (used.has(r)) continue;
    used.add(r);
    const other = r.opponent ? rows.find(o => o !== r && !used.has(o) && o.team === r.opponent) : null;
    if (other) used.add(other);
    pairs.push(r.side === 'Home' ? { away: other, home: r } : { away: r, home: other });
  }
  return pairs;
}
const gameKey = (p: { away: any; home: any }) => `${p.away?.team || '???'}@${p.home?.team || '???'}`;

// One team's block: PA-weighted lineup line, then a row per hitter keyed by
// initials, with the chosen stat across SZN/L30/L14/L7.
function buildTeamLineup(rec: any, teamRows: any[], playerRows: Record<string, any[]>, statKey: string): string[] {
  const abbr = rec?.team || '???';
  const meta = STATS.find(s => s[0] === statKey) || STATS[0];
  const [key, label, col, hb, thr] = meta;
  const batters = parseOrder(rec);
  const status = rec?.lineup_status === 'Confirmed' ? '✓' : '~';
  const sp = rec?.pitcher_name || 'TBD';
  const head = [`${abbr} ${status}  SP ${sp}`];
  if (!batters.length) return [...head, '  lineup not posted yet'];

  const keys = initialsForLineup(batters.map((b: any) => b.name));
  const seasonRows = playerRows['season'] || [];

  // PA-weighted lineup average, and how it compares to the team's season number
  let wsum = 0, acc = 0, matched = 0;
  batters.forEach((b: any) => {
    const st = findPlayer(seasonRows, b.name, abbr);
    const v = st ? num(st[col]) : null;
    if (st && st[col] != null) { const w = PA_BY_SLOT[(b.order || 1) - 1] ?? 4.2; wsum += w; acc += w * (v as number); matched++; }
  });
  const teamRow = teamRows.find(r => r.tm === abbr);
  const teamVal = teamRow && teamRow[col] != null ? num(teamRow[col]) : null;
  if (matched) {
    const lv = acc / wsum;
    const d = teamVal != null ? lv - teamVal : null;
    const arrow = d == null ? '' : Math.abs(d) < (key === 'wrcPlus' ? 0.5 : 0.0005) ? '●' : (hb ? d > 0 : d < 0) ? '▲' : '▼';
    const dTxt = d == null ? '' : ` ${arrow}${fmt(key, Math.abs(d))} vs tm ${fmt(key, teamVal as number)}`;
    head.push(`lineup ${label} ${fmt(key, lv)}${SQ[tier(thr, hb as boolean, lv)]}${dTxt} (${matched}/${batters.length})`);
  }

  // Column widths sized off the values actually printed
  const cellsFor = (b: any) => RANGES.map(([rv]) => {
    const st = findPlayer(playerRows[rv] || [], b.name, abbr);
    if (!st || st[col] == null) return { text: '—', tier: '' };
    const v = num(st[col]);
    return { text: fmt(key, v), tier: tier(thr, hb as boolean, v) };
  });
  const body = batters.map((b: any, i: number) => ({ slot: `${b.order || i + 1} ${keys[i]}`, cells: cellsFor(b) }));
  const kw = Math.max(...body.map(r => r.slot.length));
  const cw = RANGES.map((r, i) => Math.max(r[1].length, ...body.map(b => b.cells[i].text.length)));

  const out = [...head, ' '.repeat(kw) + ' ' + RANGES.map((r, i) => r[1].padStart(cw[i]) + '⬜').join('')];
  body.forEach(r => out.push(r.slot.padEnd(kw) + ' ' + r.cells.map((c, i) => c.text.padStart(cw[i]) + (SQ[c.tier] || '⬜')).join('')));
  return out;
}

function buildLineupCard(pair: { away: any; home: any }, teamRows: any[], playerRows: Record<string, any[]>, statKey: string, splitLabel: string): string {
  const rec = pair.away || pair.home;
  const time = pair.away?.game_time || pair.home?.game_time || '';
  const title = `${pair.away?.team || '???'} @ ${pair.home?.team || '???'}`;
  const label = (STATS.find(s => s[0] === statKey) || STATS[0])[1];
  const out: string[] = [];
  if (pair.away) out.push(...buildTeamLineup(pair.away, teamRows, playerRows, statKey), '');
  if (pair.home) out.push(...buildTeamLineup(pair.home, teamRows, playerRows, statKey));
  return `<b>${esc(title)}</b>${time ? ` · ${esc(time)}` : ''}\n`
    + `<i>${label} · ${splitLabel} · SZN/L30/L14/L7 · hitters by initials</i>\n`
    + `<pre>${esc(out.join('\n'))}</pre>`;
}

const SPLIT_LABEL: Record<string, string> = { no_split: 'Overall', vs_lhp: 'vs LHP', vs_rhp: 'vs RHP' };
const lineupMarkup = (gk: string, statKey: string, split: string) => ({
  inline_keyboard: [
    LU_STAT_KEYS.map(k => ({ text: (k === statKey ? '• ' : '') + (STATS.find(s => s[0] === k) as any)[1], callback_data: `l:${gk}:${k}:${split}` })),
    Object.keys(SPLIT_LABEL).map(s => ({ text: (s === split ? '• ' : '') + SPLIT_LABEL[s], callback_data: `l:${gk}:${statKey}:${s}` })),
  ],
});

// Send one game's lineup card. `split` empty ⇒ pick the split that matches the
// handedness each lineup is actually facing (falls back to overall).
async function sendLineup(chatId: number, gk: string, statKey: string, split: string) {
  const [lineups, teamRows] = await Promise.all([fetchLineups(), fetchAll()]);
  const pair = pairGames(lineups).find(p => gameKey(p) === gk);
  if (!pair) { await send(chatId, `No lineup on file for <b>${esc(gk)}</b> ⚾`); return; }
  let sp = split;
  if (!sp) {
    // Each side faces the OTHER team's starter; use it only when both agree.
    const aHand = pair.home?.pitcher_hand, hHand = pair.away?.pitcher_hand;
    sp = aHand && aHand === hHand ? (aHand === 'L' ? 'vs_lhp' : 'vs_rhp') : 'no_split';
  }
  const rows = await fetchPlayerSplits(sp);
  const byRange: Record<string, any[]> = {};
  for (const r of rows) (byRange[(r.date_range || '').trim()] ||= []).push(r);
  const seasonTeams = teamRows.filter(r => (r.split || '').trim() === sp && (r.date_range || '').trim() === 'season');
  await send(chatId, buildLineupCard(pair, seasonTeams, byRange, statKey, SPLIT_LABEL[sp] || sp), lineupMarkup(gk, statKey, sp));
}

async function sendSlate(chatId: number) {
  const lineups = await fetchLineups();
  const pairs = pairGames(lineups);
  if (!pairs.length) { await send(chatId, 'No lineups posted yet — they land through the afternoon ⚾'); return; }
  const date = lineups[0]?.game_date || '';
  const rows = pairs.map(p => [{
    text: `${p.away?.team || '???'} @ ${p.home?.team || '???'}${p.away?.game_time ? ' · ' + p.away.game_time.replace(' ET', '') : ''}`,
    callback_data: `l:${gameKey(p)}::`,
  }]);
  await send(chatId, `<b>Lineups · ${esc(date)}</b>\nPick a game for per-hitter stats 📋`, { inline_keyboard: rows });
}

// ── Handlers ────────────────────────────────────────────────────────────────
const HELP = 'Send a team abbreviation for its offensive card. ⚾\n'
  + '<b>LINEUPS</b> — today\'s slate with per-hitter stats 📋\n'
  + 'Type part of a name ("NY", "dodg") and I\'ll offer the matches.\n'
  + 'In any chat, type <code>@thisbot ny</code> to pick a team as you type.';

async function handleMessage(chatId: number, text: string) {
  const rows = await fetchAll();
  const teams = [...new Set(rows.map(r => (r.tm || '').trim()).filter(Boolean))].sort();
  const raw = text.trim().replace(/^\//, '').split(/\s+/)[0].toUpperCase();

  if (!raw || raw === 'START' || raw === 'HELP') {
    await send(chatId, HELP, { inline_keyboard: [...teamButtons(teams), [{ text: 'Lineups 📋', callback_data: 'slate' }]] });
    return;
  }
  if (raw === 'LINEUPS' || raw === 'LINEUP' || raw === 'SLATE') { await sendSlate(chatId); return; }

  const abbr = raw.replace(/[^A-Z0-9]/g, '');
  if (teams.includes(abbr)) {
    const card = buildCard(rows, abbr);
    if (card) { await send(chatId, card, { inline_keyboard: [[{ text: 'Lineup stats 📋', callback_data: `lt:${abbr}` }]] }); return; }
  }
  // Not an exact team — offer whatever it does match instead of just erroring.
  const hits = matchTeams(text.trim(), teams).slice(0, 10);
  if (hits.length === 1) {
    const card = buildCard(rows, hits[0]);
    if (card) { await send(chatId, card, { inline_keyboard: [[{ text: 'Lineup stats 📋', callback_data: `lt:${hits[0]}` }]] }); return; }
  }
  if (hits.length) {
    await send(chatId, `Did you mean… 🔎`, { inline_keyboard: teamButtons(hits, 3) });
    return;
  }
  await send(chatId, `No team matches "<b>${esc(abbr)}</b>". Pick one: ❓`, { inline_keyboard: teamButtons(teams) });
}

async function handleCallback(cb: any) {
  const chatId = cb?.message?.chat?.id;
  const data = String(cb?.data || '');
  await tg('answerCallbackQuery', { callback_query_id: cb.id });
  if (!chatId) return;

  if (data === 'slate') { await sendSlate(chatId); return; }
  if (data.startsWith('t:')) {
    const tm = data.slice(2);
    const rows = await fetchAll();
    const card = buildCard(rows, tm);
    await send(chatId, card || `No data for ${esc(tm)} ❓`,
      card ? { inline_keyboard: [[{ text: 'Lineup stats 📋', callback_data: `lt:${tm}` }]] } : undefined);
    return;
  }
  if (data.startsWith('lt:')) {
    // "lineup stats for this team" — resolve the team to whichever game it's in.
    const tm = data.slice(3);
    const pairs = pairGames(await fetchLineups());
    const pair = pairs.find(p => p.away?.team === tm || p.home?.team === tm);
    if (!pair) { await send(chatId, `${esc(tm)} has no lineup posted today ⚾`); return; }
    await sendLineup(chatId, gameKey(pair), 'wrcPlus', '');
    return;
  }
  if (data.startsWith('l:')) {
    const [, gk, statKey, split] = data.split(':');
    await sendLineup(chatId, gk, statKey || 'wrcPlus', split || '');
    return;
  }
}

// Inline mode: this is what makes options pop up AS you type, in any chat.
async function handleInline(iq: any) {
  const rows = await fetchAll();
  const teams = [...new Set(rows.map(r => (r.tm || '').trim()).filter(Boolean))].sort();
  const hits = matchTeams(String(iq.query || ''), teams).slice(0, 8);
  const results = hits.map(tm => {
    const card = buildCard(rows, tm);
    return {
      type: 'article',
      id: tm,
      title: `${tm} — ${TEAM_NAMES[tm] || tm}`,
      description: 'Offensive card · 5 splits × SZN/L30/L14/L7',
      input_message_content: { message_text: card || tm, parse_mode: 'HTML', disable_web_page_preview: true },
    };
  });
  await tg('answerInlineQuery', { inline_query_id: iq.id, results, cache_time: 30, is_personal: true });
}

Deno.serve(async (req) => {
  if (WEBHOOK_SECRET && req.headers.get('X-Telegram-Bot-Api-Secret-Token') !== WEBHOOK_SECRET) {
    return new Response('forbidden', { status: 403 });
  }
  const update = await req.json().catch(() => null);
  const chatId = update?.message?.chat?.id ?? update?.callback_query?.message?.chat?.id;

  try {
    if (update?.inline_query) await handleInline(update.inline_query);
    else if (update?.callback_query) await handleCallback(update.callback_query);
    else if (update?.message) await handleMessage(update.message.chat.id, String(update.message.text || ''));
  } catch (e) {
    if (chatId) await send(chatId, `Error: ${esc((e as Error).message)} ⚠️`);
  }
  return new Response('ok');
});
