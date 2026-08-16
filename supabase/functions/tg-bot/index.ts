// Interactive Telegram bot (#41) — Supabase Edge Function webhook.
// Reply to the bot with a team abbreviation (e.g. "LAD") and it returns that
// team's offensive card: all 5 splits × all date ranges, per-cell color-graded,
// with ▲/▼ trends — the same format as the daily digest.
//
// Deploy:
//   supabase functions deploy tg-bot --no-verify-jwt
//   supabase secrets set TELEGRAM_TOKEN=<botToken> WEBHOOK_SECRET=<anyRandomString>
//   curl "https://api.telegram.org/bot<botToken>/setWebhook?url=https://<ref>.functions.supabase.co/tg-bot&secret_token=<WEBHOOK_SECRET>"
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

const num = (v: unknown) => parseFloat(String(v ?? '').replace('%', '')) || 0;
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

async function fetchAll(): Promise<any[]> {
  const res = await fetch(`${SUPABASE_URL}/rest/v1/fangraphs_splits?select=*&limit=1000`,
    { headers: { apikey: SUPABASE_ANON, Authorization: `Bearer ${SUPABASE_ANON}` } });
  if (!res.ok) throw new Error(`Supabase ${res.status}`);
  return await res.json();
}
async function send(chatId: number, text: string) {
  await fetch(`https://api.telegram.org/bot${TOKEN}/sendMessage`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ chat_id: chatId, text, parse_mode: 'HTML', disable_web_page_preview: true }),
  });
}

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
  return `<b>${tm}</b>\n<i>🟩 great · 🟨 avg · 🟧 below · 🟥 poor · ▲▼ vs season</i>\n<pre>${out.join('\n')}</pre>`;
}

Deno.serve(async (req) => {
  if (WEBHOOK_SECRET && req.headers.get('X-Telegram-Bot-Api-Secret-Token') !== WEBHOOK_SECRET) {
    return new Response('forbidden', { status: 403 });
  }
  const update = await req.json().catch(() => null);
  const msg = update?.message;
  const chatId = msg?.chat?.id;
  const text = String(msg?.text || '').trim();
  if (!chatId) return new Response('ok');

  try {
    const rows = await fetchAll();
    const teams = [...new Set(rows.map(r => (r.tm || '').trim()).filter(Boolean))].sort();
    const abbr = text.toUpperCase().split(/\s+/)[0].replace(/[^A-Z]/g, '');

    if (!abbr || abbr === 'START' || abbr === 'HELP') {
      await send(chatId, `⚾ Send a team abbreviation to get its offensive card.\n\nTeams: ${teams.join(', ')}`);
      return new Response('ok');
    }
    const card = teams.includes(abbr) ? buildCard(rows, abbr) : null;
    await send(chatId, card || `❓ Unknown team "<b>${abbr}</b>". Try one of: ${teams.join(', ')}`);
  } catch (e) {
    await send(chatId, `⚠️ Error: ${(e as Error).message}`);
  }
  return new Response('ok');
});
