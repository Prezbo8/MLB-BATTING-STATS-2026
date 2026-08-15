// Daily report → Telegram: one text message per game with a full stat table
// per team — every stat across all date ranges (SZN/L30/L14/L7), each cell
// showing value + #rank, with ▲/▼ trend arrows (recent vs season, 3% rule).
// Built from the live site's data so numbers/ranks stay consistent.
//
// Env: TELEGRAM_TOKEN, TELEGRAM_CHAT_ID | REPORT_URL (override) | DRY_RUN=1
const { chromium } = require('playwright');

const TOKEN   = process.env.TELEGRAM_TOKEN;
const CHAT_ID = process.env.TELEGRAM_CHAT_ID;
const DRY_RUN = process.env.DRY_RUN === '1' || !TOKEN;
const REPORT_URL = process.env.REPORT_URL
  || 'https://prezbo8.github.io/MLB-BATTING-STATS-2026/index.html#tab=8';

const STATS  = ['wrcPlus', 'ops', 'avg', 'obp', 'slg', 'woba', 'iso', 'babip', 'kPct', 'bbPct'];
const RANGES = [['season', 'SZN'], ['last_30', 'L30'], ['last_14', 'L14'], ['last_7', 'L7']];

const sleep = ms => new Promise(r => setTimeout(r, ms));

async function tgMessage(text) {
  if (DRY_RUN) { console.log('\n───── MESSAGE ─────\n' + text.replace(/<\/?(b|pre)>/g, '') + '\n'); return; }
  const res = await fetch(`https://api.telegram.org/bot${TOKEN}/sendMessage`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ chat_id: CHAT_ID, text, parse_mode: 'HTML', disable_web_page_preview: true }),
  });
  if (!res.ok) throw new Error(`Telegram ${res.status}: ${(await res.text()).slice(0, 200)}`);
}

// Tier → colored square (Telegram can't color text). Every cell gets exactly
// one uniform-width square, so per-cell coloring stays aligned in the table.
const SQ = { elite: '🟩', good: '🟩', mid: '🟨', bad: '🟧', ass: '🟥' };

// One team's lines: header (ABBR #rank | SZN L30 L14 L7) + a row per stat, each
// data cell = tier-square + value#rank(+trend). Columns padded on the text only.
function teamLines(team) {
  const headerRow = [`${team.abbr} #${team.wrcRank}`, ...RANGES.map(r => r[1])];
  const grid = [headerRow, ...team.rows.map(r => [r.label, ...r.cells.map(c => c.text)])];
  const w = [];
  for (let c = 0; c < grid[0].length; c++) w[c] = Math.max(...grid.map(r => (r[c] || '').length));
  // Label + one space, then the range cells run together (the square separates
  // and colors each), so the whole row stays narrow enough for a phone.
  const line = (row, tiers) => {
    const label = (row[0] || '').padEnd(w[0]);
    const cells = row.slice(1).map((cell, i) => {
      const sq = tiers ? (SQ[tiers[i]] || '⬜') : '⬜';
      return sq + (cell || '').padStart(w[i + 1]);
    }).join('');
    return `${label} ${cells}`;
  };
  const lines = [line(headerRow, null)];
  team.rows.forEach(r => lines.push(line([r.label, ...r.cells.map(c => c.text)], r.cells.map(c => c.tier))));
  return lines;
}
const gameTables = (away, home) => '<pre>' + [...teamLines(away), '', ...teamLines(home)].join('\n') + '</pre>';

(async () => {
  console.log(DRY_RUN ? '● DRY RUN (no Telegram)' : '● Live send', '→', REPORT_URL);
  const browser = await chromium.launch();
  const page = await browser.newPage({ viewport: { width: 1280, height: 900 }, timezoneId: 'America/New_York', locale: 'en-US' });
  page.setDefaultTimeout(60000);
  await page.goto(REPORT_URL, { waitUntil: 'load' });
  await page.waitForFunction(() =>
    typeof allData !== 'undefined' && Object.keys(allData).length > 0
    && document.getElementById('mlbGameDropdown')?.options.length > 1, { timeout: 90000 });

  const dateStr = await page.evaluate(() => (typeof localDateStr === 'function' ? localDateStr() : new Date().toISOString().slice(0, 10)));
  const games = await page.evaluate(() =>
    Array.from(document.getElementById('mlbGameDropdown').options).filter(o => o.value)
      .map(o => ({ value: o.value, label: o.textContent.trim() })));

  // Game times + probable-pitcher names.
  const NAME2ABBR = await page.evaluate(() => MLB_NAME_TO_ABBR);
  const schedMap = {};
  try {
    const r = await fetch(`https://statsapi.mlb.com/api/v1/schedule?sportId=1&date=${dateStr}&hydrate=team,probablePitcher`);
    const j = await r.json();
    (j.dates?.[0]?.games || []).forEach(gm => {
      const aA = NAME2ABBR[gm.teams?.away?.team?.name], hA = NAME2ABBR[gm.teams?.home?.team?.name];
      if (aA && hA) schedMap[`${aA}-${hA}`] = {
        time: gm.gameDate ? new Date(gm.gameDate).toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit', timeZone: 'America/New_York' }) : '',
        awayProbable: gm.teams?.away?.probablePitcher?.fullName || '',
        homeProbable: gm.teams?.home?.probablePitcher?.fullName || '',
      };
    });
  } catch (e) { console.error('schedule fetch failed:', e.message); }
  console.log(`Found ${games.length} game(s) for ${dateStr}`);

  if (!games.length) { await tgMessage(`📭 No MLB games scheduled for ${dateStr}.`); await browser.close(); return; }

  let ok = 0;
  for (const g of games) {
    const [awayAbbr, homeAbbr] = g.value.split('-');
    try {
      const d = await page.evaluate(({ awayAbbr, homeAbbr, STATS, RANGES }) => {
        const meta = k => statMeta.find(s => s.key === k);
        const rankOf = (arr, k, hb, tm) => {
          const s = arr.filter(r => r[k] != null).sort((a, b) => hb ? b[k] - a[k] : a[k] - b[k]);
          const i = s.findIndex(r => r.tm === tm); return i >= 0 ? i + 1 : null;
        };
        const trend = (cur, szn, hb) => {
          if (szn == null || cur == null || szn === 0) return '';
          const diff = cur - szn; if (Math.abs(diff / szn) < 0.03) return '';
          return (hb ? diff > 0 : diff < 0) ? '▲' : '▼';
        };
        const team = tm => {
          const seasonArr = allData['no_split-season'] || [];
          const srow = seasonArr.find(r => r.tm === tm);
          if (!srow) return null;
          const rows = STATS.map(k => {
            const m = meta(k);
            const cells = RANGES.map(([rv], ci) => {
              const arr = allData[`no_split-${rv}`] || [];
              const row = arr.find(r => r.tm === tm);
              if (!row || row[k] == null) return { text: '—', tier: '' };
              const tr = ci === 0 ? '' : trend(row[k], srow[k], m.higherBetter);
              return { text: `${m.fmt(row[k])}${tr}`, tier: getTier(m, row[k]) };
            });
            return { label: m.label, cells };
          });
          return { abbr: tm, name: MLB_FULL_NAMES[tm] || tm, wrcRank: rankOf(seasonArr, 'wrcPlus', true, tm), rows };
        };
        return { away: team(awayAbbr), home: team(homeAbbr) };
      }, { awayAbbr, homeAbbr, STATS, RANGES });

      if (!d.away || !d.home) { console.log(`  – skip ${g.value} (no team data)`); continue; }

      const sm = schedMap[g.value] || {};
      const aSP = sm.awayProbable || 'TBD', hSP = sm.homeProbable || 'TBD';
      const header = `⚾ <b>${d.away.abbr} @ ${d.home.abbr}</b>${sm.time ? ` · ${sm.time} ET` : ''}`;
      const msg = `${header}\n🎯 <b>SP:</b> ${aSP} vs ${hSP}\n${gameTables(d.away, d.home)}`;

      await tgMessage(msg);
      console.log(`  ✓ ${g.label}`);
      if (!DRY_RUN) await sleep(1400);
      ok++;
    } catch (e) {
      console.error(`  ✗ ${g.value}: ${e.message}`);
    }
  }
  console.log(`Done: ${ok}/${games.length} game(s).`);
  await browser.close();
  if (!ok) process.exit(1);
})().catch(e => { console.error('FATAL', e); process.exit(1); });
