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
const SPLITS = [['no_split', 'OVR'], ['vs_lhp', 'vLHP'], ['vs_rhp', 'vRHP'], ['home', 'HOME'], ['away', 'AWAY']];

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

// A team's block: name/rank line, then one sub-table per split (OVR/vLHP/vRHP/
// HOME/AWAY), each with a split-label header row + a row per stat. Columns are
// sized across all splits so the whole block aligns. Range cells run together
// (the colored square separates + grades each) to stay narrow.
function teamBlock(team) {
  const allRows = [];
  team.splits.forEach(sp => {
    allRows.push(['', ...RANGES.map(r => r[1])]);            // column-header row (no split label)
    sp.rows.forEach(r => allRows.push([r.label, ...r.cells.map(c => c.text)]));
  });
  const w = [];
  for (let c = 0; c < 5; c++) w[c] = Math.max(...allRows.map(r => (r[c] || '').length));
  const line = (row, tiers) => {
    const label = (row[0] || '').padEnd(w[0]);
    // value first, tier square on the RIGHT; date-range columns divided by │
    const cells = row.slice(1).map((cell, i) => (cell || '').padStart(w[i + 1]) + (tiers ? (SQ[tiers[i]] || '⬜') : '⬜')).join('│');
    return `${label}│${cells}`;
  };
  // Split label: its own centered row above the data, ALL CAPS, underlined via
  // the U+0332 combining low line (real <u> doesn't render inside <pre>).
  const WIDTH = line(['', ...RANGES.map(r => r[1])], null).length;
  const underline = s => [...s].map(c => c + '̲').join('');
  const center = label => ' '.repeat(Math.max(0, Math.floor((WIDTH - label.length) / 2))) + underline(label.toUpperCase());
  const out = [`${team.abbr} #${team.wrcRank}`];
  team.splits.forEach(sp => {
    out.push(center(sp.label));
    out.push(line(['', ...RANGES.map(r => r[1])], null));
    sp.rows.forEach(r => out.push(line([r.label, ...r.cells.map(c => c.text)], r.cells.map(c => c.tier))));
  });
  return out;
}
const teamPre = team => '<pre>' + teamBlock(team).join('\n') + '</pre>';

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

  // ── #43 Morning digest: one message ranking today's games by offensive mismatch ──
  const wrcRank = await page.evaluate(() => {
    const arr = (allData['no_split-season'] || []).filter(r => r.wrcPlus != null).sort((a, b) => b.wrcPlus - a.wrcPlus);
    const m = {}; arr.forEach((r, i) => m[r.tm] = { rank: i + 1 }); return m;
  });
  const dsq = rk => rk <= 8 ? '🟩' : rk <= 15 ? '🟨' : rk <= 22 ? '🟧' : '🟥';
  const digestRows = games.map(g => {
    const [a, h] = g.value.split('-'); const A = wrcRank[a], H = wrcRank[h];
    if (!A || !H) return null;
    return { a, h, ar: A.rank, hr: H.rank, gap: Math.abs(A.rank - H.rank), time: (schedMap[g.value] || {}).time || '' };
  }).filter(Boolean).sort((x, y) => y.gap - x.gap);
  if (digestRows.length) {
    const lines = digestRows.map((r, i) => `${i + 1}. ${dsq(r.ar)}${r.a} #${r.ar}  ⚔️  ${dsq(r.hr)}${r.h} #${r.hr}  ·  Δ${r.gap}${r.time ? `  ·  ${r.time} ET` : ''}`);
    await tgMessage(`🌅 <b>Today's Offensive Mismatches</b> · ${dateStr}\n<i>Games ranked by gap in team wRC+ rank (bigger Δ = bigger edge)</i>\n\n${lines.join('\n')}`);
    if (!DRY_RUN) await sleep(1400);
    console.log(`  ✓ digest (${digestRows.length} games)`);
  }

  let ok = 0;
  for (const g of games) {
    const [awayAbbr, homeAbbr] = g.value.split('-');
    try {
      const d = await page.evaluate(({ awayAbbr, homeAbbr, STATS, RANGES, SPLITS }) => {
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
          const overall = allData['no_split-season'] || [];
          if (!overall.find(r => r.tm === tm)) return null;
          const splits = SPLITS.map(([sv, slabel]) => {
            const seasonArr = allData[`${sv}-season`] || [];
            const srow = seasonArr.find(r => r.tm === tm);
            if (!srow) return null;   // team not present in this split
            const rows = STATS.map(k => {
              const m = meta(k);
              const cells = RANGES.map(([rv], ci) => {
                const arr = allData[`${sv}-${rv}`] || [];
                const row = arr.find(r => r.tm === tm);
                if (!row || row[k] == null) return { text: '—', tier: '' };
                const tr = ci === 0 ? '' : trend(row[k], srow[k], m.higherBetter);
                return { text: `${m.fmt(row[k])}${tr}`, tier: getTier(m, row[k]) };
              });
              return { label: m.label, cells };
            });
            return { label: slabel, rows };
          }).filter(Boolean);
          return { abbr: tm, name: MLB_FULL_NAMES[tm] || tm, wrcRank: rankOf(overall, 'wrcPlus', true, tm), splits };
        };
        return { away: team(awayAbbr), home: team(homeAbbr) };
      }, { awayAbbr, homeAbbr, STATS, RANGES, SPLITS });

      if (!d.away || !d.home) { console.log(`  – skip ${g.value} (no team data)`); continue; }

      const sm = schedMap[g.value] || {};
      const aSP = sm.awayProbable || 'TBD', hSP = sm.homeProbable || 'TBD';
      const header = `⚾ <b>${d.away.abbr} @ ${d.home.abbr}</b>${sm.time ? ` · ${sm.time} ET` : ''}\n🎯 <b>SP:</b> ${aSP} vs ${hSP}`;
      const combined = `${header}\n${teamPre(d.away)}\n${teamPre(d.home)}`;
      // Telegram caps at 4096 chars; with all 5 splits, split into two (per team) if needed.
      if (combined.length <= 3900) {
        await tgMessage(combined);
      } else {
        await tgMessage(`${header}\n${teamPre(d.away)}`);
        if (!DRY_RUN) await sleep(1400);
        await tgMessage(teamPre(d.home));
      }
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
