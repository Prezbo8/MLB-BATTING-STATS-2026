// Daily report → Telegram: sends a prose scouting-note text message per game
// (no PDF). Pulls the same live data the site uses (headless Chromium) so the
// numbers/ranks stay consistent, and uses the vs-LHP/vs-RHP split that matches
// each starting pitcher.
//
// Env: TELEGRAM_TOKEN, TELEGRAM_CHAT_ID | REPORT_URL (override) | DRY_RUN=1
const { chromium } = require('playwright');

const TOKEN   = process.env.TELEGRAM_TOKEN;
const CHAT_ID = process.env.TELEGRAM_CHAT_ID;
const DRY_RUN = process.env.DRY_RUN === '1' || !TOKEN;
const REPORT_URL = process.env.REPORT_URL
  || 'https://prezbo8.github.io/MLB-BATTING-STATS-2026/index.html#tab=8';

const sleep = ms => new Promise(r => setTimeout(r, ms));

async function tgMessage(text) {
  if (DRY_RUN) { console.log('\n───── MESSAGE ─────\n' + text.replace(/<\/?b>/g, '') + '\n'); return; }
  const res = await fetch(`https://api.telegram.org/bot${TOKEN}/sendMessage`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ chat_id: CHAT_ID, text, parse_mode: 'HTML', disable_web_page_preview: true }),
  });
  if (!res.ok) throw new Error(`Telegram ${res.status}: ${(await res.text()).slice(0, 200)}`);
}

// ── Prose composition (runs in Node from data the page returns) ───────────────
const NOUN = { obp: 'getting on base', slg: 'slugging', iso: 'power', woba: 'overall production', bbPct: 'drawing walks', avg: 'hitting for average' };
const nounFor = (s, kind) => s.key === 'kPct'
  ? (kind === 'strength' ? 'avoiding strikeouts' : 'striking out a lot')
  : (NOUN[s.key] || s.label);

function teamProse(team, facingSP) {
  const wrc = team.stats.find(s => s.key === 'wrcPlus');
  const others = team.stats.filter(s => s.key !== 'wrcPlus' && s.rank != null);
  const strengths  = others.filter(s => s.rank <= 8).sort((a, b) => a.rank - b.rank).slice(0, 2);
  const weaknesses = others.filter(s => s.rank >= 23).sort((a, b) => b.rank - a.rank).slice(0, 2);
  const chip = (s, kind) => `${nounFor(s, kind)} (${s.value}, #${s.rank})`;
  let out = `The <b>${team.name}</b> rank #${wrc.rank} in offense (${wrc.value} wRC+)`;
  if (strengths.length)  out += `, strong at ${strengths.map(s => chip(s, 'strength')).join(' and ')}`;
  if (weaknesses.length) out += `${strengths.length ? ' but' : ', though'} weak at ${weaknesses.map(s => chip(s, 'weakness')).join(' and ')}`;
  out += '.';
  if (team.split && facingSP && facingSP.hand) {
    const side = facingSP.hand === 'L' ? 'lefties' : 'righties';
    out += ` Against ${side}${facingSP.name ? ` like ${facingSP.name}` : ''} they've produced a ${team.split.wrc} wRC+ (${team.split.label}, #${team.split.rank}).`;
  } else if (team.split) {
    out += ` ${team.split.label}: ${team.split.wrc} wRC+ (#${team.split.rank}).`;
  }
  return out;
}

(async () => {
  console.log(DRY_RUN ? '● DRY RUN (no Telegram)' : '● Live send', '→', REPORT_URL);
  const browser = await chromium.launch();
  const page = await browser.newPage({ viewport: { width: 1280, height: 900 }, timezoneId: 'America/New_York', locale: 'en-US' });
  page.setDefaultTimeout(60000);
  await page.goto(REPORT_URL, { waitUntil: 'load' });

  // Need team data (allData) + the schedule dropdown; lineups are best-effort.
  await page.waitForFunction(() =>
    typeof allData !== 'undefined' && Object.keys(allData).length > 0
    && document.getElementById('mlbGameDropdown')?.options.length > 1, { timeout: 90000 });
  await page.waitForFunction(() => typeof lineupGames !== 'undefined' && lineupGames.length > 0, { timeout: 20000 }).catch(() => {});

  const dateStr = await page.evaluate(() => (typeof localDateStr === 'function' ? localDateStr() : new Date().toISOString().slice(0, 10)));
  const games = await page.evaluate(() =>
    Array.from(document.getElementById('mlbGameDropdown').options).filter(o => o.value)
      .map(o => ({ value: o.value, label: o.textContent.trim() })));
  // Starting pitcher (name + handedness) from projected_lineups.
  const lineupByTeam = await page.evaluate(() => Object.fromEntries(
    (lineupGames || []).filter(r => r.pitcher_name).map(r => [r.team, { name: r.pitcher_name, hand: (r.pitcher_hand || '').toUpperCase() }])));

  // Game times + probable-pitcher names (name-only fallback when lineups aren't posted yet).
  const NAME2ABBR = await page.evaluate(() => MLB_NAME_TO_ABBR);
  const schedMap = {};
  try {
    const r = await fetch(`https://statsapi.mlb.com/api/v1/schedule?sportId=1&date=${dateStr}&hydrate=team,probablePitcher`);
    const j = await r.json();
    (j.dates?.[0]?.games || []).forEach(gm => {
      const aA = NAME2ABBR[gm.teams?.away?.team?.name], hA = NAME2ABBR[gm.teams?.home?.team?.name];
      if (!aA || !hA) return;
      schedMap[`${aA}-${hA}`] = {
        time: gm.gameDate ? new Date(gm.gameDate).toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit', timeZone: 'America/New_York' }) : '',
        awayProbable: gm.teams?.away?.probablePitcher?.fullName || '',
        homeProbable: gm.teams?.home?.probablePitcher?.fullName || '',
      };
    });
  } catch (e) { console.error('schedule fetch failed:', e.message); }
  console.log(`Found ${games.length} game(s) for ${dateStr}`);

  if (!games.length) { await tgMessage(`📭 No MLB games scheduled for ${dateStr}.`); await browser.close(); return; }

  const STATS = ['wrcPlus', 'obp', 'slg', 'iso', 'woba', 'bbPct', 'kPct', 'avg']; // babip excluded (luck)

  let ok = 0;
  for (const g of games) {
    const [awayAbbr, homeAbbr] = g.value.split('-');
    try {
      const sm = schedMap[g.value] || {};
      const aSP = lineupByTeam[awayAbbr] || (sm.awayProbable ? { name: sm.awayProbable, hand: '' } : null);
      const hSP = lineupByTeam[homeAbbr] || (sm.homeProbable ? { name: sm.homeProbable, hand: '' } : null);
      // away offense faces the HOME starter's hand, and vice-versa
      const d = await page.evaluate(({ awayAbbr, homeAbbr, awayFacing, homeFacing, STATS }) => {
        const overall = allData['no_split-season'] || [];
        const meta = k => statMeta.find(s => s.key === k);
        const rankOf = (arr, k, hb, tm) => {
          const s = arr.filter(r => r[k] != null).sort((a, b) => hb ? b[k] - a[k] : a[k] - b[k]);
          const i = s.findIndex(r => r.tm === tm); return i >= 0 ? i + 1 : null;
        };
        const info = (tm, facingHand) => {
          const row = overall.find(r => r.tm === tm);
          if (!row) return null;
          const stats = STATS.map(k => { const m = meta(k); return { key: k, value: m.fmt(row[k]), rank: rankOf(overall, k, m.higherBetter, tm) }; });
          let split = null;
          const sp = facingHand === 'L' ? 'vs_lhp' : facingHand === 'R' ? 'vs_rhp' : null;
          if (sp) { const arr = allData[`${sp}-season`] || []; const r = arr.find(x => x.tm === tm); if (r) { const m = meta('wrcPlus'); split = { label: sp === 'vs_lhp' ? 'vs LHP' : 'vs RHP', wrc: m.fmt(r.wrcPlus), rank: rankOf(arr, 'wrcPlus', true, tm) }; } }
          return { abbr: tm, name: MLB_FULL_NAMES[tm] || tm, stats, split };
        };
        return { away: info(awayAbbr, awayFacing), home: info(homeAbbr, homeFacing) };
      }, { awayAbbr, homeAbbr, awayFacing: hSP?.hand || '', homeFacing: aSP?.hand || '', STATS });

      if (!d.away || !d.home) { console.log(`  – skip ${g.value} (no team data)`); continue; }

      const spLine = `🎯 <b>SP:</b> ${aSP?.name || 'TBD'}${aSP?.hand ? ` (${aSP.hand})` : ''} vs ${hSP?.name || 'TBD'}${hSP?.hand ? ` (${hSP.hand})` : ''}`;
      const paragraph = `${teamProse(d.away, hSP)} ${teamProse(d.home, aSP)}`;
      const header = `⚾ <b>${d.away.name} @ ${d.home.name}</b>${sm.time ? ` · ${sm.time} ET` : ''}`;
      const msg = `${header}\n${spLine}\n\n${paragraph}`;

      await tgMessage(msg);
      console.log(`  ✓ ${g.label}`);
      if (!DRY_RUN) await sleep(1400); // Telegram rate limit
      ok++;
    } catch (e) {
      console.error(`  ✗ ${g.value}: ${e.message}`);
    }
  }
  console.log(`Done: ${ok}/${games.length} game(s).`);
  await browser.close();
  if (!ok) process.exit(1);
})().catch(e => { console.error('FATAL', e); process.exit(1); });
