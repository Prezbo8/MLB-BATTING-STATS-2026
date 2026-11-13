// assets/js/shared.js — code shared by index.html (desktop) and mobile.html (phone)
// Round F/G features: team trend lines, early-season priors, matchups + edges,
// pitcher drawer, tier cards. Loaded before each page's main script; it only
// touches page globals (SUPABASE_URL, statMeta, findPlayerStats, …) inside
// functions, so the order is safe. Page-specific markup stays in each page.

// ── PURE MATH (no page globals) ────────────────────────────────────────
// tests/test_shared_js.py runs these in Node and checks them against the Python
// (scripts/score_pitcher_overall.py, scripts/backtest_edges.py), so the site and
// the scoring can't quietly drift apart.
const SPLIT_PRIOR_PA = 100;                 // a vs-hand split leans on the overall line
const PRIOR_PA = 60, LAST_SEASON_SHRINK_PA = 100, EARLY_PA = 100;
const STARTER_STRONG = 1.0, STARTER_LEAN = 0.4;   // edge labels, in starter spreads (EDGES.md)
// Last season's line pulled toward 100; rookies (no line) start at 100
function priorFromLast(lastWrc, lastPa) {
    return lastWrc != null && lastPa ? (lastWrc * lastPa + 100 * LAST_SEASON_SHRINK_PA) / (lastPa + LAST_SEASON_SHRINK_PA) : 100;
}
// Overall line (this season's PA + last season worth PRIOR_PA), then the vs-hand
// split leaning on it. Returns null when there is no data at all.
function blendBatter(curWrc, curPa, splitWrc, splitPa, priorWrc, hasLast) {
    curPa = curPa || 0;
    const overall = ((curPa ? curWrc * curPa : 0) + priorWrc * PRIOR_PA) / (curPa + PRIOR_PA);
    if (splitPa) return { wrc: (splitWrc * splitPa + overall * SPLIT_PRIOR_PA) / (splitPa + SPLIT_PRIOR_PA), overall, fallback: false };
    if (curPa || hasLast) return { wrc: overall, overall, fallback: true };
    return null;
}
// Median and spread of established starters' scores (fallback 50/25 under 20)
function scaleFrom(scores) {
    const v = scores.filter(x => x != null && !isNaN(x)).map(Number).sort((a, b) => a - b);
    if (v.length < 20) return { mid: 50, sd: 25 };
    const mid = v[Math.floor(v.length / 2)];
    const sd = Math.sqrt(v.reduce((a, x) => a + (x - mid) ** 2, 0) / v.length) || 25;
    return { mid, sd };
}
// The drawer's "how the score is built": mirrors compute_composite_scores —
// season score × year weight × min(1, IP/60), plus 50 at weight 0.10
const PD_SEASON_WEIGHTS = [0.32, 0.20, 0.12, 0.08, 0.05, 0.03];   // newest season first
const PD_CAREER_WEIGHT = 0.20, PD_FULL_IP = 60, PD_PRIOR = 0.10, PD_MIN_IP = 20;
function drawerBlend(rows, newest) {
    const wOf = r => {
        const y = parseInt(r.season, 10);
        const base = y > 0 ? (PD_SEASON_WEIGHTS[newest - y] ?? 0) : PD_CAREER_WEIGHT;
        return base * Math.min(1, Math.max(0, +r.ip || 0) / PD_FULL_IP);
    };
    const items = rows.map(r => ({ r, y: parseInt(r.season, 10), s: r.season_score != null ? +r.season_score : null }))
        .sort((a, b) => (b.y || 0) - (a.y || 0) || (a.y ? -1 : 1));
    let sw = 0, num = 0;
    items.forEach(o => { o.w = o.s != null ? wOf(o.r) : 0; sw += o.w; num += o.w * (o.s || 0); });
    const total = sw + PD_PRIOR;
    return { items, sw, total, raw: sw > 0 ? (num + 50 * PD_PRIOR) / total : null };
}

// ── TEAM TRENDS (Round F4): each team's wRC+ month by month this season ──
// One small request per split (30 teams × ~7 months) from the monthly history
// table; drawn as an inline SVG on a fixed 60–160 scale so lines compare across teams.
const _trendCache = {};
let _trendSeasonP = null;
const _trendHdr = () => ({ apikey: SUPABASE_KEY, Authorization: `Bearer ${SUPABASE_KEY}` });
function trendSeason() {
    if (CUR_SEASON) return Promise.resolve(CUR_SEASON);
    if (!_trendSeasonP) _trendSeasonP = fetch(`${SUPABASE_URL}/rest/v1/fangraphs_splits_history?select=season&order=season.desc&limit=1`, { headers: _trendHdr() })
        .then(r => r.json()).then(d => parseInt(d?.[0]?.season, 10) || null).catch(() => null);
    return _trendSeasonP;
}
function loadTrends(split) {
    if (!_trendCache[split]) _trendCache[split] = (async () => {
        const S = await trendSeason();
        if (!S) throw new Error('no season');
        const r = await fetch(`${SUPABASE_URL}/rest/v1/fangraphs_splits_history?select=tm,date_range,wrcplus&season=eq.${S}&split=eq.${split}&order=date_range.asc`, { headers: _trendHdr() });
        const rows = await r.json();
        if (!Array.isArray(rows)) throw new Error('bad response');
        const by = {};
        rows.forEach(x => { const v = parseFloat(x.wrcplus); if (!isNaN(v) && x.tm) (by[x.tm] = by[x.tm] || []).push({ m: x.date_range, v }); });
        return by;
    })().catch(e => { console.warn('[trends]', e); delete _trendCache[split]; return {}; });
    return _trendCache[split];
}
const TREND_MON = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
function sparkSvg(pts, w = 84, h = 26) {
    if (!pts || pts.length < 2) return '<span class="spark-na">—</span>';
    const meta = statMeta.find(s => s.key === 'wrcPlus');
    const lo = 60, hi = 160, pad = 3;
    const x = i => pad + i * (w - 2 * pad) / (pts.length - 1);
    const y = v => pad + (1 - (Math.max(lo, Math.min(hi, v)) - lo) / (hi - lo)) * (h - 2 * pad);
    const line = pts.map((p, i) => `${i ? 'L' : 'M'}${x(i).toFixed(1)},${y(p.v).toFixed(1)}`).join('');
    const dots = pts.map((p, i) => `<circle class="d-${getTier(meta, p.v)}" cx="${x(i).toFixed(1)}" cy="${y(p.v).toFixed(1)}" r="${i === pts.length - 1 ? 2.6 : 1.7}"/>`).join('');
    const label = pts.map(p => `${TREND_MON[+p.m.slice(5, 7) - 1] || p.m} ${Math.round(p.v)}`).join(' · ');
    return `<svg class="spark" viewBox="0 0 ${w} ${h}" width="${w}" height="${h}" role="img" aria-label="Monthly wRC+: ${label}"><title>Monthly wRC+: ${label}</title>`
        + `<line class="spark-avg" x1="0" x2="${w}" y1="${y(100).toFixed(1)}" y2="${y(100).toFixed(1)}"/><path class="spark-line" d="${line}"/>${dots}</svg>`;
}
// Fill every [data-spark] cell under root once the split's months are in
function fillSparks(root, split, isCurrent, w, h) {
    loadTrends(split).then(by => {
        if (!isCurrent()) return;
        root.querySelectorAll('[data-spark]').forEach(td => { td.innerHTML = sparkSvg(by[td.dataset.spark], w, h); });
    });
}

// ── EARLY-SEASON PRIORS (Round G1) ──────────────────────────────────────
// In April a batter's line is a few days of games. His current-season line leans on
// last season's line (worth PRIOR_PA plate appearances); last season's line itself
// leans on 100 by LAST_SEASON_SHRINK_PA, and rookies with no history start at 100.
// By May it is mostly this season's numbers; on Opening Day mostly last year's.
let _priorIdx = null, _priorP = null;
function loadBatterPriors() {
    if (_priorIdx) return Promise.resolve(_priorIdx);
    if (!_priorP) _priorP = (async () => {
        const S = await trendSeason();
        if (!S) throw new Error('no season');
        const r = await fetch(`${SUPABASE_URL}/rest/v1/fangraphs_player_history?select=playerid,name,wrcplus,pa&season=eq.${S - 1}&split=eq.no_split&period=eq.season`, { headers: _trendHdr() });
        const rows = await r.json();
        if (!Array.isArray(rows)) throw new Error('bad response');
        const byId = {}, byName = {};
        rows.forEach(x => {
            const v = parseFloat(x.wrcplus), pa = +x.pa || 0;
            if (isNaN(v) || !pa) return;
            const o = { wrc: v, pa };
            if (x.playerid != null) byId[String(x.playerid)] = o;
            const k = normalizeName(x.name || '');
            (byName[k] = byName[k] || []).push(o);
        });
        return (_priorIdx = { byId, byName });
    })().catch(e => { console.warn('[batter priors]', e); _priorP = null; return null; });
    return _priorP;
}
function batterPrior(name, playerid) {
    let last = null;
    if (_priorIdx) {
        last = (playerid && _priorIdx.byId[playerid]) || null;
        if (!last) { const c = _priorIdx.byName[normalizeName(name)]; if (c && c.length === 1) last = c[0]; }  // same name twice: no guess
    }
    return last ? { wrc: priorFromLast(last.wrc, last.pa), lastWrc: last.wrc, lastPa: last.pa }
                : { wrc: 100, lastWrc: null, lastPa: 0 };
}
const pctOrdinal = n => { const t = n % 100; return (t >= 11 && t <= 13) ? 'th' : ({ 1: 'st', 2: 'nd', 3: 'rd' }[n % 10] || 'th'); };

// ── MATCHUPS (Round F2): batters vs the opposing starter's hand + edges ──
// A batter's vs-hand split is blended with his overall line, which counts as
// SPLIT_PRIOR_PA plate appearances: 450 PA vs RHP is mostly the split, 34 PA vs
// LHP is mostly the overall line (small splits are noise — regression to the mean).
const battersOf = rec => !rec?.batting_order ? [] : Array.isArray(rec.batting_order) ? rec.batting_order
    : (() => { try { return JSON.parse(rec.batting_order); } catch (e) { return []; } })();
const handSplit = hand => hand === 'L' ? 'vs_lhp' : hand === 'R' ? 'vs_rhp' : null;
const handLabel = hand => hand === 'L' ? 'vs LHP' : hand === 'R' ? 'vs RHP' : 'overall';

// One batter's season wRC+ against this hand, blended with his overall line
function batterVsHand(p, abbr, hand) {
    const name = correctPlayerName(p.name);
    const sp = handSplit(hand);
    const split = sp ? findPlayerStats(name, abbr, `${sp}-season`) : null;
    const all = findPlayerStats(name, abbr, 'no_split-season');
    const ok = v => v && !isNaN(v.wrcPlus) && v.pa > 0;
    const prior = batterPrior(name, (all && all.playerid) || (split && split.playerid));
    // His overall line leans on last season (Round G1); his vs-hand split leans on that
    const curPa = ok(all) ? all.pa : 0;
    const b = blendBatter(curPa ? all.wrcPlus : 0, curPa, ok(split) ? split.wrcPlus : 0, ok(split) ? split.pa : 0, prior.wrc, !!prior.lastPa);
    if (!b) return null;
    return b.fallback ? { wrc: b.wrc, pa: curPa, overall: b.overall, curPa, prior, fallback: true, splitPa: split ? split.pa : 0 }
                      : { wrc: b.wrc, pa: split.pa, raw: split.wrcPlus, overall: b.overall, curPa, prior, fallback: false };
}
// Lineup wRC+ vs this hand, weighted by expected PA per batting-order slot.
// Takes a lineup record (desktop) or its list of batters (phone).
function lineupVsHand(recOrBats, abbr, hand) {
    const bats = Array.isArray(recOrBats) ? recOrBats : battersOf(recOrBats);
    if (!bats.length) return null;
    let w = 0, acc = 0, n = 0; const curPas = []; const fallbacks = [];
    bats.forEach(p => {
        const v = batterVsHand(p, abbr, hand);
        if (!v) return;
        const wt = PA_BY_SLOT[(p.order || 0) - 1] ?? 4.2;
        w += wt; acc += wt * v.wrc; n++; curPas.push(v.curPa || 0);
        if (v.fallback) fallbacks.push(correctPlayerName(p.name));
    });
    return n ? { wrc: Math.round(acc / w), n, total: bats.length, fallbacks, curPas } : null;
}

// Current-season pitcher scores, looked up by name (lineups store names only)
let _pitcherIdx = null, _pitcherIdxPromise = null, _pitcherPrev = null;
function loadPitcherIndex() {
    if (_pitcherIdx) return Promise.resolve(_pitcherIdx);
    if (!_pitcherIdxPromise) _pitcherIdxPromise = (async () => {
        try {
            // Opened straight from a link, the page may not know the season yet:
            // ask pitcher_scores for its newest one rather than sending season=null
            let season = CUR_SEASON;
            if (!season) {
                const q = await fetch(`${SUPABASE_URL}/rest/v1/pitcher_scores?select=season&season=neq.career&order=season.desc&limit=1`, { headers: _trendHdr() });
                season = parseInt((await q.json())?.[0]?.season, 10) || null;
            }
            if (!season) throw new Error('no season');
            const r = await fetch(`${SUPABASE_URL}/rest/v1/pitcher_scores?select=name,team,composite_score,tier,ip&season=eq.${season}`, { headers: _trendHdr() });
            const rows = await r.json();
            const cur = Array.isArray(rows) ? rows : [];
            // Early season (under 20 starters at 100 IP): also load last season's rows,
            // for the starter yardstick and for starters who haven't pitched yet
            if (cur.filter(p => +p.ip >= 100).length < 20) {
                const q2 = await fetch(`${SUPABASE_URL}/rest/v1/pitcher_scores?select=name,team,composite_score,tier,ip&season=eq.${season - 1}`, { headers: _trendHdr() });
                const prev = await q2.json();
                _pitcherPrev = Array.isArray(prev) ? prev : null;
            }
            _pitcherIdx = cur;
        } catch (e) { console.warn('[pitcher index]', e); _pitcherIdxPromise = null; return []; }
        return _pitcherIdx;
    })();
    return _pitcherIdxPromise;
}
// Starters as a group score well above the all-pitcher middle (50), so a starter's
// edge is measured against other starters (100+ IP): their median and spread
function starterScale() {
    if (starterScale._v) return starterScale._v;
    // Until 20 starters reach 100 IP this season (about June), use last season's
    // 100+ IP starters — nearly the same group, scored with their history (Round G1)
    const pool = rows => (rows || []).filter(p => +p.ip >= 100 && p.composite_score != null).map(p => +p.composite_score);
    let v = pool(_pitcherIdx);
    if (v.length < 20) v = pool(_pitcherPrev);
    if (v.length < 20) return { mid: 50, sd: 25 };
    return (starterScale._v = scaleFrom(v));
}
function findPitcherScore(name, team) {
    if (!_pitcherIdx || !name) return null;
    const full = normalizeName(name);
    const last = full.split(' ').slice(-1)[0], fi = full[0];
    const same = p => normalizeName(p.name);
    const look = list => list.find(p => same(p) === full && p.team === team)
        || list.find(p => same(p) === full)
        || list.find(p => same(p).split(' ').slice(-1)[0] === last && same(p)[0] === fi && p.team === team);
    // Early season: a starter who hasn't pitched yet is found in last season's rows
    // (a pitcher's score is shared by all his rows, so it is his current score)
    return look(_pitcherIdx) || (_pitcherPrev ? look(_pitcherPrev) : null) || null;
}
// Edge for one offense: lineup vs the starter's hand (context) + the starter matchup (the label)
// offRec: the offense's lineup record (desktop) or its list of batters (phone)
function offenseEdge(offRec, offAbbr, defRec, defAbbr) {
    const hand = defRec?.pitcher_hand;
    const lv = lineupVsHand(offRec, offAbbr, hand);
    const ps = findPitcherScore(defRec?.pitcher_name, defAbbr);
    if (!lv) return null;
    const sc = starterScale();
    // Label from the starter matchup only (Round G2). The 2026 backtest (EDGES.md) found
    // the lineup part adds nothing to one game's runs, while offenses facing a much
    // better starter scored 3.86 runs vs 4.66 against an average one.
    const z = ps && ps.composite_score != null ? (sc.mid - ps.composite_score) / sc.sd : 0;   // < 0: tougher starter
    const spLast = lastName(defRec?.pitcher_name) || 'starter';
    const label = z <= -STARTER_STRONG ? `Tough matchup: ${offAbbr} bats` : z <= -STARTER_LEAN ? `Slight edge: ${spLast}` : 'No clear edge';
    return { off: offAbbr, def: defAbbr, hand, lv, ps, sp: defRec?.pitcher_name || 'TBD', z, label };
}

// ── PITCHER DRAWER + TIER CARDS (Round F3) ──────────────────────────────
// Click any pitcher name (an element with data-pitcher) to open his drawer;
// hover or tap any tier pill for what that tier means, with backtest numbers
// read from backtest_results.json (rerunning the backtest updates them).
// Each page supplies three adapters: PD_LOGO, PD_COLOR, PD_ENSURE_MATCHUPS.
// The 8 heaviest stats in the score (STAT_WEIGHTS); dir 1 = higher is better
const PD_STATS = [
    { col: 'pitching_plus', label: 'Pitching+', dir: 1,  fmt: v => Math.round(v) },
    { col: 'siera',         label: 'SIERA',     dir: -1, fmt: v => (+v).toFixed(2) },
    { col: 'k_bb_pct',      label: 'K-BB%',     dir: 1,  fmt: v => (v * 100).toFixed(1) + '%' },
    { col: 'stuff_plus',    label: 'Stuff+',    dir: 1,  fmt: v => Math.round(v) },
    { col: 'xera',          label: 'xERA',      dir: -1, fmt: v => (+v).toFixed(2) },
    { col: 'csw_pct',       label: 'CSW%',      dir: 1,  fmt: v => (v * 100).toFixed(1) + '%' },
    { col: 'swstr_pct',     label: 'SwStr%',    dir: 1,  fmt: v => (v * 100).toFixed(1) + '%' },
    { col: 'location_plus', label: 'Location+', dir: 1,  fmt: v => Math.round(v) },
];
const PD_SPLITS = [['vsLHH', 'vs LHH'], ['vsRHH', 'vs RHH'], ['Home', 'Home'], ['Away', 'Away']];
const pdTier = s => s == null ? '' : s >= 90 ? 'Elite' : s >= 75 ? 'Good' : s >= 50 ? 'Mid' : s >= 25 ? 'Bad' : 'Ass';
const pdPill = (tier, txt) => tier ? `<span class="tier-pill tier-${tier.toLowerCase()}">${esc(txt ?? tier)}</span>` : '';
const pdPct = v => v == null || isNaN(v) ? '—' : ((+v > 1 ? +v : +v * 100).toFixed(1) + '%');
const pdAttr = (name, team) => name && name !== 'TBD' ? ` data-pitcher="${esc(name)}" data-pteam="${esc(team || '')}" tabindex="0" role="button"` : '';

function pdGet(path) {
    return fetch(`${SUPABASE_URL}/rest/v1/${path}`, { headers: { apikey: SUPABASE_KEY, Authorization: `Bearer ${SUPABASE_KEY}` } })
        .then(r => r.json()).then(d => Array.isArray(d) ? d : []);
}
let _pdNewest = null;
async function pdNewestSeason() {
    if (!_pdNewest) _pdNewest = pdGet('pitcher_scores?select=season&season=neq.career&order=season.desc&limit=1')
        .then(d => parseInt(d[0]?.season, 10) || CUR_SEASON);
    return _pdNewest;
}
const _pdPools = {};
function pdPool(season) {     // every 20+ IP pitcher of a season, for percentiles
    if (!_pdPools[season]) _pdPools[season] = pdGet(`pitcher_scores?select=${PD_STATS.map(s => s.col).join(',')}&season=eq.${season}&ip=gte.${PD_MIN_IP}`);
    return _pdPools[season];
}

function pdEnsureDom() {
    if (document.getElementById('pd-drawer')) return;
    document.body.insertAdjacentHTML('beforeend', `<div id="pd-scrim" hidden></div>
        <aside id="pd-drawer" role="dialog" aria-modal="true" aria-labelledby="pd-name" hidden>
            <button class="pd-close" type="button" aria-label="Close">×</button><div id="pd-body"></div></aside>`);
    document.getElementById('pd-scrim').addEventListener('click', closePitcherDrawer);
    document.querySelector('#pd-drawer .pd-close').addEventListener('click', closePitcherDrawer);
}
let _pdOpener = null;
function closePitcherDrawer() {
    const d = document.getElementById('pd-drawer');
    if (!d || d.hidden) return;
    d.classList.remove('open'); document.getElementById('pd-scrim').classList.remove('open');
    setTimeout(() => { d.hidden = true; document.getElementById('pd-scrim').hidden = true; }, 180);
    document.documentElement.classList.remove('pd-lock');
    if (_pdOpener && _pdOpener.focus) _pdOpener.focus();
}

async function openPitcherDrawer(name, team, opener) {
    pdEnsureDom();
    _pdOpener = opener || null;
    const d = document.getElementById('pd-drawer'), scrim = document.getElementById('pd-scrim'), body = document.getElementById('pd-body');
    body.innerHTML = `<div class="pd-loading">Loading ${esc(name)}…</div>`;
    d.hidden = scrim.hidden = false;
    requestAnimationFrame(() => { d.classList.add('open'); scrim.classList.add('open'); });
    document.documentElement.classList.add('pd-lock');
    d.querySelector('.pd-close').focus();
    const token = (openPitcherDrawer._t = (openPitcherDrawer._t || 0) + 1);
    try {
        await loadPitcherIndex();
        const hit = findPitcherScore(name, team);
        const canon = hit ? hit.name : name;
        const cols = 'season,team,ip,g,gs,era,fip,k_pct,bb_pct,season_score,composite_score,tier,' + PD_STATS.map(s => s.col).join(',');
        const [rows, newest] = await Promise.all([pdGet(`pitcher_scores?select=${cols}&name=eq.${encodeURIComponent(canon)}`), pdNewestSeason()]);
        if (token !== openPitcherDrawer._t) return;
        if (!rows.length) { body.innerHTML = `<h2 class="pd-name" id="pd-name">${esc(name)}</h2><div class="pd-empty">No pitcher score for ${esc(name)} yet.</div>`; return; }
        const years = rows.map(r => parseInt(r.season, 10)).filter(y => y > 0);
        const S = Math.max(...years);
        const cur = rows.find(r => +r.season === S);
        const [pool, splits] = await Promise.all([pdPool(S),
            pdGet(`pitcher_split_scores?select=split,ip,tbf,woba,k_pct,bb_pct,composite_split_score,split_tier&name=eq.${encodeURIComponent(canon)}&period=eq.${S}`)]);
        if (token !== openPitcherDrawer._t) return;
        body.innerHTML = pdHeader(canon, cur, S) + pdBuilt(rows, newest) + pdStrengths(cur, pool, S) + pdSplitsHtml(splits, S)
            + `<section class="pd-sec" id="pd-tonight"><h3>Next start</h3><div class="pd-empty">Checking lineups…</div></section>`;
        const tn = await pdTonight(canon, name);
        if (token !== openPitcherDrawer._t) return;
        const el = document.getElementById('pd-tonight'); if (el) el.innerHTML = tn;
    } catch (e) {
        console.warn('[pitcher drawer]', e);
        if (token === openPitcherDrawer._t) body.innerHTML = `<div class="pd-empty">Couldn't load ${esc(name)} right now.</div>`;
    }
}

function pdHeader(name, r, S) {
    const comp = r.composite_score != null ? +r.composite_score : null;
    const fact = (v, l) => `<div class="pd-fact"><b>${v}</b><span>${l}</span></div>`;
    return `<div class="pd-head">
        <div class="pd-id">${PD_LOGO(r.team)}<div><h2 class="pd-name" id="pd-name">${esc(name)}</h2>
            <div class="pd-sub"><b style="color:${PD_COLOR(r.team)}">${esc(r.team || '')}</b> · ${S} · ${r.gs > 0 ? `${Math.round(r.gs)} GS` : `${Math.round(r.g || 0)} G`}</div></div></div>
        <div class="pd-score">${comp != null ? `<b class="t-${(r.tier || '').toLowerCase()}">${Math.round(comp)}</b>${pdPill(r.tier)}` : '<span class="pd-empty">unrated</span>'}</div>
    </div>
    <div class="pd-facts">${fact(r.ip != null ? (+r.ip).toFixed(1) : '—', 'IP')}${fact(r.era != null ? (+r.era).toFixed(2) : '—', 'ERA')}${fact(r.fip != null ? (+r.fip).toFixed(2) : '—', 'FIP')}${fact(pdPct(r.k_pct), 'K%')}${fact(pdPct(r.bb_pct), 'BB%')}</div>`;
}

// Mirrors compute_composite_scores: each season's score × its year weight × min(1, IP/60),
// plus an "average pitcher" (50) worth 0.10; the blend is then ranked against active pitchers
function pdBuilt(rows, newest) {
    const { items: list, total, raw } = drawerBlend(rows, newest);
    const comp = rows[0]?.composite_score;
    const row = (lbl, ip, s, w, note) => `<div class="pd-brow">
        <span class="pd-blbl">${lbl}</span><span class="pd-bip">${ip}</span>
        <span class="pd-bbar"><span style="width:${s != null ? Math.max(2, Math.min(100, s)) : 0}%" class="b-${pdTier(s).toLowerCase()}"></span></span>
        <span class="pd-bval">${s != null ? s.toFixed(1) : '—'}</span><span class="pd-bw">${note || (Math.round(w / total * 100) + '%')}</span></div>`;
    return `<section class="pd-sec"><h3>How the score is built</h3>
        <div class="pd-brow pd-bhead"><span>Season</span><span>IP</span><span>Season score</span><span></span><span>Counts</span></div>
        ${list.map(o => row(o.y ? o.y : 'Career', o.r.ip != null ? Math.round(+o.r.ip) : '—', o.s, o.w,
            o.s == null ? `<i>&lt;${PD_MIN_IP} IP</i>` : o.w === 0 ? '<i>too old</i>' : '')).join('')}
        ${row('Average', '', 50, PD_PRIOR)}
        <p class="pd-note">${raw != null ? `Weighted blend <b>${raw.toFixed(1)}</b> → ranks <b>${comp != null ? Math.round(comp) : '—'}</b> among active pitchers → ${pdPill(pdTier(comp))}. ` : ''}
        A season counts fully from ${PD_FULL_IP} IP; the "Average" row pulls thin records toward 50.</p></section>`;
}

function pdStrengths(r, pool, S) {
    const items = PD_STATS.map(st => {
        const v = r[st.col];
        if (v == null || isNaN(+v)) return null;
        const vals = pool.map(p => p[st.col]).filter(x => x != null && !isNaN(+x)).map(Number);
        if (vals.length < 20) return null;
        const below = vals.filter(x => st.dir > 0 ? x < +v : x > +v).length, ties = vals.filter(x => x === +v).length;
        return { st, v: +v, pct: Math.max(1, Math.min(99, Math.round((below + ties / 2) / vals.length * 100))), n: vals.length };
    }).filter(Boolean);
    if (!items.length) return '';
    return `<section class="pd-sec"><h3>Strengths &amp; weaknesses</h3>
        ${items.map(o => `<div class="pd-srow"><span class="pd-slbl">${o.st.label}</span><span class="pd-sval">${o.st.fmt(o.v)}</span>
            <span class="pd-bbar"><span style="width:${Math.max(2, o.pct)}%" class="b-${pdTier(o.pct).toLowerCase()}"></span></span>
            <span class="pd-spct t-${pdTier(o.pct).toLowerCase()}">${o.pct}<small>${pctOrdinal(o.pct)}</small></span></div>`).join('')}
        <p class="pd-note">Percentile among the ${items[0].n} pitchers with ${PD_MIN_IP}+ IP in ${S} (higher = better, so a low ERA-type stat scores high). These are the 8 stats that weigh most in the score.</p></section>`;
}

function pdSplitsHtml(splits, S) {
    const by = Object.fromEntries(splits.map(s => [s.split, s]));
    const rows = PD_SPLITS.filter(([k]) => by[k]).map(([k, lbl]) => {
        const s = by[k];
        const sc = s.composite_split_score != null ? +s.composite_split_score : null;
        return `<div class="pd-prow"><span class="pd-slbl">${lbl}</span><span>${s.ip != null ? (+s.ip).toFixed(1) : '—'} <small>IP</small></span>
            <span>${s.woba != null ? (+s.woba).toFixed(3).replace(/^0/, '') : '—'} <small>wOBA</small></span><span>${pdPct(s.k_pct)} <small>K</small></span>
            <span class="pd-pp">${sc != null ? pdPill(s.split_tier || pdTier(sc), `${s.split_tier || pdTier(sc)} ${Math.round(sc)}`) : '—'}</span></div>`;
    });
    return `<section class="pd-sec"><h3>Splits · ${S}</h3>${rows.length ? rows.join('') : '<div class="pd-empty">No split data</div>'}
        <p class="pd-note">wOBA allowed (lower is better). Split scores rank him among pitchers in the same split.</p></section>`;
}

async function pdTonight(canon, shown) {
    try { await PD_ENSURE_MATCHUPS(); } catch (e) { return '<div class="pd-empty">Lineups unavailable right now</div>'; }
    const same = n => n && (normalizeName(n) === normalizeName(canon) || normalizeName(n) === normalizeName(shown));
    for (const { away, home } of pairLineupGames(lineupGames)) {
        const mine = same(away?.pitcher_name) ? away : same(home?.pitcher_name) ? home : null;
        if (!mine) continue;
        const opp = mine === away ? home : away;
        if (!opp) break;
        const e = offenseEdge(opp, opp.team, mine, mine.team);
        const d = mine.game_date || opp.game_date;
        const when = d === localDateStr() ? 'Today' : (d ? fmtDay(d) : '');
        const lv = e && e.lv;
        return `<div class="pd-next"><div class="pd-sub">${esc(when)}${mine.game_time ? ' · ' + esc(mine.game_time) : ''} · ${mine === away ? '@' : 'vs'}
                ${PD_LOGO(opp.team)}<b style="color:${PD_COLOR(opp.team)}">${esc(opp.team)}</b></div>
            ${lv ? `<div class="pd-srow"><span class="pd-slbl">${esc(opp.team)} lineup ${mine.pitcher_hand === 'L' ? 'vs LHP' : mine.pitcher_hand === 'R' ? 'vs RHP' : ''}</span>
                <span class="pd-sval">${lv.wrc}</span><span class="pd-bbar"><span style="width:${Math.max(4, Math.min(100, (lv.wrc - 60) / 0.9))}%" class="b-${getWrcTier(lv.wrc)}"></span></span>
                <span class="pd-spct">wRC+</span></div>` : ''}
            ${e ? `<div class="pd-edge">${esc(e.label)}</div>` : ''}
            <p class="pd-note">From the latest posted lineups. Lineup = wRC+ vs his hand (100 = average).</p></div>`;
    }
    return '<div class="pd-empty">Not in the latest posted lineups.</div>';
}
const getWrcTier = v => v >= 120 ? 'elite' : v >= 110 ? 'good' : v >= 100 ? 'mid' : v >= 90 ? 'bad' : 'ass';

// ── Tier cards ──
let _btPromise = null;
const loadBacktest = () => _btPromise || (_btPromise = fetch('backtest_results.json').then(r => r.ok ? r.json() : null).catch(() => null));
const PD_TIER_RANGE = { Elite: '90–100 · top 10%', Good: '75–89 · next 15%', Mid: '50–74 · middle 25%', Bad: '25–49 · next 25%', Ass: 'under 25 · bottom 25%' };
function tierCardHtml(tier, bt) {
    let out = `<b class="tt-h">${tier}</b><div class="tt-range">Pitcher score ${PD_TIER_RANGE[tier]} of active pitchers</div>`;
    const s = bt?.season_test?.tiers_RA9 || [], g = bt?.game_test?.tiers || [];
    const st = s.find(t => t.tier === tier), gt = g.find(t => t.tier === tier);
    if (st) {
        const ip = s.reduce((a, t) => a + t.ip, 0), avg = s.reduce((a, t) => a + t.ra9 * t.ip, 0) / ip;
        out += `<div class="tt-bt">Backtest ${bt.seasons[0]}–${bt.seasons[bt.seasons.length - 1]}: preseason ${tier} pitchers allowed <b>${st.ra9.toFixed(2)}</b> runs per 9 (all tiers: ${avg.toFixed(2)})`
            + (gt ? `; teams allowed <b>${gt.team_ra_per_game.toFixed(2)}</b> runs a game behind a ${tier} starter.` : '.') + '</div>';
        const bad = s.find(t => t.tier === 'Bad'), ass = s.find(t => t.tier === 'Ass');
        if ((tier === 'Bad' || tier === 'Ass') && bad && ass && ass.ra9 <= bad.ra9)
            out += `<div class="tt-warn">The backtest doesn't separate Bad from Ass (${bad.ra9.toFixed(2)} vs ${ass.ra9.toFixed(2)}) — only the Ass pitchers teams keep using are in it.</div>`;
    }
    return out;
}
function pdShowTip(pill) {
    const m = /tier-(elite|good|mid|bad|ass)/.exec(pill.className);
    if (!m) return;
    const tier = m[1][0].toUpperCase() + m[1].slice(1);
    let tip = document.getElementById('tier-tip');
    if (!tip) { tip = document.createElement('div'); tip.id = 'tier-tip'; tip.setAttribute('role', 'tooltip'); document.body.appendChild(tip); }
    tip._pill = pill;
    loadBacktest().then(bt => {
        if (tip._pill !== pill) return;
        tip.innerHTML = tierCardHtml(tier, bt);
        tip.className = 'show tt-' + m[1];
        const r = pill.getBoundingClientRect(), w = tip.offsetWidth, h = tip.offsetHeight;
        const left = Math.max(8, Math.min(innerWidth - w - 8, r.left + r.width / 2 - w / 2));
        const top = r.bottom + 8 + h > innerHeight && r.top - 8 - h > 0 ? r.top - 8 - h : r.bottom + 8;
        tip.style.left = left + 'px'; tip.style.top = top + 'px';
    });
}
function pdHideTip() { const t = document.getElementById('tier-tip'); if (t) { t.className = ''; t._pill = null; } }
const PD_TOUCH = matchMedia('(hover: none)').matches;
document.addEventListener('mouseover', e => { if (PD_TOUCH) return; const p = e.target.closest('.tier-pill'); if (p) pdShowTip(p); });
document.addEventListener('mouseout', e => { if (PD_TOUCH) return; const p = e.target.closest('.tier-pill'); if (p && !p.contains(e.relatedTarget)) pdHideTip(); });
document.addEventListener('scroll', pdHideTip, true);
// Capture phase: a pill or name inside a clickable card opens its own thing, not the card's
document.addEventListener('click', e => {
    const pill = e.target.closest('.tier-pill');
    if (pill && PD_TOUCH) {
        e.preventDefault(); e.stopPropagation();
        const t = document.getElementById('tier-tip');
        if (t && t._pill === pill && t.classList.contains('show')) pdHideTip(); else pdShowTip(pill);
        return;
    }
    if (!pill) pdHideTip();
    const who = e.target.closest('[data-pitcher]');
    if (who) { e.preventDefault(); e.stopPropagation(); openPitcherDrawer(who.dataset.pitcher, who.dataset.pteam, who); }
}, true);
document.addEventListener('keydown', e => {
    if (e.key === 'Escape') { pdHideTip(); closePitcherDrawer(); }
    if ((e.key === 'Enter' || e.key === ' ') && e.target.matches && e.target.matches('[data-pitcher]')) {
        e.preventDefault(); openPitcherDrawer(e.target.dataset.pitcher, e.target.dataset.pteam, e.target);
    }
});

// ── KEYBOARD ACCESS (Round G4) ──────────────────────────────────────────
// Older tables and cards are clicked through onclick on <th>/<div>/<span> (or a
// delegated listener). This gives each such element a Tab stop and makes Enter /
// Space click it, and lets sortable headers announce their sort order. It re-runs
// whenever a table redraws, so new rows are covered without touching each table.
const A11Y_CLICKABLE = [
    '[onclick]:not(button):not(a):not(input):not(select):not(textarea):not(label):not(option)',
    '[data-pid]:not([data-pid=""])',            // batter names → player card
    '.pr-suggest-item',                          // pitcher-compare suggestions
].join(', ');
function a11yEnhance(root) {
    root.querySelectorAll(A11Y_CLICKABLE).forEach(el => {
        if (!el.hasAttribute('tabindex')) el.setAttribute('tabindex', '0');
        if (el.tagName !== 'TH' && el.tagName !== 'TR' && !el.hasAttribute('role')) el.setAttribute('role', 'button');
    });
    root.querySelectorAll('th[onclick]').forEach(th => {
        const t = th.textContent;
        if (th.classList.contains('sort-active') && /[↓↑]/.test(t)) th.setAttribute('aria-sort', t.includes('↑') ? 'ascending' : 'descending');
        else th.removeAttribute('aria-sort');
    });
}
document.addEventListener('DOMContentLoaded', () => {
    a11yEnhance(document);
    // Observer callbacks already arrive batched; it only watches added/removed nodes,
    // so the attributes set here don't re-trigger it
    new MutationObserver(() => a11yEnhance(document)).observe(document.body, { childList: true, subtree: true });
});
document.addEventListener('keydown', e => {
    if (e.key !== 'Enter' && e.key !== ' ') return;
    const el = e.target;
    if (!el || !el.matches || !el.matches(A11Y_CLICKABLE) || el.matches('[data-pitcher]')) return;   // the drawer has its own
    e.preventDefault();
    el.click();
});
