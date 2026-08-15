// Daily report → Telegram: renders the live Daily Report page with headless
// Chromium, selects each game, prints it to PDF, and sends one document per
// game to Telegram. Reuses the deployed site so no report logic is duplicated.
//
// Env: TELEGRAM_TOKEN, TELEGRAM_CHAT_ID  (secrets)
//      REPORT_URL   (optional override)
//      DRY_RUN=1    (save PDFs to ./report_pdfs/ instead of sending)
const fs = require('fs');
const { chromium } = require('playwright');

const TOKEN   = process.env.TELEGRAM_TOKEN;
const CHAT_ID = process.env.TELEGRAM_CHAT_ID;
const DRY_RUN = process.env.DRY_RUN === '1' || !TOKEN;
const REPORT_URL = process.env.REPORT_URL
  || 'https://prezbo8.github.io/MLB-BATTING-STATS-2026/index.html#tab=8';

const sleep = ms => new Promise(r => setTimeout(r, ms));

async function tgDocument(pdf, filename, caption) {
  const form = new FormData();
  form.append('chat_id', CHAT_ID);
  form.append('caption', caption);
  form.append('parse_mode', 'HTML');
  form.append('document', new Blob([pdf], { type: 'application/pdf' }), filename);
  const res = await fetch(`https://api.telegram.org/bot${TOKEN}/sendDocument`, { method: 'POST', body: form });
  if (!res.ok) throw new Error(`Telegram ${res.status}: ${(await res.text()).slice(0, 200)}`);
}

async function tgMessage(text) {
  if (DRY_RUN) { console.log('[dry-run msg]', text); return; }
  await fetch(`https://api.telegram.org/bot${TOKEN}/sendMessage`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ chat_id: CHAT_ID, text, parse_mode: 'HTML' }),
  }).catch(e => console.error('tgMessage failed', e.message));
}

(async () => {
  console.log(DRY_RUN ? '● DRY RUN (no Telegram)' : '● Live send', '→', REPORT_URL);
  const browser = await chromium.launch();
  const page = await browser.newPage({
    viewport: { width: 1440, height: 1000 },
    timezoneId: 'America/New_York',   // so the page's "today" is the ET slate
    locale: 'en-US',
  });
  page.setDefaultTimeout(60000);
  await page.goto(REPORT_URL, { waitUntil: 'load' });

  // Wait for team data + the schedule dropdown to be ready.
  await page.waitForFunction(
    () => typeof allData !== 'undefined' && Object.keys(allData).length > 0
          && document.getElementById('mlbGameDropdown')?.options.length > 1,
    { timeout: 90000 },
  );

  const games = await page.evaluate(() =>
    Array.from(document.getElementById('mlbGameDropdown').options)
      .filter(o => o.value).map(o => ({ value: o.value, label: o.textContent.trim() })));
  const dateStr = await page.evaluate(() =>
    (typeof localDateStr === 'function' ? localDateStr() : new Date().toISOString().slice(0, 10)));
  console.log(`Found ${games.length} game(s) for ${dateStr}`);

  if (!games.length) {
    await tgMessage(`📭 No MLB games scheduled for ${dateStr}.`);
    await browser.close();
    return;
  }

  if (DRY_RUN) fs.mkdirSync('report_pdfs', { recursive: true });
  let ok = 0;
  for (const g of games) {
    try {
      await page.evaluate(v => handleGameSelect(v), g.value);
      // Wait for exactly this one card, fully rendered (its stat tables exist).
      await page.waitForFunction(() => {
        const cards = document.querySelectorAll('#report-container .report-game-card');
        return cards.length === 1 && cards[0].querySelectorAll('table').length > 0;
      }, { timeout: 45000 });
      await sleep(1200); // let logos/tables settle before printing
      // Mirror the site's "Save as PDF": hide the app chrome, print only the
      // report card (already filtered to this one game), keep the dark theme.
      await page.evaluate((matchup) => {
        document.getElementById('print-report-root')?.remove();
        document.getElementById('print-report-style')?.remove();
        const style = document.createElement('style');
        style.id = 'print-report-style';
        style.textContent =
          '#print-report-root{display:none;}@media print{' +
          '@page{size:landscape;margin:0.25in;}' +
          'body{background:#080a0f !important;}' +
          'body > *:not(#print-report-root){display:none !important;}' +
          '#print-report-root{display:block !important;background:#080a0f;color:#e8eaf0;}' +
          // The card is taller than a page; override the site print rule that
          // would push the whole (avoid-break) card to a fresh page.
          '#print-report-root .report-game-card{break-inside:auto !important;page-break-inside:auto !important;margin:0 !important;}' +
          '#print-report-root tr{break-inside:avoid;page-break-inside:avoid;}' +
          '*{-webkit-print-color-adjust:exact !important;print-color-adjust:exact !important;}}';
        document.head.appendChild(style);
        const root = document.createElement('div');
        root.id = 'print-report-root';
        const title = document.createElement('div');
        title.style.cssText = 'font-family:system-ui,sans-serif;font-size:15px;font-weight:900;color:#e8eaf0;margin-bottom:10px;letter-spacing:0.04em;';
        title.textContent = '📰 MLB 2026 Daily Report — ' + matchup;
        root.appendChild(title);
        const clone = document.getElementById('report-container').cloneNode(true);
        clone.querySelectorAll('.export-png-btn').forEach(b => b.remove());
        root.appendChild(clone);
        document.body.appendChild(root);
      }, g.label);
      const pdf = await page.pdf({
        format: 'A3', landscape: true, printBackground: true,
        margin: { top: '10mm', bottom: '10mm', left: '8mm', right: '8mm' },
      });
      await page.evaluate(() => {
        document.getElementById('print-report-root')?.remove();
        document.getElementById('print-report-style')?.remove();
      });
      const fname = `report_${dateStr}_${g.value}.pdf`;
      const caption = `⚾ <b>${g.label}</b> — ${dateStr}`;
      if (DRY_RUN) {
        fs.writeFileSync(`report_pdfs/${fname}`, pdf);
        console.log(`  ✓ saved ${fname} (${(pdf.length / 1024).toFixed(0)} KB)`);
      } else {
        await tgDocument(pdf, fname, caption);
        console.log(`  ✓ sent ${g.label}`);
        await sleep(1600); // stay under Telegram per-chat rate limit
      }
      ok++;
    } catch (e) {
      console.error(`  ✗ ${g.value}: ${e.message}`);
      await tgMessage(`⚠️ Report PDF failed for ${g.label} (${dateStr}): ${e.message}`);
    }
  }
  console.log(`Done: ${ok}/${games.length} game(s).`);
  await browser.close();
  if (!ok) process.exit(1);
})().catch(e => { console.error('FATAL', e); process.exit(1); });
