# One week of upgrades — Nov 11–17, 2026

A compacted log of the improvement series on the MLB model (site, scrapers, pitcher score,
Telegram bot), PRs **#1 → #35**. It started from an outside audit by Fable 5.2 and went round by
round. Each round was explained before it was built, built on a branch, checked by CI and merged
only when green. The season is paused (Oct 1 2026 → Mar 26 2027), so all of this was built in
the offseason and is ready for Opening Day 2027.

**Report card (Fable 5.2 audits):** C+ → B- → B+ (after Round D) → B+ (after F4) →
low A- (after Round G) → **solid A-** (after Round H). Every item on the final "to reach A" list
is done, except the checks that can only happen in the first live week (automated, see Round J).

**By the numbers:**
- 35 PRs merged.
- 0 tests → **226 tests** (Python, a real browser, JavaScript-vs-Python, the keeper loop).
- 4 seasons of game-by-game backtests.
- The home page download went from 4.6 MB to 1.4 MB.

---

## Day 1 (Nov 11): the audit fixes — PRs #1–#8

| PR | What changed |
|---|---|
| #1 | **Pitcher scores need a minimum sample before they're ranked**; all seasons are refreshed daily, not just the newest |
| #2 | **Fail loudly:** Supabase tables are swapped in one transaction (never half-written); workflows go red on real failures and stop going red on false ones |
| #3 | **Season rollover (scrapers):** the season comes from MLB's Opening Day; no year hardcoded in any script |
| #4 | **Season rollover (site, phone page, Telegram bot, R/G columns)**, so 2027 needs no edits |
| #5 | The results scraper moved into the repo, built on **MLB's official API** |
| #6 | Pipeline cleanup: FanGraphs retries, one commit per run, the chain-B fix, pinned package versions |
| #7 | Site polish: one pitcher hand, the real lineup date, safer name matching, fonts, nav |
| #8 | **Security:** the bot fails closed without its secret; SQL makes the public key read-only |

## Rounds A–E (Nov 12–13): data, scoring, tests, proof

| PR | What changed |
|---|---|
| #9 Round A | History scrapers swap atomically; unpause date set to **the day after Opening Day** (2027: Mar 26); README pause/unpause steps |
| #10 | Monthly history stops at the last regular-season day |
| #11 Round B | **Pitcher scores that work from Opening Day:** composites blend toward an average pitcher (prior weight 0.10) instead of jumping on 5 innings; sample-scaled season weights; split pools comparable to each other |
| #12 Round C | **An offline test suite + CI**; tests must pass to merge |
| #13 Round D | **Backtest: does the pitcher score predict?** (BACKTEST.md) |
| #14 Round E | Tiers ranked among **active** pitchers; safer season fallback when MLB's API is down; a rehearsal input for the pipeline |

## Round F (Nov 13): the new look

| PR | What changed |
|---|---|
| #15 F1 | **ESPN-style layout:** top nav, scoreboard, sidebar, Today view, dark/light theme |
| #16 F2 | Matchup chips, starter pills, **today's edges** (which offenses face a tough starter) |
| #17 F3 | **Pitcher drawer** (how the score is built, season by season) + tier pills that explain themselves on hover/tap |
| #18, #20 | Team Rankings names: first city over nickname, then (by request) **logo + team name only**, centered |
| #19 F4 | **Team trend lines** (wRC+) + the ticker grouped into Leaders / Hot / Cold / Today |

## Round G (Nov 13–14): correct in April, tested, accessible, faster

| PR | What changed |
|---|---|
| #21 G1 | **April-correct matchups:** early in the season, batters lean on last season's line (shrunk toward average) instead of 20 plate appearances |
| #22 G2 | Backtest of Today's edges on 2026; labels come from the starter matchup (what the data supported) |
| #23 G3 | **One shared.js** for the matchup/drawer math, with tests that run it in Node against the Python, so the site and model can't drift apart |
| #24 G4 | **Accessibility basics** on both pages: keyboard reach, labels, skip link, ticker pause button, readable contrast |
| #25 G5 | **Download only what's on screen** (home page 4.6 MB → 1.4 MB); pitchers matched by **player id**, not name (two Logan Allens no longer share numbers) |

## Round H (Nov 16–17): runs on time, proven by data

| PR | What changed |
|---|---|
| #26 H1 | **The keeper:** GitHub's cron fires late or not at all, so a keeper workflow starts every daily job at its Eastern-time slot (5:30 scores · 6:00 pipeline · 6:30 team · 7:00 player · 7:30 R/G · 11:00 report · lineups hourly 11–9). It hands off to a fresh keeper before GitHub's time limit, has backup crons that skip if the job already ran, and pauses itself in the offseason |
| #27 H2 | Edge labels use the plain starter-score cut-offs the backtest graded: **Tough 90+, Slight 73.5–90** |
| #28 H3 | **Player ids for the 2021–25 pitcher files** (45 files): Cleveland's Logan Allen is now scored with his own full history (26.4 → 11.8) |
| #29 H4 | **A real browser test in CI:** Chromium walks every desktop tab and phone panel on recorded data with a frozen clock, checking JS errors, error messages, sideways scroll, keyboard reach, control names, the drawer, tier cards, download size |
| #30 H5 | **Edges backtest on 2023–2026** with in-season starter scores rebuilt month by month and ballparks taken out (EDGES.md) |
| #31 | **Keeper loop test:** CI runs the real keeper script against a fake GitHub CLI (starts, skips paused jobs, retries, hand-off) |
| #32 | Keeper dry run checks the slot even in the offseason, then hands off for real; **proven on GitHub** (run 44009797064 started a fresh keeper by itself) |

Also live: the pitcher pipeline was run once by hand (run 44010189550) to publish the H3 fix;
none of the 1,868 older `pitcher_scores` rows lacks a player id.

## Round I (Nov 17): the last open questions

**#33: Is the pitcher score better than SIERA? Do split scores help?** The test used the same
14,768 team-games from 2023–2026, with each measure as known before the game.

| Starter measure | Predicts runs allowed |
|---|---|
| **Pitcher score, in-season** (what the site shows) | **0.123** |
| Split score weighted to the actual lineup's lefties/righties | 0.113 |
| Pitcher score, Opening Day | 0.112 |
| Split score, home or away | 0.107 |
| SIERA, last season | 0.102 |
| SIERA, season to date | 0.094 |

- **The pitcher score beats SIERA:** +0.030 in-season, with the whole 95% range above zero.
- **Split scores add nothing** over the overall score, so the site keeps the overall score.

**#35: the Leaders box matches the table.** The side Leaders box and Today's "Top pitchers"
showed the multi-season composite, so everyone read 99–100. They now show **this season's score**,
the same number as the Pitcher Rankings table (Sale 93.9, Misiorowski 93.8, Skubal 92.8).
- Minimum innings: **20 IP in April–May, 40 IP from June on.**
- The composite stays the model's number (edges, pills, drawer).
- The browser test now runs on Eastern time. A recording made at 11 PM Eastern had replayed as "tomorrow" on GitHub's UTC machines.

## Round J (Nov 17): Opening Day runs itself — PR #34

- **`season_start.yml`**, the day after Opening Day (2027: **Fri Mar 26**): re-enables the six daily jobs, starts the keeper, and sends a GitHub issue + Telegram reminder to **work on the pitcher score**.
- **A week later (Fri Apr 2):** a schedule report (did every job start on time?), plus a re-record of the browser-test data, committed only if the browser test passes on it. The results arrive as an issue + Telegram message.
- Opening Day comes from MLB's API, so it repeats every season. It never acts outside 30 days of Opening Day.
- **`keepalive.yml`:** GitHub disables a public repo's scheduled workflows after 60 days without commits. The keepalive re-enables them (never the paused six) and makes a one-line commit after 45 quiet days.
- **Both stages were dry-run on GitHub** (runs 44017335514 and 44017584955): every step was shown and nothing was changed.

---

## What the backtests say, in one place

- **Starter labels predict runs every season.** Offenses facing a Tough-matchup starter (90+) scored **3.81** runs a game, Slight 4.30, others 4.69 (league 4.48). By season, Tough/Slight/none: 2023 4.03/4.33/4.86 · 2024 3.81/4.11/4.59 · 2025 3.69/4.32/4.66 · 2026 3.69/4.45/4.68.
- **The lineup matters a little, far less than the starter.** Within the same game it predicts 0.061 vs the starter's 0.129. So the label uses the starter only, and lineup wRC+ is shown as context.
- **The pitcher score beats SIERA** on game outcomes (Round I). The season-level test in BACKTEST.md had called it a tie; 14,768 starts make the difference visible.

## Bugs caught along the way (selected)

- A deep link loaded the pitcher index with no season → the newest season is looked up first.
- Unsorted phone pagination under concurrency dropped a batter (Spencer Jones) → every phone fetch is sorted.
- Same-name pitchers borrowed each other's split numbers → keyed by player id.
- The site's edge yardstick (72.2) didn't match the backtest's (62.5) → plain-score cut-offs.
- Ubuntu 26.04 can't install Playwright's Chromium → CI pinned to ubuntu-24.04, with a test enforcing it.
- The accessibility audit had blind spots (clipped phone content, cursor-only "clickables") → fixed and found 3 fake-clickable phone headers.
- MLB API timeouts in the backtest → retries with backoff.

## What's next

- **Mar 25, 2027 (Opening Day):** pitcher-score work (reminder arrives automatically Mar 26).
- **Mar 26:** the season turns itself back on.
- **Apr 2:** the first-week check reports in.

Details: `OPENING_DAY_NOTE.md`, `BACKTEST.md`, `EDGES.md`, `README.md`.
