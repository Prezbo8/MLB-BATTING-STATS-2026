# Opening Day reminder

**On Opening Day 2027 (Thu, Mar 25): work on the pitcher score.**

Just a note for now — nothing has been decided about what to change.

## What happens on its own (`season_start.yml`)

- **The day after Opening Day (2027: Fri, Mar 26):** the six daily workflows are re-enabled,
  the keeper is started, and a GitHub issue + Telegram message remind you of the pitcher-score
  work above (this note is the issue's text).
- **A week later (2027: Fri, Apr 2):** the schedule report checks that every job started on time,
  the browser-test data is re-recorded from live data and committed if the browser test passes on
  it, and the results arrive as a GitHub issue + Telegram message.
- **All winter:** `keepalive.yml` stops GitHub from switching scheduled workflows off after 60
  quiet days (it re-enables them, and makes a tiny commit when the repo has been quiet 45 days).

Opening Day comes from MLB's API, so this repeats every season with no dates to edit. To see what
it would do at any time: `gh workflow run season_start.yml -f stage=start` (dry run by default).

**Pitcher scores:** the Round H3 fix is already live (one manual pipeline run on Nov 17, 2026):
the 2021–25 files carry player ids, Cleveland's Logan Allen is scored with his full history
(26.4 → 11.8) and every older `pitcher_scores` row has an id.
