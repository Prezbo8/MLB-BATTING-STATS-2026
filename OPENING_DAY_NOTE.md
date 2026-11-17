# Opening Day reminder

**On Opening Day 2027 (Thu, Mar 25): work on the pitcher score.**

Just a note for now — nothing has been decided about what to change.

**Also after the unpause (Mar 26):** re-enabling the six daily workflows is enough — the keeper
(`keeper.yml`) starts on its own within ~6 hours (or start it right away with
`gh workflow run keeper.yml`). A week later, run `python3 scripts/schedule_report.py` to check the
jobs start on time.

**Pitcher scores:** the Round H3 fix is already live (one manual pipeline run on Nov 17, 2026):
the 2021–25 files carry player ids, Cleveland's Logan Allen is scored with his full history
(26.4 → 11.8) and every older `pitcher_scores` row has an id.
