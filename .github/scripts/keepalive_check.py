"""Should keepalive.yml make a commit? (pure; tested)

GitHub disables a public repo's scheduled workflows after 60 days without repository activity
(commits). The offseason has none, so the keeper's restart schedule, the scores job and the
season-start schedule would all be switched off before March. keepalive.yml re-enables every
scheduled workflow that isn't paused on purpose, and when the newest commit is QUIET_DAYS old it
makes one small commit (.github/keepalive) to reset GitHub's 60-day clock.

  python3 .github/scripts/keepalive_check.py <newest commit ISO time>  -> prints "commit=true|false"
"""
import sys
from datetime import datetime, timedelta, timezone

QUIET_DAYS = 45


def needs_commit(last_commit, now):
    return now - last_commit >= timedelta(days=QUIET_DAYS)


if __name__ == "__main__":
    last = datetime.fromisoformat(sys.argv[1].replace("Z", "+00:00"))
    print(f"commit={'true' if needs_commit(last, datetime.now(timezone.utc)) else 'false'}")
