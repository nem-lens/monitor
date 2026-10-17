# Hosting on GitHub Actions and Pages

The monitor can run without a server: `.github/workflows/pages.yml` runs
`nem-monitor once` every five minutes, which loads the previous state from
the live site, polls NEMWeb once, rebuilds the site and deploys it to Pages.
Deploying adds no commits, so the repository does not grow.

## Set up

1. Make the repository public. Pages on private repositories needs a paid
   plan, and a five-minute schedule uses about 8,600 runner-minutes a month,
   which is free only on public repositories.
2. **Settings → Pages → Source: GitHub Actions.**
3. Optional repository variables (**Settings → Secrets and variables →
   Actions → Variables**):
   - `NTFY_TOPIC`: an ntfy topic for phone push.
   - `ALERTS_CATEGORY`: a Discussions category for alerts, for example
     `Alerts`. Create it first (**Discussions → categories**) as an
     announcement-type category, so only maintainers and the workflow can
     start threads.
4. While `nem-lens/aemo` is private, add a secret `NEM_LENS_READ_TOKEN` with
   read access to it.
5. Run **Actions → Pages → Run workflow** once. The first run finds no
   previous state and starts fresh; later runs continue from
   `<site>/data/state.json`.

Before relying on it, check from a workflow run that GitHub-hosted runners
can reach nemweb.com.au. The daily `nemweb-canary.yml` workflow does this.

## What the site holds

| Path | Contents |
| --- | --- |
| `index.html` | The public page in static mode: reads `data/*.json`, refreshes every 5 minutes, no live stream or webhook form |
| `data/status.json`, `data/events.json`, `data/events/<id>.json`, `data/notifications.json` | The JSON API's documents |
| `feed.atom`, `feed.json` | Feeds |
| `data/state.json` | State for the next run: detector state, events, public notifications, 48 hours of dispatch outcomes, next-day data seen |
| `data/github.json` | The Discussion for each event and the notifications already posted |

The state is public with the site. It holds public notifications only;
internal ones (opened, discarded) are not exported. The detector state does
show developing events, which are AEMO's public prices in any case.

If the state cannot be fetched (anything except a 404), the run fails rather
than starting fresh, so a transient error cannot re-announce recent events.

## Subscribing

- **Watch → Custom → Discussions** on the repository: a GitHub notification
  when an event is confirmed (a new Discussion) and on each update (a
  comment). Releases (**Watch → Custom → Releases**) are planned for the
  morning-after briefs once `nem-report` exists.
- The Atom and JSON feeds, and ntfy if `NTFY_TOPIC` is set.

## Discussions posting

`scripts/github_discussions.py` runs after `once` and before the deploy. It
creates one Discussion per event on its first public notification and
comments for later ones. Each post carries a hidden marker, so if a deploy
fails after posting, the next run finds the earlier posts rather than
posting twice. On the first run it records existing notifications as posted
without posting them; use `--post-existing` to post history.

Test it without touching GitHub:

```sh
python3 scripts/github_discussions.py --site site --previous https://<owner>.github.io/<repo> \
  --repo <owner>/<repo> --category Alerts --dry-run
```

## Testing the workflow end to end

Run the workflow with **fixture: true** on a test repository (not the live
site). It replays the synthetic 12 October fixture instead of polling and
should produce one Discussion with update comments; rerunning it posts
nothing new, because the replayed intervals are older than the saved state.

## Keeping the schedule alive

GitHub disables scheduled workflows after 60 days without repository
activity. `keepalive.yml` re-enables the scheduled workflows through the API
on the 1st of each month, which resets that clock without commits.

## Limits

- Scheduled runs are best effort: GitHub may delay or skip runs at busy
  times, so alerts can arrive later than from an always-on server.
- Event bundles are not built in this mode: runs have no disk between them,
  so the raw DispatchIS buffer does not persist. Use an always-on server for
  bundles.
- ntfy pushes are sent before the deploy; a run that fails after pushing can
  push again on the next run.
