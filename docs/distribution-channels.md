# Distribution and hosting options: pros and cons

*Prepared 17 October 2026 for future discussion. Prices and platform policies change often, so verify the items marked "check" before committing.*

## Summary

The monitor already supports a public web page, Atom and JSON feeds, ntfy push, a live stream and signed webhooks. For broader reach, the recommended next channels are Bluesky and Telegram (free, no approval step), followed by X if the target audience is active there. GitHub Watch works well for a technical audience on the GitHub Actions hosting option. Reddit, LinkedIn and Threads are better handled manually.

For hosting, a trial on a work machine is safest with the monitor bound to localhost and webhooks disabled. A public service is better run either on a small VPS outside any work network or as a GitHub Actions plus Pages deployment.

## Hosting options

| Option | Pros | Cons |
| --- | --- | --- |
| **Laptop, localhost only** (trial) | Fastest to start; no inbound exposure; alerts within about a minute | Stops when the laptop sleeps (catches up for 6 hours on wake); page only visible on that machine |
| **Small VPS** with Caddy | Always on; full feature set including webhooks and the live stream; isolated from work networks | Small monthly cost; someone maintains the server |
| **Option 1: GitHub Actions + Pages** | No server at all; free on a public repo; GitHub handles hosting and TLS; Watch-based subscriptions; triggers become follow-on workflows | Schedule minimum is 5 minutes and runs are often late or skipped, so alerts lag by 5 to 15 minutes; no live stream; no open webhook registration; scheduled workflows on public repos pause after 60 days without commits; private repos exceed free minutes; NEMWeb access from GitHub runners needs testing |
| **Option 2: own machine + publish to Pages** | Detection within about a minute; public page with nothing listening for inbound connections, so a work network is not exposed | Still needs an always-on machine; webhooks limited to a fixed list |

## Notification channels

| Channel | Pros | Cons | Effort and cost |
| --- | --- | --- | --- |
| **Web page + JSON API** (built) | Anyone can view; event charts and history | People must remember to visit | Done |
| **Atom / JSON Feed** (built) | Open standard; works with feed readers, Slack and newsroom tools | Feed readers poll, so delivery is not instant | Done, free |
| **ntfy push** (built) | Phone push with no account; trivial to subscribe | Topic names are guessable (acceptable for public data); less familiar app | Done, free |
| **Live stream (SSE)** (built) | Real-time for dashboards and bots | Needs an always-on server; unavailable on Pages | Done |
| **Signed webhooks** (built) | Programmatic, filterable by region and severity | Open registration is the main security risk (outbound requests to user-supplied URLs) | Done; disable with `max_webhooks = 0` on sensitive networks |
| **GitHub Watch** (Discussions per event, Releases per brief) | GitHub manages the subscriber list; users self-subscribe at two levels; Releases carry the brief PDF and give a free Atom feed; Discussions host Q&A per event | Subscribers need a GitHub account; notification email is often filtered; mobile push depends on user settings | Low, free |
| **Bluesky** | Free API; generous limits (about 1,600 posts per hour); open protocol; easy threading | Audience size relative to X uncertain for Australian energy (check); add bot self-label | Low, free |
| **X** | Large existing energy-market audience | No free tier for new projects since February 2026 (check docs.x.com); roughly $0.015 per text post and $0.20 per post with a link; prepaid billing enrolment required | Low; tens of dollars a year at expected volumes |
| **Mastodon** | Free; mark account as bot; self-hostable | Smaller audience; instance must permit bots | Low, free |
| **Telegram channel** | Real phone push; no platform account beyond Telegram; one bot token | Less common among Australian professional audiences (check) | Very low, free |
| **Discord webhook** | Suits a community server with discussion per event | Only reaches people in that server | Very low, free |
| **Reddit** | Discussion-friendly; reachable energy communities | New apps need explicit approval since November 2025, with slow responses reported; bot accounts carry an "App" label; moderators can ban bots regardless | High; free for non-commercial use; best by agreement with subreddit moderators or in an own subreddit |
| **LinkedIn / Threads** | Professional reach (LinkedIn) | App review needed before automated posting | High; better posted manually alongside the media brief |
| **Email list** | Familiar to journalists | Needs a mail provider, double opt-in and unsubscribe handling | Medium; deferred from version 1 |

## Design notes for social channels

- **Volume.** Post only on confirmed, closed and brief-ready transitions. Escalation and whiplash updates stay on ntfy and the feeds, which avoids bursts during volatile events.
- **Threading.** Each event becomes one thread: the confirmation post starts it and later updates reply to it. This needs a small table of platform post IDs per event.
- **Images.** Attaching the event chart as a PNG (rendered in Rust from the existing chart) improves reach.
- **Secrets.** Platform tokens live in repo secrets (option 1) or in an environment file outside the config (server options).

## Security notes

- **Localhost binding.** With the default trial configuration, the only traffic is outbound HTTPS to nemweb.com.au (and ntfy.sh if configured).
- **Webhooks.** Open registration is the main exposure: a crafted address could slip past the address pinning via a corporate proxy, or reach internal hosts that use public IP ranges. Mitigations in place include HTTPS only, private-range blocking, address pinning, no redirects, a challenge echo, per-IP rate limits and auto-disable.
- **Tunnels.** Avoid tunnels such as cloudflared on work networks without IT approval, since they bypass the firewall.
- **Review status.** The code has not had a security review and should be treated as a prototype.

## Open questions

- [ ] Which audience matters most: journalists, analysts, or developers? This decides between Bluesky, X, Telegram and GitHub Watch.
- [ ] Is a 5 to 15 minute alert delay acceptable (option 1), or is about one minute required (VPS or option 2)?
- [ ] Is a small budget available for X posting?
- [ ] Should the project approach moderators of an Australian energy subreddit?

## Sources

- [AEMC: market price cap 2026-27](https://www.aemc.gov.au/news-centre/media-releases/aemc-updates-market-price-cap-2026-27)
- [OpenTweet: Is the X API free in 2026?](https://opentweet.io/answers/is-the-x-api-free-in-2026) (third party; verify against docs.x.com)
- [Reddit bot rules 2026 (redditapis.com)](https://www.redditapis.com/blogs/reddit-bot-rules-2026) (third party)
- [Bluesky API rate limits (OpenTweet)](https://opentweet.io/answers/bluesky-api-rate-limit-posts-per-day) (third party)
