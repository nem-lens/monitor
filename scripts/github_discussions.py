#!/usr/bin/env python3
"""Post public notifications to GitHub Discussions (handover P2).

One Discussion per event, created on its first public notification
(normally "confirmed"); later transitions are comments. People who Watch the
repository (Custom: Discussions) get each one as a GitHub notification.

State lives with the site in data/github.json: the Discussion for each event
and the notification sequence numbers already posted. Each post also carries
a hidden marker, so a rerun after a failed deploy finds what it already
posted instead of posting twice.

On the first run (no previous data/github.json) existing notifications are
recorded as posted without posting, so enabling this does not replay history;
pass --post-existing to post them.

Usage: github_discussions.py --site site --previous URL|DIR --repo owner/name
                             --category Alerts [--dry-run] [--post-existing]
Environment: GH_TOKEN (needs discussions: write).
"""
import argparse
import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path

API = os.environ.get("GITHUB_GRAPHQL_URL", "https://api.github.com/graphql")
STATE = "data/github.json"
KEEP_POSTED = 2000


def graphql(query, **variables):
    req = urllib.request.Request(
        API,
        data=json.dumps({"query": query, "variables": variables}).encode(),
        headers={"Authorization": f"bearer {os.environ['GH_TOKEN']}", "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        out = json.loads(r.read())
    if out.get("errors"):
        raise RuntimeError(out["errors"])
    return out["data"]


def load_previous(src):
    """data/github.json from the previous site; None if there is none yet."""
    if src.startswith(("http://", "https://")):
        try:
            with urllib.request.urlopen(f"{src.rstrip('/')}/{STATE}", timeout=30) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            raise
    p = Path(src) / STATE
    return json.loads(p.read_text()) if p.exists() else None


def marker(event_id, seq=None):
    return f"<!-- nem:event={event_id} -->" if seq is None else f"<!-- nem:event={event_id} seq={seq} -->"


def body(n):
    return f"{n['summary']}\n\n[Event page]({n['url']})\n\n{marker(n['event_id'])}\n{marker(n['event_id'], n['seq'])}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--site", required=True)
    ap.add_argument("--previous", required=True)
    ap.add_argument("--repo", required=True)
    ap.add_argument("--category", required=True)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--post-existing", action="store_true")
    a = ap.parse_args()

    notes = json.loads((Path(a.site) / "data/notifications.json").read_text())["notifications"]
    notes.sort(key=lambda n: n["seq"])
    prev = load_previous(a.previous)
    state = prev or {"discussions": {}, "posted": []}
    posted = set(state["posted"])
    out_path = Path(a.site) / STATE

    def save():
        state["posted"] = sorted(posted)[-KEEP_POSTED:]
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(state, indent=1))

    if prev is None and not a.post_existing:
        posted.update(n["seq"] for n in notes)
        save()
        print(f"first run: recorded {len(notes)} existing notifications as posted without posting")
        return

    todo = [n for n in notes if n["seq"] not in posted]
    if not todo:
        save()
        print("nothing new to post")
        return
    if a.dry_run:
        known = set(state["discussions"])
        for n in todo:
            kind = "comment on" if n["event_id"] in known else "create discussion for"
            known.add(n["event_id"])
            print(f"would {kind} {n['event_id']}: [{n['transition']}] {n['title']}")
        return

    owner, name = a.repo.split("/", 1)
    repo = graphql(
        """query($owner: String!, $name: String!) {
          repository(owner: $owner, name: $name) { id discussionCategories(first: 50) { nodes { id name } } } }""",
        owner=owner, name=name,
    )["repository"]
    cat = next((c["id"] for c in repo["discussionCategories"]["nodes"] if c["name"] == a.category), None)
    if cat is None:
        sys.exit(f"no Discussions category named {a.category!r}")
    # Recent discussions and their comments, to find posts a failed run already made.
    recent = graphql(
        """query($owner: String!, $name: String!, $cat: ID!) {
          repository(owner: $owner, name: $name) {
            discussions(first: 50, categoryId: $cat, orderBy: {field: CREATED_AT, direction: DESC}) {
              nodes { id url body comments(last: 100) { nodes { body } } } } } }""",
        owner=owner, name=name, cat=cat,
    )["repository"]["discussions"]["nodes"]
    existing_text = {d["id"]: d["body"] + "".join(c["body"] for c in d["comments"]["nodes"]) for d in recent}

    for n in todo:
        ev, seq = n["event_id"], n["seq"]
        d = state["discussions"].get(ev)
        if d is None:
            found = next((x for x in recent if marker(ev) in x["body"]), None)
            if found:
                d = state["discussions"][ev] = {"id": found["id"], "url": found["url"]}
        if d is not None and marker(ev, seq) in existing_text.get(d["id"], ""):
            posted.add(seq)
            continue
        if d is None:
            made = graphql(
                """mutation($repo: ID!, $cat: ID!, $title: String!, $body: String!) {
                  createDiscussion(input: {repositoryId: $repo, categoryId: $cat, title: $title, body: $body}) {
                    discussion { id url } } }""",
                repo=repo["id"], cat=cat, title=n["title"], body=body(n),
            )["createDiscussion"]["discussion"]
            state["discussions"][ev] = made
            print(f"created {made['url']} for {ev}")
        else:
            graphql(
                """mutation($id: ID!, $body: String!) {
                  addDiscussionComment(input: {discussionId: $id, body: $body}) { comment { id } } }""",
                id=d["id"], body=f"**{n['title']}**\n\n{body(n)}",
            )
            print(f"commented on {d['url']}: {n['title']}")
        posted.add(seq)
        save()  # after every post, so a crash part-way loses nothing
    save()


if __name__ == "__main__":
    main()
