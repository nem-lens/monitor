#!/bin/sh
# Run by the data_ready trigger: add market notices to the event's bundle,
# store its full record in the private event store, publish its public view
# (AEMO figures only) to the event library, and push both.
#
#   publish-event.sh BUNDLE
#
# STORE is a clone of the private event store repository and LIBRARY a
# clone of nem-lens/library, both pushable by this user. The engine is not
# run here: replays and counterfactuals are computed on users' own machines.
set -eu

bundle=$1
: "${NEM_MONITOR:=/usr/local/bin/nem-monitor}"
: "${STORE:=/var/lib/nem-monitor/events}"
: "${LIBRARY:=/var/lib/nem-monitor/library}"
id=${NEM_EVENT_ID:-$(basename "$bundle")}

# NEMWeb keeps recent notices only; a failure here should not block publishing.
"$NEM_MONITOR" bundle add-notices "$bundle" || echo "market notices not added" >&2

# One publish at a time: triggers can run concurrently.
exec 9>"$LIBRARY.lock"
flock 9

push() {
  git -C "$1" add -A
  if ! git -C "$1" diff --cached --quiet; then
    git -C "$1" commit -q -m "$2 $id"
    git -C "$1" push -q
  fi
}

git -C "$STORE" pull -q --rebase
"$NEM_MONITOR" store put "$bundle" --store "$STORE"
push "$STORE" "Record"

git -C "$LIBRARY" pull -q --rebase
"$NEM_MONITOR" library add "$STORE/$id" --out "$LIBRARY"
push "$LIBRARY" "Add"
