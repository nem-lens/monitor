#!/bin/sh
# Run by the data_ready trigger: replay the event with every scenario,
# publish its headline to the event library and push the library.
#
#   publish-event.sh BUNDLE
#
# LIBRARY is a clone of nem-lens/library that this user can push to.
set -eu

bundle=$1
: "${NEMCF:=/opt/nemcf/bin/nemcf}"
: "${NEM_MONITOR:=/usr/local/bin/nem-monitor}"
: "${SCENARIOS:=/opt/nemcf/scenarios}"
: "${LIBRARY:=/var/lib/nem-monitor/library}"

"$NEMCF" replay --bundle "$bundle" --scenario "$SCENARIOS" \
  --out "$bundle/replay" --save-snapshots "$bundle/replay/snapshots.json"

# One publish at a time: triggers can run concurrently.
exec 9>"$LIBRARY.lock"
flock 9
git -C "$LIBRARY" pull -q --rebase
"$NEM_MONITOR" library add "$bundle" --out "$LIBRARY" --scenarios "$SCENARIOS"
git -C "$LIBRARY" add -A
if ! git -C "$LIBRARY" diff --cached --quiet; then
  git -C "$LIBRARY" commit -q -m "Add ${NEM_EVENT_ID:-$(basename "$bundle")}"
  git -C "$LIBRARY" push -q
fi
