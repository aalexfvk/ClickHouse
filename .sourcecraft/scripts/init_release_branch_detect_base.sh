#!/bin/bash
set -ex
# Find the most recent upstream tag reachable from yc/master HEAD.
# This is the "old base" — the tag our patches currently sit on top of.
# "--match v*" matches only upstream tags (e.g. v25.12.1.1-stable).
# Our own release tags intentionally have no leading 'v' (e.g. 25.12.1.1-stable-yc.1)
# so they are never picked up here.
OLD_BASE=$(git describe --tags --abbrev=0 --match "v*" HEAD)
echo "Current patch base in yc/master: $OLD_BASE"
echo "old_base=$OLD_BASE" >> $SOURCECRAFT_ENV
