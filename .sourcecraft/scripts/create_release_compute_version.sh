#!/bin/bash
set -ex

# Find the latest upstream tag reachable from HEAD (e.g. v25.12.7.21-stable).
# Our own tags have no leading 'v', so --match "v*" skips them.
UPSTREAM_TAG=$(git describe --tags --abbrev=0 --match "v*" HEAD)
# Strip leading 'v': v25.12.7.21-stable → 25.12.7.21-stable
UPSTREAM_VER="${UPSTREAM_TAG#v}"

# Find the latest our yc tag reachable from HEAD (e.g. 25.12.7.21-stable-yc.2).
# If none exists yet — this is the first release on this upstream base.
LAST_YC_TAG=$(git describe --tags --abbrev=0 --match "*-yc.*" HEAD 2>/dev/null || true)

if [ -n "$LAST_YC_TAG" ] && [[ "$LAST_YC_TAG" == "${UPSTREAM_VER}-yc."* ]]; then
  # Same upstream base — increment the yc counter
  LAST_NUM="${LAST_YC_TAG##*-yc.}"
  NEXT_NUM=$((LAST_NUM + 1))
else
  # New upstream base or no yc tags yet — start from 1
  NEXT_NUM=1
fi

RELEASE_VER="${UPSTREAM_VER}-yc.${NEXT_NUM}"

if git rev-parse "$RELEASE_VER" >/dev/null 2>&1; then
  echo "::error::Tag $RELEASE_VER already exists (race condition?). Re-run the workflow."
  exit 1
fi

echo "Computed release version: $RELEASE_VER"
echo "ver=$RELEASE_VER" >> $SOURCECRAFT_ENV

SHA=$(git rev-parse HEAD)
echo "sha=$SHA" >> $SOURCECRAFT_ENV
