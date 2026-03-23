#!/bin/bash
set -ex
UPSTREAM_TAG="${1}"

# "--match v*" matches only upstream tags (e.g. v25.12.1.1-stable).
# Our own release tags intentionally have no leading 'v' (e.g. 25.12.1.1-stable-yc.1)
# so they are never picked up here.
OLD_BASE=$(git describe --tags --abbrev=0 --match "v*" HEAD)
echo "Current base determined as: $OLD_BASE"

# Is already rebased
if [ "$OLD_BASE" == "$UPSTREAM_TAG" ]; then
  echo "::error::Branch is already based on $UPSTREAM_TAG."
  exit 1
fi

# Is new tag ancestor of old base
if git merge-base --is-ancestor "$UPSTREAM_TAG" HEAD; then
  echo "::warning::Tag $UPSTREAM_TAG is already merged into history."
  exit 1
fi

echo "old_base=$OLD_BASE" >> $SOURCECRAFT_ENV
