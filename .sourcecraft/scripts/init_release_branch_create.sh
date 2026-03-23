#!/bin/bash
set -ex
TAG="${1}"
APPLY_PATCHES="${2:-true}"
NEW_BRANCH="$new_branch"

# Create the target branch from current HEAD.
git checkout -b "$NEW_BRANCH"

if [ "$APPLY_PATCHES" != "true" ]; then
  # HEAD is already at TAG (set during checkout step) — nothing more to do.
  echo "✅ Clean branch $NEW_BRANCH created at $TAG (no patches applied)."
  exit 0
fi

# Rebase our patches (old_base..NEW_BRANCH) onto the target upstream tag.
# --onto TARGET OLD_BASE CURRENT_BRANCH
# Takes all commits between old_base and HEAD, replays them on top of TAG.
if git rebase --onto "$TAG" "$old_base" "$NEW_BRANCH"; then
  echo "✅ Rebase successful. Patches applied on top of $TAG."
else
  echo "::error::Rebase failed due to conflicts."
  echo "::error::Manual intervention required: rebase yc/master onto $TAG locally,"
  echo "::error::resolve conflicts, then push as $NEW_BRANCH manually."
  git rebase --abort
  exit 1
fi
