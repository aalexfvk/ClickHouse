#!/bin/bash
set -ex
TAG="${1}"
NEW_BRANCH_INPUT="${2}"

# Auto-generate branch name from tag if not provided:
# v25.12.6.38-stable → yc/25.12
if [ -n "$NEW_BRANCH_INPUT" ]; then
  NEW_BRANCH="$NEW_BRANCH_INPUT"
else
  # Strip leading 'v', extract MAJOR.MINOR (first two numeric components)
  NEW_BRANCH="yc/$(echo "${TAG#v}" | grep -oE '^[0-9]+\.[0-9]+')"
fi

echo "Target branch: $NEW_BRANCH"
echo "new_branch=$NEW_BRANCH" >> $SOURCECRAFT_ENV

# Upstream tag must exist (synced via sync_upstream)
if ! git rev-parse "$TAG" >/dev/null 2>&1; then
  echo "::error::Tag $TAG not found. Run sync_upstream first."
  exit 1
fi

# Target branch must NOT exist yet
if git ls-remote --exit-code --heads origin "$NEW_BRANCH" >/dev/null 2>&1; then
  echo "::error::Branch $NEW_BRANCH already exists."
  echo "::error::To update an existing release branch, use create_rebase_pr instead."
  exit 1
fi
