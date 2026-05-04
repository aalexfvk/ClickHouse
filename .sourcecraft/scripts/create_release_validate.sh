#!/bin/bash
set -ex
SOURCE_BRANCH="${1}"

# Release branches must follow the yc/XX.YY pattern
if ! echo "$SOURCE_BRANCH" | grep -qE '^yc/[0-9]+\.[0-9]+$'; then
  echo "::error::Invalid branch: $SOURCE_BRANCH"
  echo "::error::Only release branches matching yc/XX.YY are allowed."
  echo "::error::To create a new release branch, use init_release_branch first."
  exit 1
fi

if ! git ls-remote --exit-code --heads origin "$SOURCE_BRANCH" >/dev/null 2>&1; then
  echo "::error::Branch $SOURCE_BRANCH does not exist on remote."
  echo "::error::Create it first with init_release_branch."
  exit 1
fi

echo "target_branch=$SOURCE_BRANCH" >> $SOURCECRAFT_ENV
