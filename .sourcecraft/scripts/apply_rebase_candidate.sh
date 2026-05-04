#!/bin/bash
set -ex
CANDIDATE="${1}"
TARGET="${2}"
PR_NUMBER="${3}"

# 1. Extract original SHA from name candidate branch
# Example: yc/rebase/master/v25.1/from-a1b2c3d4e5 -> a1b2c3d4e5
EXPECTED_SHA=$(echo "$CANDIDATE" | grep -o 'from-.*' | cut -d'-' -f2)

if [ -z "$EXPECTED_SHA" ]; then
  echo "::error::Branch name $CANDIDATE does not contain the /from-<sha> marker!"
  echo "::error::Cannot guarantee safe push. Aborting to prevent data loss."
  exit 1
fi

git fetch origin "$CANDIDATE"
CANDIDATE_SHA=$(git rev-parse "origin/$CANDIDATE")

echo "🚀 Attempting safe push with force-with-lease..."
echo "  Target Ref: refs/heads/$TARGET"
echo "  Expected Old SHA: $EXPECTED_SHA"
echo "  New Rebased SHA: $CANDIDATE_SHA"

set +e
git push --force-with-lease="refs/heads/$TARGET:$EXPECTED_SHA" origin "$CANDIDATE_SHA:refs/heads/$TARGET"
PUSH_STATUS=$?
set -e

if [ $PUSH_STATUS -eq 0 ]; then
  echo "✅ Success. '$TARGET' is safely updated."
  git push origin --delete "$CANDIDATE" || true
else
  echo "::error::🚨 RACE CONDITION PROTECTION WORKED!"
  echo "::error::New commits appeared in $TARGET since this PR was created."
  echo "::error::Your changes are safe, but the PR is outdated."
  echo "::error::👉 Please run 'create_rebase_pr' pipeline again to re-rebase on top of the new commits."
  
  if [ -n "$PR_NUMBER" ]; then
    COMMENT="🚨 **RACE CONDITION PROTECTION WORKED!**
    New commits appeared in \`$TARGET\` while this PR was open. 
    To prevent overwriting colleague's code, the merge was blocked.
    Please close this PR and run \`create_rebase_pr\` pipeline again to generate a fresh branch."
    
    python3 .sourcecraft/scripts/sourcecraft_cli.py post-comment "$PR_NUMBER" "$COMMENT" || true
  fi
  exit 1
fi
