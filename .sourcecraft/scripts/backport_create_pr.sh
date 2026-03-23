#!/bin/bash
set -ex
TARGET="${1}"
BP_BRANCH="$bp_branch"
SHA="${2}"

COMMIT_TITLE=$(git log --format=%s -1 "$SHA")
SHORT_SHA="${SHA::7}"

TITLE="[Backport $TARGET] $COMMIT_TITLE"
DESC="Backport of commit $SHORT_SHA ($COMMIT_TITLE) to $TARGET."

python3 .sourcecraft/scripts/sourcecraft_cli.py create-pr "$BP_BRANCH" "$TARGET" "$TITLE" "$DESC"
