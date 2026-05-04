#!/bin/bash
set -ex
TARGET="${1}"
BP_BRANCH="$bp_branch"
SHA="${2}"

TITLE="[Backport $TARGET] Cherry-pick $SHA"
DESC="Backport of commit $SHA to $TARGET."

python3 .sourcecraft/scripts/sourcecraft_cli.py create-pr "$BP_BRANCH" "$TARGET" "$TITLE" "$DESC"
