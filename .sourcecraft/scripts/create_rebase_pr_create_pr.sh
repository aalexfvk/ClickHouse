#!/bin/bash
set -ex
TARGET_BRANCH="${1}"
UPSTREAM_TAG="${2}"

if [ "$skip_rebase" == "true" ]; then
  exit 0
fi

TITLE="Rebase $TARGET_BRANCH on $UPSTREAM_TAG"
DESCRIPTION="### 🤖 Automated Rebase PR

**Type:** Manual resolution or Auto-rebase
**Branch:** $TARGET_BRANCH
**Upstream Tag:** $UPSTREAM_TAG
**Base Commit Protected:** \`${original_sha}\`

---
⚠️ **DO NOT MERGE MANUALLY**
Use the **Apply Rebase** pipeline to finalize."

python3 .sourcecraft/scripts/sourcecraft_cli.py create-pr "$candidate_name" "$TARGET_BRANCH" "$TITLE" "$DESCRIPTION"
