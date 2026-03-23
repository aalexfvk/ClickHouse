#!/bin/bash
set -euo pipefail

ALLOWED_SUFFIXES=("stable" "lts")

echo "Mode: $([ "$CREATE_PR" = "true" ] && echo "full (create PR)" || echo "dry-run (check only)")"

get_latest_tag() {
  local release_line=$1
  local patterns=()
  for suffix in "${ALLOWED_SUFFIXES[@]}"; do
    if [[ "$release_line" == "master" ]]; then
      patterns+=("v*-${suffix}")
    else
      patterns+=("v${release_line}.*-${suffix}")
    fi
  done
  echo "  [DEBUG] get_latest_tag: release_line=$release_line patterns=${patterns[*]}" >&2
  local result count
  result=$(git tag -l "${patterns[@]}" --sort=-v:refname | head -n 1)
  count=$(git tag -l "${patterns[@]}" | wc -l)
  echo "  [DEBUG] get_latest_tag: total_matching=$count result='$result'" >&2
  echo "  [DEBUG] get_latest_tag: first 5 tags:" >&2
  git tag -l "${patterns[@]}" --sort=-v:refname | head -n 5 >&2
  echo "$result"
}

declare -a BRANCHES
if [[ "$INPUT_BRANCH" == "all" ]]; then
  mapfile -t BRANCHES < <(git ls-remote --heads origin | awk '{print $2}' | sed 's|refs/heads/||' | grep -E '^yc/(master|[0-9]+\.[0-9]+)$')
else
  BRANCHES=("$INPUT_BRANCH")
fi

echo "Branches to check: ${BRANCHES[*]}"
FAILED_BRANCHES=()

for BRANCH in "${BRANCHES[@]}"; do
  echo -e "\n========================================="
  echo "Processing branch: $BRANCH"
  echo "========================================="

  git fetch origin "$BRANCH" --quiet

  OLD_BASE=$(git describe --tags --abbrev=0 --match "v*" "origin/$BRANCH" 2>/dev/null || true)
  if [[ -z "$OLD_BASE" ]]; then
    echo "::warning::No upstream tag found on $BRANCH, skipping."
    continue
  fi
  echo "Current base: $OLD_BASE"

  RELEASE_LINE=${BRANCH#yc/}

  LATEST_TAG=$(get_latest_tag "$RELEASE_LINE")
  if [[ -z "$LATEST_TAG" ]]; then
    echo "::warning::No upstream tag found for release line $RELEASE_LINE, skipping."
    continue
  fi
  echo "Latest upstream tag: $LATEST_TAG"

  if [[ "$OLD_BASE" == "$LATEST_TAG" ]]; then
    echo "✅ $BRANCH is already based on $LATEST_TAG, nothing to do."
    continue
  fi

  echo "⚠️ $BRANCH needs rebase: $OLD_BASE → $LATEST_TAG"
  CURRENT_SHA=$(git rev-parse --short=10 "origin/$BRANCH")
  CANDIDATE="yc/rebase/${RELEASE_LINE}/${LATEST_TAG}/from-${CURRENT_SHA}"

  if git ls-remote --exit-code --heads origin "$CANDIDATE" >/dev/null 2>&1; then
    echo "ℹ️ Candidate branch $CANDIDATE already exists. Skipping."
    continue
  fi

  # ================= DRY-RUN MODE =================
  if [[ "$CREATE_PR" != "true" ]]; then
    echo "⏳ Starting dry-run rebase..."
    git checkout --detach "origin/$BRANCH" --quiet
    
    if git rebase --onto "$LATEST_TAG" "$OLD_BASE" HEAD >/dev/null 2>&1; then
      echo "✅ Dry-run rebase of $BRANCH onto $LATEST_TAG succeeded."
    else
      git rebase --abort || true
      echo "::error::Dry-run rebase of $BRANCH onto $LATEST_TAG has CONFLICTS."
      FAILED_BRANCHES+=("$BRANCH")
    fi
    
    git reset --hard HEAD --quiet
    continue
  fi

  # ================= FULL MODE (Create PR) =================
  echo "⏳ Starting full rebase and PR creation..."
  git checkout -B "$CANDIDATE" "origin/$BRANCH" --quiet
  
  if git rebase --onto "$LATEST_TAG" "$OLD_BASE" "$CANDIDATE" >/dev/null 2>&1; then
    git push origin "$CANDIDATE" --force --quiet
    PR_TITLE="[Auto-rebase] $BRANCH → $LATEST_TAG"
    PR_DESC="### 🤖 Automated Rebase PR

**Type:** Auto-rebase
**Branch:** $BRANCH
**Previous Base:** $OLD_BASE
**Upstream Tag:** $LATEST_TAG

---
⚠️ **DO NOT MERGE MANUALLY**
1. Wait for checks to pass.
2. Use the **Apply Rebase** pipeline to finalize."
  else
    git rebase --abort || true
    git checkout -B "$CANDIDATE" "origin/$BRANCH" --quiet
    git push origin "$CANDIDATE" --force --quiet
    
    PR_TITLE="[Auto-rebase CONFLICT] $BRANCH → $LATEST_TAG"
    PR_DESC="### ⚠️ Rebase Conflict — Manual Fix Required

**Type:** Manual resolution
**Branch:** $BRANCH
**Previous Base:** $OLD_BASE
**Upstream Tag:** $LATEST_TAG
**Base Commit:** ${CURRENT_SHA}

Rebase failed with conflicts. Please:
1. \`git fetch origin && git checkout $CANDIDATE\`
2. \`git rebase --onto $LATEST_TAG $OLD_BASE\`
3. Resolve conflicts, then \`git push origin $CANDIDATE --force\`
4. CI will re-run tests automatically."
    
    FAILED_BRANCHES+=("$BRANCH")
  fi

  python3 .sourcecraft/scripts/sourcecraft_cli.py create-pr "$CANDIDATE" "$BRANCH" "$PR_TITLE" "$PR_DESC"

  echo "✅ PR created: $PR_TITLE"
  git checkout --detach HEAD --quiet
done

# ================= FINALIZE =================
if [[ ${#FAILED_BRANCHES[@]} -gt 0 ]]; then
  echo -e "\n❌ The following branches have rebase conflicts:\n"
  for fb in "${FAILED_BRANCHES[@]}"; do echo "  - $fb"; done
  
  if [[ "$CREATE_PR" == "true" ]]; then
    echo "::error::PRs created for conflicting branches. Manual resolution required."
  else
    echo "::error::Dry-run conflicts detected. Manual intervention required before creating PRs."
  fi
  exit 1
fi

echo -e "\n✅ All branches processed successfully."
