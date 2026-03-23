#!/bin/bash
set -ex
TARGET="${1}"
SHA="${2}"

echo "Backporting commit $SHA to $TARGET..."

git fetch origin "$TARGET"
# git checkout "$TARGET"
git checkout -b "$TARGET" origin/$TARGET

# Strip leading 'yc/' from target branch name: yc/25.12 → 25.12
SAFE_TARGET=$(echo "$TARGET" | sed 's|^yc/||')
BP_BRANCH="yc/backport/${SAFE_TARGET}/${SHA::7}"

echo "Creating branch: $BP_BRANCH"                
git checkout -b "$BP_BRANCH"

echo "bp_branch=$BP_BRANCH" >> $SOURCECRAFT_ENV
