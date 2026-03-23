#!/bin/bash
set -ex
UPSTREAM_TAG="${1}"
TARGET_BRANCH="${2}"
FORCE_UPDATE="${3}"

SAFE_TAG=$(echo "$UPSTREAM_TAG" | sed 's/[^a-zA-Z0-9.-]//g')
SAFE_TARGET=$(echo "$TARGET_BRANCH" | sed 's|^yc/||')

ORIGINAL_SHA=$(git rev-parse --short=10 HEAD)

CANDIDATE_NAME="yc/rebase/${SAFE_TARGET}/${SAFE_TAG}/from-${ORIGINAL_SHA}"
echo "candidate_name=$CANDIDATE_NAME" >> $SOURCECRAFT_ENV
echo "original_sha=$ORIGINAL_SHA" >> $SOURCECRAFT_ENV

SKIP_REBASE="false"

if [ "$FORCE_UPDATE" == "true" ]; then
   echo "⚠️ force_update requested! Branch will be recreated."
elif git ls-remote --exit-code --heads origin "$CANDIDATE_NAME"; then
   echo "✅ Candidate branch with this exact base ($ORIGINAL_SHA) already exists. Skipping rebase."
   SKIP_REBASE="true"
fi

echo "skip_rebase=$SKIP_REBASE" >> $SOURCECRAFT_ENV
