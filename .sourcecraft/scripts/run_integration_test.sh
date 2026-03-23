#!/bin/bash
PART=$1
set +e
# Updated: amd_binary replaced with amd_llvm_coverage (amd_binary N/5 jobs no longer exist)
python3 -m ci.praktika run "Integration tests (amd_llvm_coverage, $PART/5)"
ret="$?"
set -e

if [ "$ret" -ne 0 ]; then
  tar czf ci/tmp/integration-logs.tar.gz \
    --ignore-failed-read \
    ci/tmp/*.log ci/tmp/*.jsonl ci/tmp/*.json \
    tests/integration/test_*/_instances* \
    2>/dev/null || true
  s3cmd --config /etc/s3cmd/bucket.conf put ci/tmp/integration-logs.tar.gz \
    s3://clickhouse-builds/$SOURCECRAFT_RUN_ID/integration-tests-$PART/ || true
  echo "Integration tests failed"
  exit 1
fi
