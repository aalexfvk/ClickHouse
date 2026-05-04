#!/bin/bash
set -ex

git remote add upstream ${CLICKHOUSE_UPSTREAM_REPO_URL}

# Dynamically generate the list of refspecs for master and release branches (25.12, 26.1, etc.)
#    - git ls-remote --heads upstream : retrieves the list of all remote branches
#    - awk '{print $2}' : keeps only the ref names (e.g., refs/heads/master, refs/heads/24.3)
#    - grep -E ... : filters only 'master' and release branches (format: digit.digit)
#    - awk ... : converts them to the fetch refspec format "+refs/heads/NAME:refs/remotes/upstream/NAME"
REF_SPECS=$(git ls-remote --heads upstream | \
awk '{print $2}' | \
grep -E 'refs/heads/(master|[0-9]+\.[0-9]+)$' | \
awk -F/ '{print "+"$0":refs/remotes/upstream/"$3}' | \
tr '\n' ' ')

# Execute fetch with the substituted list of branches.
git fetch upstream --tags --quiet $REF_SPECS 2>&1 | grep -vE "multiple configurations|Skipping second one" || true

# Push to origin
git push origin "refs/remotes/upstream/*:refs/heads/*" --force
git push origin --tags --force
