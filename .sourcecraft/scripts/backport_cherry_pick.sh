#!/bin/bash
set -ex
SHA="${1}"

git fetch origin "$SHA"

# '-x' adds "(cherry picked from commit ...)" to description
if git cherry-pick -x "$SHA"; then
  echo "✅ Cherry-pick successful."
else
  echo "::error::Cherry-pick failed due to conflicts."
  echo "::error::Automatic backport is not possible."
  echo "::notice::Please perform manual backport locally."
  git cherry-pick --abort
  exit 1
fi
