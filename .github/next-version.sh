#!/usr/bin/env bash
# Print VERSION=X.Y.Z of the tag this commit gets in CI: its own tag, or the last tag with the patch + 1.
# Used by the build jobs (the version goes into the library) and by the tag job.
set -euo pipefail
tag=$(git tag --points-at HEAD | grep '^v[0-9]' | head -n 1 || true)
if [ -z "$tag" ]; then
    last=$(git describe --tags --abbrev=0 --match 'v[0-9]*' 2>/dev/null || echo v0.0.0)
    IFS=. read -r major minor patch <<< "${last#v}"
    tag="v$major.$minor.$((patch + 1))"
fi
echo "VERSION=${tag#v}"
