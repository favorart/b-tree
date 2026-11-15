#!/usr/bin/env bash
# Pack the headers and the static library built by this job: mydb-vX.Y.Z-<os>-<arch>.tar.gz
set -euo pipefail
mkdir -p pkg/lib
cp -r include pkg/
cp build/libmydb.a pkg/lib/
cp LICENSE README.md pkg/
tar -czf "mydb-v${VERSION}-${RUNNER_OS}-${RUNNER_ARCH}.tar.gz" -C pkg .
