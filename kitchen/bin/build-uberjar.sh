#!/usr/bin/env bash
# Usage: build-uberjar.sh
#
# Builds the kitchen uberjar.

set -ex

KITCHEN_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd )"

pushd ${KITCHEN_DIR}

lein voom build-deps
lein uberjar

popd
