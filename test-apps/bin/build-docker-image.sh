#!/usr/bin/env bash
# Usage: build-docker-image.sh
#
# Builds a docker image that includes the test apps and can also be used as a minimesos agent.

set -ex

if [ "$1" == --minikube ]; then
    echo 'Using minikube docker environment'
    eval $(minikube docker-env)
fi

TEST_APPS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd )"
KITCHEN_IMAGE=twosigma/kitchen
INTEGRATION_IMAGE=twosigma/integration

cd ${TEST_APPS_DIR}
echo "Building docker image for ${KITCHEN_IMAGE}"
docker build -t ${KITCHEN_IMAGE} .

echo "Building docker image for ${INTEGRATION_IMAGE}"
docker build -t ${INTEGRATION_IMAGE} - <<EOF
FROM ${KITCHEN_IMAGE}
ENV INTEGRATION_TEST_SENTINEL_VALUE="Integration Test Sentinel Value"
EOF
