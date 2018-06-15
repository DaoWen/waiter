#!/usr/bin/env bash
# Usage: run-integration-tests-k8s-scheduler.sh [TEST_COMMAND] [TEST_SELECTOR]
#
# Examples:
#   run-integration-tests-k8s-scheduler.sh parallel-test integration-fast
#   run-integration-tests-k8s-scheduler.sh parallel-test integration-slow
#   run-integration-tests-k8s-scheduler.sh parallel-test
#   run-integration-tests-k8s-scheduler.sh
#
# Runs the Waiter integration tests using the (local) k8s scheduler, and dumps log files if the tests fail.

set -e

TEST_COMMAND=${1:-parallel-test}
TEST_SELECTOR=${2:-integration}

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
WAITER_DIR=${DIR}/../..
KITCHEN_DIR=${WAITER_DIR}/../kitchen

# Start minikube
${DIR}/minikube-setup.sh

# Pre-fetch our test image
docker pull daowen/waiter-kitchen:latest

# Start waiter
WAITER_PORT=9091
${WAITER_DIR}/bin/run-using-k8s.sh ${WAITER_PORT} &

# Run the integration tests
WAITER_AUTH_RUN_AS_USER=${USER} WAITER_URI=127.0.0.1:${WAITER_PORT} WAITER_TEST_KITCHEN_CMD=/opt/kitchen/container-run.sh \
    LEIN_TEST_THREADS=4 \
    ${WAITER_DIR}/bin/test.sh ${TEST_COMMAND} ${TEST_SELECTOR} || test_failures=true

# If there were failures, dump the logs
if [ "$test_failures" = true ]; then
    echo "integration tests failed -- dumping logs"
    tail -n +1 -- log/*.log
    exit 1
fi
