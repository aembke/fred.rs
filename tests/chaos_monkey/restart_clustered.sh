#!/bin/bash
. $ROOT/tests/chaos_monkey/util.sh
check_env
[[ -z "${WAIT}" ]] && SLEEP_DIR='0' || SLEEP_DIR="${WAIT}"

$CREATE_CLUSTER_PATH stop
sleep "$SLEEP_DIR"
$CREATE_CLUSTER_PATH start