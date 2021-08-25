#!/bin/bash
. $ROOT/tests/chaos_monkey/util.sh
check_env
[[ -z "${WAIT}" ]] && SLEEP_DIR='0' || SLEEP_DIR="${WAIT}"

$REDIS_CLI_PATH -p $PORT shutdown
sleep "$SLEEP_DIR"
nohup $REDIS_SERVER_PATH > $REDIS_ROOT_DIR/centralized_server.log 2>&1 &
echo $! > $REDIS_ROOT_DIR/redis_server.pid