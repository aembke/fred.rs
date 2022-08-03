#!/bin/bash

# Settings
BIN_PATH="../../src/"
CREDS_PATH="../../../../creds"
CLUSTER_HOST=127.0.0.1
PORT=40000
TIMEOUT=2000
NODES=6
REPLICAS=1
PROTECTED_MODE=yes
ADDITIONAL_OPTIONS=""

# You may want to put the above config parameters into config.sh in order to
# override the defaults without modifying this script.

if [ -a config.sh ]
then
    source "config.sh"
fi

# Computed vars
ENDPORT=$((PORT+NODES))
CLIENT_TLS_ARGS="--tls --cert $CREDS_PATH/client.pem --key $CREDS_PATH/client.key --cacert $CREDS_PATH/ca.pem"

if [ "$1" == "start" ]
then
    while [ $((PORT < ENDPORT)) != "0" ]; do
        PORT=$((PORT+1))
        echo "Starting $PORT"
        $BIN_PATH/redis-server --port 0 \
          --protected-mode $PROTECTED_MODE \
          --cluster-enabled yes \
          --cluster-config-file nodes-${PORT}.conf \
          --cluster-node-timeout $TIMEOUT \
          --appendonly yes \
          --appendfilename appendonly-${PORT}.aof \
          --dbfilename dump-${PORT}.rdb \
          --logfile ${PORT}.log \
          --tls-port $PORT \
          --tls-cluster yes \
          --tls-replication yes \
          --tls-ca-cert-file ${CREDS_PATH}/ca.pem \
          --tls-key-file ${CREDS_PATH}/node-${PORT}.key \
          --tls-cert-file ${CREDS_PATH}/node-${PORT}.pem \
          --tls-client-cert-file ${CREDS_PATH}/client.pem \
          --tls-client-key-file ${CREDS_PATH}/client.key \
          --tls-auth-clients optional \
          --daemonize yes ${ADDITIONAL_OPTIONS}
    done
    exit 0
fi

if [ "$1" == "create" ]
then
    HOSTS=""
    while [ $((PORT < ENDPORT)) != "0" ]; do
        PORT=$((PORT+1))
        HOSTS="$HOSTS $CLUSTER_HOST:$PORT"
    done
    OPT_ARG=""
    if [ "$2" == "-f" ]; then
        OPT_ARG="--cluster-yes"
    fi
    echo "$BIN_PATH/redis-cli $CLIENT_TLS_ARGS --cluster create $HOSTS --cluster-replicas $REPLICAS $OPT_ARG"
    $BIN_PATH/redis-cli $CLIENT_TLS_ARGS --cluster create $HOSTS --cluster-replicas $REPLICAS $OPT_ARG
    exit 0
fi

if [ "$1" == "stop" ]
then
    while [ $((PORT < ENDPORT)) != "0" ]; do
        PORT=$((PORT+1))
        echo "Stopping $PORT"
        $BIN_PATH/redis-cli $CLIENT_TLS_ARGS -p $PORT shutdown nosave
    done
    exit 0
fi

if [ "$1" == "watch" ]
then
    PORT=$((PORT+1))
    while [ 1 ]; do
        clear
        date
        $BIN_PATH/redis-cli $CLIENT_TLS_ARGS -p $PORT cluster nodes | head -30
        sleep 1
    done
    exit 0
fi

if [ "$1" == "tail" ]
then
    INSTANCE=$2
    PORT=$((PORT+INSTANCE))
    tail -f ${PORT}.log
    exit 0
fi

if [ "$1" == "tailall" ]
then
    tail -f *.log
    exit 0
fi

if [ "$1" == "call" ]
then
    while [ $((PORT < ENDPORT)) != "0" ]; do
        PORT=$((PORT+1))
        $BIN_PATH/redis-cli $CLIENT_TLS_ARGS -p $PORT $2 $3 $4 $5 $6 $7 $8 $9
    done
    exit 0
fi

if [ "$1" == "clean" ]
then
    rm -rf *.log
    rm -rf appendonly*.aof
    rm -rf dump*.rdb
    rm -rf nodes*.conf
    exit 0
fi

if [ "$1" == "clean-logs" ]
then
    rm -rf *.log
    exit 0
fi

echo "Usage: $0 [start|create|stop|watch|tail|clean|call]"
echo "start       -- Launch Redis Cluster instances."
echo "create [-f] -- Create a cluster using redis-cli --cluster create."
echo "stop        -- Stop Redis Cluster instances."
echo "watch       -- Show CLUSTER NODES output (first 30 lines) of first node."
echo "tail <id>   -- Run tail -f of instance at base port + ID."
echo "tailall     -- Run tail -f for all the log files at once."
echo "clean       -- Remove all instances data, logs, configs."
echo "clean-logs  -- Remove just instances logs."
echo "call <cmd>  -- Call a command (up to 7 arguments) on all nodes."
