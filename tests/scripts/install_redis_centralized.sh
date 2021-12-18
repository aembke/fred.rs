#!/bin/bash

function check_root_dir {
  if [ ! -d "./tests/tmp" ]; then
    echo "Must be in application root for redis installation scripts to work."
    exit 1
  fi
}

if [ -z "$REDIS_VERSION" ]; then
    echo "REDIS_VERSION must be set!"
    exit 1
fi

ROOT=$PWD
[[ -z "${JOBS}" ]] && PARALLEL_JOBS='2' || PARALLEL_JOBS="${JOBS}"

# Returns 0 if not installed, 1 otherwise.
function check_redis {
  if [ -d "$ROOT/tests/tmp/redis_$REDIS_VERSION/redis-$REDIS_VERSION" ]; then
    echo "Skipping redis install."
    return 1
  else
    echo "Redis install not found."
    return 0
  fi
}

function install_redis {
  echo "Installing redis..."
  pushd $ROOT > /dev/null
  rm -rf tests/tmp/redis_cluster_$REDIS_VERSION
  cd tests/tmp
  curl -O http://download.redis.io/releases/redis-$REDIS_VERSION.tar.gz
  mkdir redis_$REDIS_VERSION
  tar xf redis-$REDIS_VERSION.tar.gz -C redis_$REDIS_VERSION
  rm redis-$REDIS_VERSION.tar.gz
  cd redis_$REDIS_VERSION/redis-$REDIS_VERSION
  make -j"${PARALLEL_JOBS}"
  mv redis.conf redis.conf.bk
  popd > /dev/null
}

function configure_acl {
  if [ -z "$REDIS_USERNAME" ]; then
    echo "Skipping ACL setup due to missing REDIS_USERNAME..."
    return
  fi
  if [ -z "$REDIS_PASSWORD" ]; then
    echo "Skipping ACL setup due to missing REDIS_PASSWORD..."
    return
  fi

  echo "Configuring ACL rules..."
  pushd $ROOT > /dev/null
  cd tests/tmp/redis_$REDIS_VERSION/redis-$REDIS_VERSION
  echo "user $REDIS_USERNAME on allkeys allcommands allchannels >$REDIS_PASSWORD" > ./test_users.acl
  echo "aclfile `pwd`/test_users.acl" > ./redis_centralized.conf
  popd > /dev/null
}

function start_server {
  pushd $ROOT > /dev/null
  cd $ROOT/tests/tmp/redis_$REDIS_VERSION/redis-$REDIS_VERSION

  if [ -f "./redis_server.pid" ]; then
    echo "Found running redis server. Stopping..."
    kill -9 `cat ./redis_server.pid`
  fi

  if [ -f "./redis_centralized.conf" ]; then
    echo "Starting server with config file..."
    nohup ./src/redis-server ./redis_centralized.conf > ./centralized_server.log 2>&1 &
  else
    echo "Starting server without config file..."
    nohup ./src/redis-server > ./centralized_server.log 2>&1 &
  fi
  echo $! > ./redis_server.pid
  echo "Redis server PID is `cat redis_server.pid`"
  popd > /dev/null
}

check_root_dir
check_redis
if [[ "$?" -eq 0 ]]; then
  install_redis
fi
configure_acl
start_server

echo "Finished installing centralized redis server."