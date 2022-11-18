#!/bin/bash

TLS_CLUSTER_PORT=30000

function check_root_dir {
  if [ ! -d "./tests/tmp" ]; then
    echo "Must be in application root for redis installation scripts to work."
    exit 1
  fi
}

declare -a arr=("REDIS_VERSION" "REDIS_USERNAME" "REDIS_PASSWORD" "REDIS_SENTINEL_PASSWORD")

for env in "${arr[@]}"
do
  if [ -z "$env" ]; then
    echo "$env must be set. Run `source tests/environ` if needed."
    exit 1
  fi
done

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
  make BUILD_TLS=yes -j"${PARALLEL_JOBS}"
  mv redis.conf redis.conf.bk
  popd > /dev/null
}

function configure_centralized_acl {
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

function start_centralized {
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

function start_cluster {
  echo "Creating and starting cluster..."
  pushd $ROOT > /dev/null
  cd $ROOT/tests/tmp/redis_$REDIS_VERSION/redis-$REDIS_VERSION/utils/create-cluster
  ./create-cluster stop
  ./create-cluster clean
  ./create-cluster start
  ./create-cluster create -f
  popd > /dev/null
}

function start_cluster_tls {
  echo "Creating and starting TLS cluster..."
  pushd $ROOT > /dev/null
  cd $ROOT/tests/tmp/redis_$REDIS_VERSION/redis-$REDIS_VERSION/utils/create-cluster-tls
  ./create-cluster stop
  ./create-cluster clean
  ./create-cluster start
  ./create-cluster create -f
  popd > /dev/null

  echo "Cluster with TLS started on ports $((TLS_CLUSTER_PORT+1))-$((TLS_CLUSTER_PORT+6))"
}

# Modify the /etc/hosts file to map the node-<index>.example.com domains to localhost.
function modify_etc_hosts {
  if [ -z "$CIRCLECI_TESTS" ]; then
    read -p "Modify /etc/hosts with TLS hostnames? [y/n]: " DNS_INPUT
    if [ "$DNS_INPUT" = "y" ]; then
      echo "Using sudo to modify /etc/hosts..."
    else
      return
    fi
  fi

  TLS_HOSTS="127.0.0.1 example.com client.example.com"
  CERT_PORT=$TLS_CLUSTER_PORT
  for i in `seq 1 6`; do
    CERT_PORT=$((CERT_PORT+1))
    TLS_HOSTS="$TLS_HOSTS node-$CERT_PORT.example.com"
  done

  echo $TLS_HOSTS | sudo tee -a /etc/hosts
}

# Generate creds for a CA, a cert/key for the client, a cert/key for each node in the cluster, and sign the certs with the CA creds.
#
# Note: it's also necessary to modify DNS mappings so the CN in each cert can be used as a hostname. See `modify_etc_hosts`.
function generate_cluster_credentials {
  echo "Generating keys..."
  if [ ! -d "$ROOT/tests/tmp/creds" ]; then
    mkdir -p $ROOT/tests/tmp/creds
  fi
  pushd $ROOT > /dev/null
  cd $ROOT/tests/tmp/creds
  rm -rf ./*

  echo "Generating CA key pair..."
  openssl req -new -newkey rsa:2048 -nodes -out ca.csr -keyout ca.key -subj '/CN=*.example.com'
  openssl x509 -trustout -signkey ca.key -days 90 -req -in ca.csr -out ca.pem
  # need the CA cert in DER format too
  openssl x509 -outform der -in ca.pem -out ca.crt

  echo "Generating client key pair..."
  openssl genrsa -out client.key 2048
  openssl req -new -key client.key -out client.csr -subj '/CN=client.example.com'
  openssl x509 -req -days 90 -sha256 -in client.csr -CA ca.pem -CAkey ca.key -set_serial 01 -out client.pem
  # need the client cert in DER format for rustls
  openssl x509 -outform der -in client.pem -out client.crt
  # need the client key in PEM format for native-tls
  openssl rsa -inform der -in client.key -outform pem -out client.pem

  echo "Generating key pairs for each cluster node..."
  CERT_PORT=$TLS_CLUSTER_PORT
  for i in `seq 1 6`; do
    CERT_PORT=$((CERT_PORT+1))
    openssl genrsa -out "node-$CERT_PORT.key" 2048
    openssl req -new -key "node-$CERT_PORT.key" -out "node-$CERT_PORT.csr" -subj "/CN=node-$CERT_PORT.example.com"
    openssl x509 -req -days 90 -sha256 -in "node-$CERT_PORT.csr" -CA ca.pem -CAkey ca.key -set_serial 01 -out "node-$CERT_PORT.pem"
  done

  echo "Printing subject for each cert..."
  for cert in ./*.pem; do
    local subj=`openssl x509 -noout -subject -in $cert`
    echo "$cert: $subj"
  done

  TLS_CREDS_PATH=$PWD
  popd > /dev/null
}

function create_tls_cluster_config {
  echo "Creating cluster configuration file..."
  pushd $ROOT > /dev/null
  cd $ROOT/tests/tmp/redis_$REDIS_VERSION/redis-$REDIS_VERSION
  rm -rf utils/create-cluster-tls
  cp -rf utils/create-cluster utils/create-cluster-tls
  cp $ROOT/tests/scripts/create-cluster-tls.sh utils/create-cluster-tls/create-cluster
  cd utils/create-cluster-tls
  chmod +x ./create-cluster
  echo "" > config.sh

  popd > /dev/null
}
