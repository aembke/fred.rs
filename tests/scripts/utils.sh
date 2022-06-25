#!/bin/bash

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
  make -j"${PARALLEL_JOBS}"
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

# Modify the /etc/hosts file to map the node-<index>.example.com domains to localhost.
function modify_etc_hosts {
  # check the hosts file and ask if it can be modified only if needed
  echo "Unimplemented"
}

# Generate creds for a CA, a cert/key for the client, a cert/key for each node in the cluster, and sign the certs with the CA creds.
#
# Note: it's also necessary to modify DNS mappings so the CN in each cert can be used as a hostname. See `modify_etc_hosts`.
function generate_cluster_credentials {
  echo "Generating keys..."
  mkdir $ROOT/tests/tmp/creds
  pushd $ROOT > /dev/null
  cd $ROOT/tests/tmp/creds
  rm -rf ./*

  echo "Generating CA key pair..."
  openssl req -new -newkey rsa:2048 -nodes -out ca.csr -keyout ca.key -subj '/CN=*.example.com'
  openssl x509 -trustout -signkey ca.key -days 90 -req -in ca.csr -out ca.pem

  echo "Generating client key pair..."
  openssl genrsa -out client.key 2048
  openssl req -new -key client.key -out client.csr -subj '/CN=client.example.com'
  openssl x509 -req -days 90 -sha256 -in client.csr -CA ca.pem -CAkey ca.key -set_serial 01 -out client.pem

  echo "Generating key pairs for each cluster node..."
  for i in `seq 1 6`; do
    openssl genrsa -out "node-$i.key" 2048
    openssl req -new -key "node-$i.key" -out "node-$i.csr" -subj "/CN=node-$i.example.com"
    openssl x509 -req -days 90 -sha256 -in "node-$i.csr" -CA ca.pem -CAkey ca.key -set_serial 01 -out "node-$i.pem"
  done

  echo "Printing subject for each cert..."
  for cert in ./*.pem; do
    local subj=`openssl x509 -noout -subject -in $cert`
    echo "$cert: $subj"
  done

  TLS_CREDS_PATH=$PWD
  popd > /dev/null
}

# Set up TLS on the Redis server nodes according to the following rules:
# * Clients must authenticate with a valid cert.
# * Each server node will have its own FQDN (node-<idx>.example.com), resolvable by the client and other nodes, that matches the CN in each node's cert.
# * The fake "root" CA creds will be used by the client and server nodes. The "root" cert is a star cert for example.com (*.example.com).
# * The client will use a cert with CN of client.example.com.
function configure_tls_config {
  echo "Configuring TLS settings..."
  pushd $ROOT > /dev/null
  cd $ROOT/tests/tmp/redis_$REDIS_VERSION/redis-$REDIS_VERSION
  cp redis.conf.bk redis.conf.tls
  local escaped_path=`echo $TLS_CREDS_PATH | sed 's_/_\\/_g'`

  sed -i "s/^# port 0$/port 0/" redis.conf.tls
  sed -i "s/^# tls-port 6379$/tls-port 6379/" redis.conf.tls
  sed -i 's/^# tls-ca-cert-file ca.crt$/tls-ca-cert-file $escaped_path\/ca.pem/' redis.conf.tls
  sed -i "s/^# tls-cluster yes$/tls-cluster yes/" redis.conf.tls

  echo "Configuring each cluster node's TLS settings..."
  for i in `seq 1 6`; do
    # for each server, create a new conf file
    # # tls-cert-file redis.crt
      # tls-key-file redis.key


  done

  popd > /dev/null
}

function configure_cluster_hostname_config {
  # change the redis.conf to use the new cluster hostname features
  echo "Unimplemented"
}
