#!/bin/bash
. tests/scripts/utils.sh

check_root_dir
check_redis
if [[ "$?" -eq 0 ]]; then
  install_redis
fi
if [ -z "$INSTALL_CI_TLS" ]; then
  echo "Skip setting up TLS."
else
  generate_cluster_credentials
  configure_tls_config
fi

start_cluster
echo "Finished installing clustered redis server."