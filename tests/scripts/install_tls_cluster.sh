#!/bin/bash
. tests/scripts/utils.sh

check_root_dir
check_redis
if [[ "$?" -eq 0 ]]; then
  install_redis
fi

modify_etc_hosts
generate_cluster_credentials
create_tls_cluster_config
start_cluster_tls
echo "Finished installing clustered redis server."