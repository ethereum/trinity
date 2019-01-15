#!/usr/bin/env bash

set -o errexit
set -o nounset

apt-get update && apt-get install -y liblz4-dev libsnappy-dev libgflags-dev zlib1g-dev libbz2-dev libzstd-dev

ROCKSDBVER=6.0.2

NPROC="${1:-1}"

echo "Building on $NPROC cores. Pass number of cores (e.g. install_rocksdb.sh 4) to run concurrently"

# More about installation of RocksDB https://github.com/facebook/rocksdb/blob/master/INSTALL.md

[ -f rocksdb/rocksdb-$ROCKSDBVER/Makefile ] || { rm -rf rocksdb ; mkdir -p rocksdb; cd rocksdb; wget https://github.com/facebook/rocksdb/archive/v$ROCKSDBVER.tar.gz && tar xvf v$ROCKSDBVER.tar.gz; cd ..; }
cd rocksdb/rocksdb-$ROCKSDBVER
[ -f util/build_version.cc ] || { make util/build_version.cc ; } # use cached version if possible
export NO_UPDATE_BUILD_VERSION=1
make shared_lib -j$NPROC && make install-shared INSTALL_PATH=/usr
