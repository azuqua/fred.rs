#!/bin/bash

echo "This will not work correctly unless run from the application root..."
sleep 5

if [ -z "$REDIS_VERSION" ]; then
    echo "REDIS_VERSION must be set!"
    exit 1
fi

ROOT=$PWD

pushd $PWD

rm -rf tests/tmp/redis_cluster_$REDIS_VERSION
cd tests/tmp
curl -O http://download.redis.io/releases/redis-$REDIS_VERSION.tar.gz
mkdir redis_cluster_$REDIS_VERSION
tar xf redis-$REDIS_VERSION.tar.gz -C redis_cluster_$REDIS_VERSION
rm redis-$REDIS_VERSION.tar.gz
cd redis_cluster_$REDIS_VERSION/redis-$REDIS_VERSION
make -j2

echo "Creating cluster..."
cd utils/create-cluster
./create-cluster start

cp $ROOT/tests/scripts/start_cluster.exp ./
./start_cluster.exp

popd

echo "Finished installing clustered redis server."