#!/bin/bash

echo "This will not work correctly unless run from the application root..."
sleep 5

if [ -z "$REDIS_VERSION" ]; then
    echo "REDIS_VERSION must be set!"
    exit 1
fi  

ROOT=$PWD

pushd $PWD

rm -rf tests/tmp/redis_$REDIS_VERSION/*
cd tests/tmp
curl -O http://download.redis.io/releases/redis-$REDIS_VERSION.tar.gz
tar xf redis-$REDIS_VERSION.tar.gz
rm redis-$REDIS_VERSION.tar.gz
cd redis-$REDIS_VERSION
make

popd

echo "Finished installing centralized redis server."