#!/bin/bash

source start-docker.sh
export KAFKA_TEST_HOST=$DOCKER_VM_IP
echo "KAFKA_TEST_HOST: $KAFKA_TEST_HOST"
./node_modules/.bin/istanbul cover _mocha -- -t 10000 test/test.*js
