#!/bin/bash

if [ "$?" != "0" ]; then
    DOCKER_VM_IP=''
fi

if [ -z "$TRAVIS" ]; then
    if docker info | grep Alpine > /dev/null; then
        echo "Looks like docker for mac is running"
        DOCKER_VM_IP='127.0.0.1'
    elif docker info | grep dlite > /dev/null; then
        echo "Looks like docker based on dlite is running"
        DOCKER_VM_IP=`dlite ip`
    fi

    DOCKER_VM_IP=${DOCKER_VM_IP:-127.0.0.1}
    export KAFKA_ADVERTISED_HOST_NAME=$DOCKER_VM_IP
    docker-compose down

    if [ -z "$KAFKA_VERSION" ]; then
      docker-compose up -d
    else
      echo "Using Kafka Version: $KAFKA_VERSION"
      docker-compose -f docker-compose.yml -f docker/docker-compose.${KAFKA_VERSION}.yml up -d
    fi
else
    DOCKER_VM_IP='127.0.0.1'
fi
