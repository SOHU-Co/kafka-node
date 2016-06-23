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
    else
        DOCKER_VM_IP=`docker-machine ip dev`

        if [ "$DOCKER_VM_IP" != "" ]; then
            echo "Docker VM is running."
            echo "Docker VM IP Address: $DOCKER_VM_IP"
        else
            echo "Docker VM is not running."
            docker-machine start dev
            DOCKER_VM_IP=`docker-machine ip dev`
        fi

        eval "$(docker-machine env dev)"
    fi

    export KAFKA_ADVERTISED_HOST_NAME=$DOCKER_VM_IP
    docker-compose down
    docker-compose up -d
else
    DOCKER_VM_IP='127.0.0.1'
fi
