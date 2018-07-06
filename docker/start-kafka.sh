#!/bin/bash

#
# FIXME KAFKA_OPTS with the SASL config causes a 'bad substitution' error in
# the sed commands below. Only seems to impact the 1.x images, not entirely
# sure why.
#
# Swapping KAFKA_OPTS out temporarily lets us work around the issue.
#
if [[ -n "$KAFKA_OPTS" ]]; then
    TEMP_KAFKA_OPTS="$KAFKA_OPTS"
    unset KAFKA_OPTS
fi

if [[ -z "$KAFKA_PORT" ]]; then
    export KAFKA_PORT=9092
fi
if [[ -z "$KAFKA_ADVERTISED_PORT" && \
  -z "$KAFKA_LISTENERS" && \
  -z "$KAFKA_ADVERTISED_LISTENERS" && \
  -S /var/run/docker.sock ]]; then
    export KAFKA_ADVERTISED_PORT=$(docker port `hostname` $KAFKA_PORT | sed -r "s/.*:(.*)/\1/g")
fi
if [[ -z "$KAFKA_BROKER_ID" ]]; then
    if [[ -n "$BROKER_ID_COMMAND" ]]; then
        export KAFKA_BROKER_ID=$(eval $BROKER_ID_COMMAND)
    else
        # By default auto allocate broker ID
        export KAFKA_BROKER_ID=-1
    fi
fi
if [[ -z "$KAFKA_LOG_DIRS" ]]; then
    export KAFKA_LOG_DIRS="/kafka/kafka-logs-$HOSTNAME"
fi
if [[ -z "$KAFKA_ZOOKEEPER_CONNECT" ]]; then
    export KAFKA_ZOOKEEPER_CONNECT=$(env | grep ZK.*PORT_2181_TCP= | sed -e 's|.*tcp://||' | paste -sd ,)
fi

if [[ -n "$KAFKA_HEAP_OPTS" ]]; then
    sed -r -i "s/(export KAFKA_HEAP_OPTS)=\"(.*)\"/\1=\"$KAFKA_HEAP_OPTS\"/g" $KAFKA_HOME/bin/kafka-server-start.sh
    unset KAFKA_HEAP_OPTS
fi

if [[ -z "$KAFKA_ADVERTISED_HOST_NAME" && -n "$HOSTNAME_COMMAND" ]]; then
    export KAFKA_ADVERTISED_HOST_NAME=$(eval $HOSTNAME_COMMAND)
fi

if [[ -n "$RACK_COMMAND" && -z "$KAFKA_BROKER_RACK" ]]; then
    export KAFKA_BROKER_RACK=$(eval $RACK_COMMAND)
fi

#Issue newline to config file in case there is not one already
echo -e "\n" >> $KAFKA_HOME/config/server.properties

for VAR in `env`
do
  if [[ $VAR =~ ^KAFKA_ && ! $VAR =~ ^KAFKA_HOME && \
      ! $VAR =~ ^KAFKA_CREATE_TOPICS ]]; then
    kafka_name=`echo "$VAR" | sed -r "s/KAFKA_(.*)=.*/\1/g" | tr '[:upper:]' '[:lower:]' | tr _ .`
    env_var=`echo "$VAR" | sed -r "s/(.*)=.*/\1/g"`
    if egrep -q "(^|^#)$kafka_name=" $KAFKA_HOME/config/server.properties; then
        sed -r -i "s@(^|^#)($kafka_name)=(.*)@\2=${!env_var}@g" $KAFKA_HOME/config/server.properties #note that no config values may contain an '@' char
    else
        echo "$kafka_name=${!env_var}" >> $KAFKA_HOME/config/server.properties
    fi
  fi
done

if [[ -n "$CUSTOM_INIT_SCRIPT" ]] ; then
  eval $CUSTOM_INIT_SCRIPT
fi

create-topics.sh &
KAFKA_OPTS="$TEMP_KAFKA_OPTS" exec $KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties
