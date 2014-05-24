#!/bin/bash
kafka=()
trap 'kill ${kafka[@]}' INT TERM # imperfect
for i in {0..2} # 3 brokers
do
    port=$[9092+i]
    /usr/local/Cellar/kafka/0.8.1.1/bin/kafka-server-start.sh <(sed -e s/broker\.id=0/broker.id=$i/g -e s/9092/$port/g -e s,/usr/local/var/lib/kafka-logs,\&.$i,g /usr/local/etc/kafka/server.properties) & kafka+=($!)
done
wait
