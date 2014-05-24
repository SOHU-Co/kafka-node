#!/bin/sh
# XXX this uses 1 partition
seq 1000000 | /usr/local/Cellar/kafka/0.8.1.1/bin/kafka-console-producer.sh --broker-list localhost:9092,localhost:9093,localhost:9094 --topic test
