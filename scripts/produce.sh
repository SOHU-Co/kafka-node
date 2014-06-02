#!/bin/sh
# XXX this uses 1 partition
seq 1000000 | kafka-console-producer.sh --broker-list localhost:9092,localhost:9093,localhost:9094 --topic test
