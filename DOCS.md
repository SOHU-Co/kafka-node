### About

Kafka-node is a nodejs client with zookeeper integration for apache Kafka, only support the latest version of kafka 0.8 which is still under development, so this module
is `not production ready` so far.
Zookeeper does the following jobs:

* Load broker metadata from zookeeper before we can communicate with kafka server
* Watch broker state, if broker changed, client will refresh broker and topic metadata stored in client

### Install kafka
Follow the [instructions](https://cwiki.apache.org/KAFKA/kafka-08-quick-start.html) on the Kafka wiki to build Kafka 0.8 and get a test broker up and running.
