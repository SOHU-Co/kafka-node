#!/bin/sh
rm -rf /usr/local/var/lib/kafka-logs*
launchctl unload ~/Library/LaunchAgents/homebrew.mxcl.zookeeper.plist
sleep 1
rm -rf /usr/local/var/run/zookeeper/data/*
launchctl load ~/Library/LaunchAgents/homebrew.mxcl.zookeeper.plist
#kafka-topics.sh --zookeeper localhost:2181 --topic test --create --partitions 1000 --replication-factor 2
