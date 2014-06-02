#!/bin/sh
rm -rf /usr/local/var/lib/kafka-logs*
launchctl unload ~/Library/LaunchAgents/homebrew.mxcl.zookeeper.plist
sleep 1
rm -rf /usr/local/var/run/zookeeper/data/*
ps ax | grep '[k]afka' | awk '{print $1}' | xargs kill -9
sleep 1
rm -rf /usr/local/var/lib/kafka-logs*
launchctl load ~/Library/LaunchAgents/homebrew.mxcl.zookeeper.plist
