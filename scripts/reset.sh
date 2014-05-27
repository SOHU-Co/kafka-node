#!/bin/sh
rm -rf /usr/local/var/lib/kafka-logs*
launchctl unload ~/Library/LaunchAgents/homebrew.mxcl.zookeeper.plist
rm -rf /usr/local/var/run/zookeeper/data/*
launchctl load ~/Library/LaunchAgents/homebrew.mxcl.zookeeper.plist
