# kafka-node CHANGELOG

## 2016-11-18, Version 1.0.7
- Fix issue where `createTopics` using the `async` set to `false` was not synchronous. [#519](https://github.com/SOHU-Co/kafka-node/pull/519)

   **NOTE**: The behavior now is if `async` is true the callback is not actually called until all the topics are confirmed to have been created by Kafka. The previous behavior the callback would be called after the first request (which does not guarantee the topics have been created). This wasn't consistent with what the doc said. 

- Fix issue where messages are lost when sending a batch of keyed messages using the highLevelPartitioner [#521](https://github.com/SOHU-Co/kafka-node/pull/521)
- Upgrade UUID package [#520](https://github.com/SOHU-Co/kafka-node/pull/520)
- Check for in loops using `hasOwnProperty` to defend against insane libraries that update the prototype of `Object` [#485](https://github.com/SOHU-Co/kafka-node/pull/485)
- Refactor `HighLevelProducer` and `Producer` [#508](https://github.com/SOHU-Co/kafka-node/pull/508)

## 2016-11-15, Version 1.0.6
- Fix leave group exception that can occur in [Consumer Group](https://github.com/SOHU-Co/kafka-node#consumergroup) [#513](https://github.com/SOHU-Co/kafka-node/pull/513)

## 2016-11-03, Version 1.0.5
- Update doc added how to list all topics [#503](https://github.com/SOHU-Co/kafka-node/pull/503)
- Fix uncaught exceptions that can occur when using ConsumerGroup [#505](https://github.com/SOHU-Co/kafka-node/pull/505)

## 2016-11-01, Version 1.0.4
- Fix issue where an exception is thrown in `client.brokerForLeader` when connection with broker is lost in ConsumerGroup it should retry instead [#498](https://github.com/SOHU-Co/kafka-node/pull/498)
- Fix issue where invalid characters could be used in createTopics call [#495](https://github.com/SOHU-Co/kafka-node/pull/495) [#492](https://github.com/SOHU-Co/kafka-node/pull/492)

## 2016-10-24, Version 1.0.3
- Fix issue in [Consumer Group](https://github.com/SOHU-Co/kafka-node#consumergroup) where using the migrator with no previous HLC offsets will set initial offsets to 0 instead of the offsets provided in "fromOfset" feature [#493](https://github.com/SOHU-Co/kafka-node/pull/493)

## 2016-10-22, Version 1.0.2
- Fix issue in [Consumer Group](https://github.com/SOHU-Co/kafka-node#consumergroup) where using the migrator with no previous HLC offsets will set initial offsets to -1 [#490](https://github.com/SOHU-Co/kafka-node/pull/490)

## 2016-10-13, Version 1.0.1

- Fix missing support in [Consumer Group](https://github.com/SOHU-Co/kafka-node#consumergroup) for fromOffset using `earliest` and `none` [#483](https://github.com/SOHU-Co/kafka-node/pull/483)

## 2016-10-10, Version 1.0.0

Major version change since we're dropping support for Node older than v4.

Added a new Consumer called [Consumer Group](https://github.com/SOHU-Co/kafka-node#consumergroup) this is supported in Kafka 0.9 and greater. [#477](https://github.com/SOHU-Co/kafka-node/pull/477) 

* Add group membership API protocols 
* Add consumerGroup roundrobin assignment
* Add documentation for ConsumerGroup
* Implemented range assignment strategy
* New consumer group should still emit rebalanced and rebalancing events
* Refactor HLC rebalancing test to run against ConsumerGroup as well
* Add migration from existing HLC offsets in ConsumerGroups
* Implement rolling migration of HLC offsets for consumer groups
* Dropping support for node 0.12 and only supporting 4 and above. Updating to version 1.0.0
* Upgrade mocha
* Migrator: cover the cases of slow rebalances by adding verification by checking check four times make sure the zookeeper ownership did not come back

## 2016-10-03, Version 0.5.9
- Fix issue with highLevelConsumers and how consumer groups react to zookeeper redeploys creating a lot of [NODE_EXISTS] errors [#472](https://github.com/SOHU-Co/kafka-node/pull/472)
- Removed docker-machine support for tests [#474](https://github.com/SOHU-Co/kafka-node/pull/474)
- Minor fixes and additions to doc [#475](https://github.com/SOHU-Co/kafka-node/pull/475) [#471](https://github.com/SOHU-Co/kafka-node/pull/471) 

## 2016-09-12, Version 0.5.8
- Fix duplicate messages consumed on startup this was triggered by unnecessary rebalance (versions affected: *v0.5.4* to *v0.5.7*) [#465](https://github.com/SOHU-Co/kafka-node/pull/465)

## 2016-09-07, Version 0.5.7
- Fix regression when calling consumer's `setOffset` fails to set the given offset [#457](https://github.com/SOHU-Co/kafka-node/pull/457)
- Improved zookeeeper connection loss recovery to verify consumer is still registered [#458](https://github.com/SOHU-Co/kafka-node/pull/458)

## 2016-08-17, Version 0.5.6
- Fix older version of node issue introduced in last version [#447](https://github.com/SOHU-Co/kafka-node/pull/447)

## 2016-08-11, Version 0.5.5
- Updated doc [#443](https://github.com/SOHU-Co/kafka-node/pull/443)
- Validate topic's partition value to be a number [#442](https://github.com/SOHU-Co/kafka-node/pull/442)
- Fixed issue where module was relying on deprecated kafka configs (especially broken when SSL is enabled) [#427](https://github.com/SOHU-Co/kafka-node/issues/427)

## 2016-08-09, Version 0.5.4
- Fix lost client options when creating a `Client` w/o the `new` operator [#437](https://github.com/SOHU-Co/kafka-node/pull/437)
- Fix issue rebalances can be missed when an event occurs during rebalance [#435](https://github.com/SOHU-Co/kafka-node/pull/435)
- Fix issue where changes to a topic's number of partitions should trigger a rebalance [#430](https://github.com/SOHU-Co/kafka-node/pull/430)
- Added coverall coverage and additional tests [#433](https://github.com/SOHU-Co/kafka-node/pull/433) and [#432](https://github.com/SOHU-Co/kafka-node/pull/432)

## 2016-08-05, Version 0.5.3
- Fix for some long standing high-level consumer rebalance issues: [#423](https://github.com/SOHU-Co/kafka-node/pull/423)
	- Fixed issue where consumers who weren't assigned a partition would never rebalance... ever
	- Fixed issue where calling `close` consumer did not force the consumer to leave the consumer group and so locks the partition(s) for (the default) of 30000ms
	- Fixed issue where consumers who weren't assigned a partition never emitted a `rebalance` event after rebalancing
	- Additional cases to be addressed in the next release:
		- If consumer joins or leaves a group during another consumers rebalance then that rebalancing consumer may miss owning partitions
		- Changes to a topic's partition does not trigger a rebalance
- Update doc to warn about possible data loss with way the data is formated when sending with the producer [#425](https://github.com/SOHU-Co/kafka-node/pull/425)
- Added code coverage and additional high-level producer tests [#422](https://github.com/SOHU-Co/kafka-node/pull/422)

## 2016-07-29, Version 0.5.2
- Fix TypeError: Cannot read property 'sslHost' of undefined [#417](https://github.com/SOHU-Co/kafka-node/pull/417)

## 2016-07-27, Version 0.5.1
- Prevent fetch before offset's are updated before a rebalance [#402](https://github.com/SOHU-Co/kafka-node/pull/402)
- Add validation to `groupId` and `clientId` [#405](https://github.com/SOHU-Co/kafka-node/pull/405)
- Removed unused `autoCommitMsgCount` config option [#406](https://github.com/SOHU-Co/kafka-node/pull/406)
- Fixed issue where reconnecting brokers emits a `connect` event instead of `reconnect`. [#413](https://github.com/SOHU-Co/kafka-node/pull/413)
- Fixed uncaught exception where `sslHost` or `host` of undefined is accessed. [#413](https://github.com/SOHU-Co/kafka-node/pull/413)

## 2016-07-14, Version 0.5.0
- Fix minimatch vulnerability by upgrading snappy to v5.0.5 [#400](https://github.com/SOHU-Co/kafka-node/pull/400)
- Added ESLint to codebase [#392](https://github.com/SOHU-Co/kafka-node/pull/392)
- Added ability to make SSL connections to brokers [#383](https://github.com/SOHU-Co/kafka-node/pull/383) (Kafka 0.9+ only)

## 2016-06-27, Version 0.4.0
- Update test to run against docker [#387](https://github.com/SOHU-Co/kafka-node/pull/387)
- Fix missing npm license field warning [#386](https://github.com/SOHU-Co/kafka-node/pull/386)
- Recreate broker sockets to work around nodejs socket issue #4417 [#385](https://github.com/SOHU-Co/kafka-node/pull/385)
- Fixes [#319](https://github.com/SOHU-Co/kafka-node/issues/319) UnknownTopicOrPartition error when reassigning topic [#384](https://github.com/SOHU-Co/kafka-node/pull/384)
- Add **offset#getLatestOffsets** function to get all the latest offsets from a group of topics populating those topics partitions [#372](https://github.com/SOHU-Co/kafka-node/pull/372)

## 2016-05-27, Version 0.3.3
- Fix type error while producing messages [#360](https://github.com/SOHU-Co/kafka-node/pull/360)
- Update README [#307](https://github.com/SOHU-Co/kafka-node/pull/307) and [#352](https://github.com/SOHU-Co/kafka-node/pull/352)
- Add contributing guidelines [#346](https://github.com/SOHU-Co/kafka-node/pull/346)
- Make Snappy an optional dependency [#347](https://github.com/SOHU-Co/kafka-node/pull/347)

## 2016-02-21, Version 0.3.2
- Fix client socket when closing and error handling [#314](https://github.com/SOHU-Co/kafka-node/pull/314)
- Make `commit()` handle case when only callback is passed [#306](https://github.com/SOHU-Co/kafka-node/pull/306)
- Fix typo in offset.js [#304](https://github.com/SOHU-Co/kafka-node/pull/304)

## 2016-01-09, Version 0.3.1
- Buffer batch for async producers [#262](https://github.com/SOHU-Co/kafka-node/pull/262)

## 2016-01-08, Version 0.3.0
- Add partitions to producer [#260](https://github.com/SOHU-Co/kafka-node/pull/260)

## 2015-05-11, Version 0.2.27
- Deps: upgrade snappy to 3.2.0
- Zookeeper#listConsumers: ignore error when there is no such node in zookeeper

## 2015-04-23, Version 0.2.26
- Fix: add callback to consumer.autoCommit method [#198](https://github.com/SOHU-Co/kafka-node/pull/198)
- Emit an error when there is a problem with the socket connection to the kafka broker [#196](https://github.com/SOHU-Co/kafka-node/pull/196)
- Fix: emit the error instead of silent swallow it [#193](https://github.com/SOHU-Co/kafka-node/pull/193)
- Typo in error message [#189](https://github.com/SOHU-Co/kafka-node/pull/189)

## 2015-04-01, Version 0.2.25
- Producer support `requireAcks` option [#187](https://github.com/SOHU-Co/kafka-node/pull/187)
- Update examples [#185](https://github.com/SOHU-Co/kafka-node/pull/185)

## 2015-03-20, Version 0.2.24
- Bump deps
- Refresh metadata after auto rebalance among brokers [#180](https://github.com/SOHU-Co/kafka-node/pull/180)
- Initialize partition owner with consumerId [#178](https://github.com/SOHU-Co/kafka-node/pull/178)

## 2015-03-17, Version 0.2.23
- Fix [#175](https://github.com/SOHU-Co/kafka-node/issues/175): Refresh topic metadata in Producer when broker change
- Refactor Client#refreshMetadata method
- Add the missing semicolons, no offense, just keep style.
- Fix [#170](https://github.com/SOHU-Co/kafka-node/issues/170): In case of `offsetOutOfRange`, the consumer should be paused.
- Fix [#169](https://github.com/SOHU-Co/kafka-node/issues/169): When paused why try to fetch every 1000 ms?
- Ref: remove unused variables.
