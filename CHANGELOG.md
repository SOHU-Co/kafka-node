# kafka-node CHANGELOG

## 2018-04-27, Version 2.6.1
* Fix issue where duplicate messages are received when connecting to multiple brokers (restored dedicated consumer socket) [#956](https://github.com/SOHU-Co/kafka-node/pull/956)

## 2018-04-24, Version 2.6.0
* Fix issue during the initial connection phase can end prematurely when metadata request failed [#920](https://github.com/SOHU-Co/kafka-node/pull/920)
* Add `addTopics` method to the `ConsumerGroup` [#914](https://github.com/SOHU-Co/kafka-node/pull/914)
* Fix issue where yielding a result in `onRebalance` in `ConsumerGroup` leads to an exception being thrown [#922](https://github.com/SOHU-Co/kafka-node/pull/922)
* Add support for Kafka Fetch versions 1 and 2 this enables produced timestamps to be read from the Consumer (Kafka 0.10+). Fetches also now share socket to the broker with other kafka requests (previously fetches were on a dedicated socket) [#871](https://github.com/SOHU-Co/kafka-node/pull/871)
* Add support to auto commit on first join `ConsumerGroup` configured using `commitOffsetsOnFirstJoin` [#897](https://github.com/SOHU-Co/kafka-node/pull/897)
* Add support for Buffers as keys, which is useful for Avro encoded keys [#932](https://github.com/SOHU-Co/kafka-node/pull/932)


## 2018-04-09, Version 2.5.0
* Explicitly cast key to string in hashCode function for `KeyedPartitioner` [#870](https://github.com/SOHU-Co/kafka-node/pull/870)
* For consumer fetch loop we now clear `socket.waiting` before invoking callbacks [#819](https://github.com/SOHU-Co/kafka-node/pull/819)
* Add Support for IPv6 [#818](https://github.com/SOHU-Co/kafka-node/pull/818)
* Clear internal topicPayload array if no topic partitions are assigned to the `ConsumerGroup` [#888](https://github.com/SOHU-Co/kafka-node/pull/888)
* Fix Stale Commit Queue for `ConsumerGroupStream` [#891](https://github.com/SOHU-Co/kafka-node/pull/891)
* For rebalance case `ConsumerGroup` will try to commit before joining a group. Added `onRebalance` callback for manual commit users [#889](https://github.com/SOHU-Co/kafka-node/pull/889)

## 2018-02-06, Version 2.4.1

* Fix issue where error callbacks for broker requests are not called when connection is closed [#863](https://github.com/SOHU-Co/kafka-node/pull/863)

## 2018-02-06, Version 2.4.0

* Add compability to `Client` with broker configurations that separates external and internal traffic [#860](https://github.com/SOHU-Co/kafka-node/pull/860)
* Fix issue where `updateMetadata()` wipes out entire topic metadata when only updating specific topics [#857](https://github.com/SOHU-Co/kafka-node/pull/857)

## 2018-01-16, Version 2.3.2

* Fix issue with `ConsumerGroupStream` where lag stays at one and resuming the consumer re-reads the last read message [#850](https://github.com/SOHU-Co/kafka-node/pull/850)

## 2018-01-7, Version 2.3.1
* Fix consumer example [#842](https://github.com/SOHU-Co/kafka-node/pull/842)
* Fix issue where ConsumerGroupStream will autoCommit when the stream is explicitly closed [#843](https://github.com/SOHU-Co/kafka-node/pull/843)
* Fix missing `highWaterOffset` using compression [#821](https://github.com/SOHU-Co/kafka-node/pull/821)
* Fix Snappy buffer.from error using node 4 [#827](https://github.com/SOHU-Co/kafka-node/pull/827)
* Fix `fromOffset` option not working in ConsumerStream [#794](https://github.com/SOHU-Co/kafka-node/pull/794)
* Fixed issue where producer send failures failed to request refresh of the metadata [#810](https://github.com/SOHU-Co/kafka-node/pull/810)

## 2017-11-17, Version 2.3.0

* Add support for `ListGroups` and `DescribeGroups` protocol. They can be used through the new [Admin](https://github.com/SOHU-Co/kafka-node#admin) interface [#770](https://github.com/SOHU-Co/kafka-node/pull/770)
* Fixed missing callback call in `commit` method of `ConsumerGroupStream` [#776](https://github.com/SOHU-Co/kafka-node/pull/776)
* Flush queued commits of `ConsumerGroupStream` on tail end of auto-commit time-out [#775](https://github.com/SOHU-Co/kafka-node/pull/775)

## 2017-09-11, Version 2.2.3
* Improved recovery of `ConsumerGroup` from broker down and network issues [#758](https://github.com/SOHU-Co/kafka-node/pull/758)
* Upgrade to snappy 3 [#760](https://github.com/SOHU-Co/kafka-node/pull/760)

## 2017-08-31, Version 2.2.2
* Fix issue where connections disconnected from being idle will never reinitialize when using `KafkaClient` [#752](https://github.com/SOHU-Co/kafka-node/pull/752)
* Fix callback of producer send never being called when connection loss occurs with Broker using `KafkaClient` [#751](https://github.com/SOHU-Co/kafka-node/pull/751)

## 2017-08-21, Version 2.2.1
* Fix duplicate messages emitted when using `KafkaClient` and consuming topic/partition that span multiple brokers [#747](https://github.com/SOHU-Co/kafka-node/pull/747)
* Instead of failing silently consumers now emits a error when trying to consume messages that exceeds `fetchMaxBytes` [#744](https://github.com/SOHU-Co/kafka-node/pull/744)

## 2017-08-08, Version 2.2.0
* Fix decoding of messages produced using `KafkaClient` with timestamps [#736](https://github.com/SOHU-Co/kafka-node/pull/736)
* Add `Writable` stream `ProducerStream` [#734](https://github.com/SOHU-Co/kafka-node/pull/734)

## 2017-08-03, Version 2.1.0
* Add two `Readable` streams `ConsumerGroupStream` and `ConsumerStream` [#732](https://github.com/SOHU-Co/kafka-node/pull/732)
* Add V1 and V2 Produce request (adds client timestamps) to `KafkaClient` only supported in kafka v0.9 and v0.10 [#730](https://github.com/SOHU-Co/kafka-node/pull/730)

## 2017-07-13, Version 2.0.1
* Fix unreferenced method call in Client/KafkaClient [#708](https://github.com/SOHU-Co/kafka-node/pull/708)

## 2017-07-08, Version 2.0.0
* Fix out of range error causing idle consumers to crash. This was a performance improvement made in 1.6.1 and reverted in 1.6.2 [#672](https://github.com/SOHU-Co/kafka-node/pull/672)
* Add [KafkaClient](https://github.com/SOHU-Co/kafka-node#kafkaclient) a version of `Client` that connects directly to Kafka brokers instead of zookeeper for discovery. [#691](https://github.com/SOHU-Co/kafka-node/pull/691)

####  Producer/Consumer Key Changes [#704](https://github.com/SOHU-Co/kafka-node/pull/704)
* Fix issue where a specified `key` attribute in producer payload was missing from the message
* Fix issue where a falsey key value (such as an empty string or `0`) was not able to be saved into the message. `null` or `undefined` key value will return as `null`
* Fix issue where key of `Buffer` value was not able to be saved into the message
* **BREAKING CHANGE** The `key` is decoded as a `string` by default. Previously was a `Buffer`. The preferred encoding for the key can be defined by the `keyEncoding` option on any of the consumers and will fallback to `encoding` if omitted

## 2017-05-08, Version 1.6.2
* Reverting performance changes using BufferList since it's causing idle consumers to crash [#670](https://github.com/SOHU-Co/kafka-node/pull/670)

## 2017-05-04, Version 1.6.1
* Fix `Offset` calling `.fetchOffset` methods not yielding callback when the topic does not exist [#662](https://github.com/SOHU-Co/kafka-node/pull/662)
* Improved performance of Client using `BufferList` instead of `Buffer.slice` [#654](https://github.com/SOHU-Co/kafka-node/pull/654)

## 2017-03-16, Version 1.6.0
* Add ability to provide your own custom partitioner implementation (see docs for Producer) [#625](https://github.com/SOHU-Co/kafka-node/pull/625)
* Create topics will yield with topics created [#618](https://github.com/SOHU-Co/kafka-node/pull/618)

## 2017-02-24, Version 1.5.0
* Added `highWaterOffset` attribute to `message` and `done` results to help keep track consumer status [#610](https://github.com/SOHU-Co/kafka-node/pull/610)
* Fixed potential missing commits in HighLevelCosumer and ConsumerGroup [#613](https://github.com/SOHU-Co/kafka-node/pull/613)

## 2017-02-13, Version 1.4.0
- Logging strategy should be configurable so that `kafka-node` can be integrated into applications more easily. [#597](https://github.com/SOHU-Co/kafka-node/pull/597)
- Published NPM package should not contain testing and local development artifacts. [#598](https://github.com/SOHU-Co/kafka-node/pull/598)

## 2017-02-01, Version 1.3.4
- Producers should better recover from brokers going offline and coming back [#580](https://github.com/SOHU-Co/kafka-node/pull/580)

## 2017-01-31, Version 1.3.3
- Fix issue where `fetchEarliestOffsets` using `Offset` could fail [#572](https://github.com/SOHU-Co/kafka-node/pull/572)

## 2017-01-25, Version 1.3.2
- Fix potential stalled ConsumerGroup when a kafka broker leaves or comes back online [#574](https://github.com/SOHU-Co/kafka-node/pull/574)
- Reduce calls to commit when consumer is idle [#568](https://github.com/SOHU-Co/kafka-node/pull/568)
- Update lodash 4  [#565](https://github.com/SOHU-Co/kafka-node/pull/565)

## 2017-01-17, Version 1.3.1
- Fix consumer group not reconnecting when a broker comes back online [#563](https://github.com/SOHU-Co/kafka-node/pull/563)
- Removed an non consumer group error from consumer group error list [#562](https://github.com/SOHU-Co/kafka-node/pull/562)

## 2017-01-12, Version 1.3.0

- Add `fetchEarliestOffsets` to `Offset` [#544](https://github.com/SOHU-Co/kafka-node/pull/544)
- Fix issue where consumer heartbeat timeout was not triggering a retry [#559](https://github.com/SOHU-Co/kafka-node/pull/559)

## 2017-01-12, Version 1.2.1

- Fix potential issue where long running consumers (includes ConsumerGroup, HighLevelConsumer, and Consumer) could throw out of bounds exception during the fetch loop [#556](https://github.com/SOHU-Co/kafka-node/pull/556)

## 2017-01-11, Version 1.2.0

**Consumer Group Changes**

- Fix issue where an error in a leaving group during close will prevent a consumer from being closed. [#551](https://github.com/SOHU-Co/kafka-node/pull/551)
- Fix issue with heartbeat where consumer will continue to send heartbeats at regular intervals even when previous ones did not resolve [#547](https://github.com/SOHU-Co/kafka-node/pull/547)
- Add ability for ConsumerGroup to recover from stale offsets. This is configured by the new `outOfRangeOffset` option which takes same values as `fromOffset`. [#553](https://github.com/SOHU-Co/kafka-node/pull/553)

## 2017-01-04, Version 1.1.0

- Fixed issue with unhandled error while using Offset [#543](https://github.com/SOHU-Co/kafka-node/pull/543)
- HighLevelConsumer now allows a configurable retry options used for rebalancing this is found under the `rebalanceRetry` option key [#542](https://github.com/SOHU-Co/kafka-node/pull/542)
- Fixed issue in HighLevelConsumer where an offset of 0 for new topics was not committed [#529](https://github.com/SOHU-Co/kafka-node/pull/529)
- Upgraded `nested-error-stacks` dependency for node 7 compatibility [#540](https://github.com/SOHU-Co/kafka-node/pull/540)

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
