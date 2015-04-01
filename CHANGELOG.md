# kafka-node CHANGELOG

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
