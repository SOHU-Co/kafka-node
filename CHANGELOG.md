# kafka-node CHANGELOG

## 2015-03-20, Version 0.2.24
- Bump deps
- Refresh metadata after auto rebalance among brokers #180
- Initialize partition owner with consumerId #178

## 2015-03-17, Version 0.2.23
- Fix #175: Refresh topic metadata in Producer when broker change
- Refactor Client#refreshMetadata method
- Add the missing semicolons, no offense, just keep style.
- Fix #170: In case of `offsetOutOfRange`, the consumer should be paused.
- Fix #169: When paused why try to fetch every 1000 ms?
- Ref: remove unused variables.
