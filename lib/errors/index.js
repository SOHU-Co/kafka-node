module.exports = {
  ApiNotSupportedError: require('./ApiNotSupportedError'),
  BrokerNotAvailableError: require('./BrokerNotAvailableError'),
  TopicsNotExistError: require('./TopicsNotExistError'),
  FailedToRegisterConsumerError: require('./FailedToRegisterConsumerError'),
  InvalidConsumerOffsetError: require('./InvalidConsumerOffsetError'),
  FailedToRebalanceConsumerError: require('./FailedToRebalanceConsumerError'),
  InvalidConfigError: require('./InvalidConfigError'),
  SaslAuthenticationError: require('./SaslAuthenticationError'),
  ConsumerGroupErrors: [
    require('./GroupCoordinatorNotAvailableError'),
    require('./GroupLoadInProgressError'),
    require('./HeartbeatTimeoutError'),
    require('./IllegalGenerationError'),
    require('./NotCoordinatorForGroupError'),
    require('./RebalanceInProgressError'),
    require('./UnknownMemberIdError')
  ]
};
