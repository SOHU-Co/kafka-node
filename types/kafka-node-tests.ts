import * as kafka from '..';

/**
 * KAFKA CLIENT
 */

const basicKafkaClient = new kafka.KafkaClient();

const optionsKafkaClient = new kafka.KafkaClient({
  kafkaHost: 'localhost:9092',
  connectTimeout: 1000,
  requestTimeout: 1000,
  autoConnect: true,
  sslOptions: {},
  clientId: 'client id',
  connectRetryOptions: {
    retries: 5, factor: 0, minTimeout: 1000, maxTimeout: 1000, randomize: true
  }
});

optionsKafkaClient.connect();

/**
 * KAFKA PRODUCER
 */
const optionsProducer = new kafka.Producer(basicKafkaClient, { requireAcks: 0, ackTimeoutMs: 0, partitionerType: 0 });

const producer = new kafka.Producer(basicKafkaClient);
producer.on('error', (error: Error) => { });
producer.on('ready', () => {
  const messages = [
    { topic: 'topicName', messages: ['message body'], partition: 0, attributes: 2 },
    { topic: 'topicName', messages: ['message body'], partition: 0 },
    { topic: 'topicName', messages: ['message body'], attributes: 0 },
    { topic: 'topicName', messages: ['message body'] },
    { topic: 'topicName', messages: [new kafka.KeyedMessage('key', 'message')] }
  ];

  producer.send(messages, (err: Error) => { });
  producer.send(messages, (err: Error, data: any) => { });

  producer.createTopics(['t'], true, (err: Error, data: any) => { });
  producer.createTopics(['t'], (err: Error, data: any) => { });
  producer.createTopics(['t'], false, () => { });
  producer.close();
});

/**
 * KAFKA HIGH LEVEL PRODUCER
 */
const highLevelProducer = new kafka.HighLevelProducer(basicKafkaClient);

highLevelProducer.on('error', (error: Error) => { });
highLevelProducer.on('ready', () => {
  const messages = [
    { topic: 'topicName', messages: ['message body'], attributes: 2 },
    { topic: 'topicName', messages: ['message body'], partition: 0 },
    { topic: 'topicName', messages: ['message body'], attributes: 0 },
    { topic: 'topicName', messages: ['message body'] },
    { topic: 'topicName', messages: [new kafka.KeyedMessage('key', 'message')] }
  ];

  highLevelProducer.send(messages, (err: Error) => { });
  highLevelProducer.send(messages, (err: Error, data: any) => { });

  producer.createTopics(['t'], true, (err: Error, data: any) => { });
  producer.createTopics(['t'], (err: Error, data: any) => { });
  producer.createTopics(['t'], false, () => { });
  producer.close();
});

/**
 * KAFKA CONSUMER
 */
const fetchRequests = [{ topic: 'awesome' }];
const consumer = new kafka.Consumer(basicKafkaClient, fetchRequests, { groupId: 'abcde', autoCommit: true });

consumer.on('error', (error: Error) => { });
consumer.on('offsetOutOfRange', (error: Error) => { });
consumer.on('message', (message: kafka.Message) => {
  const topic = message.topic;
  const value = message.value;
  const offset = message.offset;
  const partition = message.partition;
  const highWaterOffset = message.highWaterOffset;
  const key = message.key;
});

consumer.addTopics(['t1', 't2'], (err: any, added: any) => { });
consumer.addTopics([{ topic: 't1', offset: 10 }], (err: any, added: any) => { }, true);

consumer.removeTopics(['t1', 't2'], (err: any, removed: number) => { });
consumer.removeTopics('t2', (err: any, removed: number) => { });

consumer.commit((err: any, data: any) => { });
consumer.commit(true, (err: any, data: any) => { });

consumer.setOffset('topic', 0, 0);

consumer.pause();
consumer.resume();
consumer.pauseTopics(['topic1', { topic: 'topic2', partition: 0 }]);
consumer.resumeTopics(['topic1', { topic: 'topic2', partition: 0 }]);

consumer.close(true, () => { });
consumer.close((err: any) => { });

/**
 * KAFKA CONSUMER GROUP
 */
const ackBatchOptions = { noAckBatchSize: 1024, noAckBatchAge: 10 };
const cgOptions: kafka.ConsumerGroupOptions = {
  kafkaHost: 'localhost:9092',
  batch: ackBatchOptions,
  groupId: 'groupID',
  id: 'consumerID',
  encoding: 'buffer',
  keyEncoding: 'buffer',
  sessionTimeout: 15000,
  protocol: ['roundrobin'],
  fromOffset: 'latest',
  migrateHLC: false,
  migrateRolling: true
};

const consumerGroup = new kafka.ConsumerGroup(cgOptions, ['topic1']);
consumerGroup.on('error', (err) => { });
consumerGroup.on('connect', () => { });
consumerGroup.on('message', (msg) => { });
consumerGroup.close(true, (err: Error) => { });

const offset = new kafka.Offset(basicKafkaClient);

offset.on('ready', () => { });

offset.fetch([{ topic: 't', partition: 0, time: Date.now(), maxNum: 1 }, { topic: 't' }], (err: any, data: any) => { });

offset.commit('groupId', [{ topic: 't', partition: 0, offset: 10 }], (err, data) => { });

offset.fetchCommits('groupId', [{ topic: 't', partition: 0 }], (err, data) => { });

offset.fetchLatestOffsets(['t'], (err, offsets) => { });
offset.fetchEarliestOffsets(['t'], (err, offsets) => { });
