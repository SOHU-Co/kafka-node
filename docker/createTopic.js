'use strict';

const execa = require('execa');
const assert = require('assert');

function createTopic (topicName, partitions, replicas, config = '') {
  assert(topicName);
  assert(partitions && partitions > 0);
  assert(replicas && replicas > 0);

  const args = ['exec', '-T', 'kafka', 'bash', '-c'];

  if (process.env.KAFKA_VERSION === '0.9') {
    const topic = `${topicName}:${partitions}:${replicas}`;
    args.push(`KAFKA_CREATE_TOPICS=${topic} KAFKA_PORT=9092 /usr/bin/create-topics.sh`);
  } else {
    if (config) {
      config = ` --config ${config}`;
    }
    args.push(
      `/opt/kafka/bin/kafka-topics.sh --create --zookeeper zookeeper:2181 --topic ${topicName} --partitions ${partitions} --replication-factor ${replicas} ${config}`
    );
  }

  const createResult = execa('docker-compose', args);
  // createResult.stdout.pipe(process.stdout);
  return createResult;
}

module.exports = createTopic;
