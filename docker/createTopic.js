'use strict';

const execa = require('execa');
const assert = require('assert');

function createTopic (topicName, partitions, replicas) {
  assert(topicName);
  assert(partitions && partitions > 0);
  assert(replicas && replicas > 0);
  const topic = `${topicName}:${partitions}:${replicas}`;
  const createResult = execa('docker-compose', [
    'exec',
    '-T',
    'kafka',
    'bash',
    '-c',
    `KAFKA_CREATE_TOPICS=${topic} KAFKA_PORT=9092 /usr/bin/create-topics.sh`
  ]);
  // createResult.stdout.pipe(process.stdout);
  return createResult;
}

module.exports = createTopic;
