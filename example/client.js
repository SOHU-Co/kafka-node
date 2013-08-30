'use strict';

var Client = require('../lib/client');

var client = new Client();

console.log(client.nextPartition());
console.log(client.nextPartition());
console.log(client.nextPartition());
console.log(client.nextPartition());
console.log(client.nextPartition());


