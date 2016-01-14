'use strict';

function groupByTopicAndPartition(payloads) {
    return payloads.reduce(function (out, p) {
        out[p.topic] = out[p.topic] || {};
        out[p.topic][p.partition] = p;
        return out;
    }, {});
}

module.exports.groupByTopicAndPartition = groupByTopicAndPartition;
