"use strict";

var kafka = require('wmf-kafka-node');
var uuid = require('cassandra-uuid');
var P = require('bluebird');

/**
 * Utility class providing high-level interfaces to kafka modules.
 *
 * @param {Object} kafkaConf Kafka connection configuration
 * @param {string} kafkaConf.uri Zookeper URI with host and port
 * @param {string} kafkaConf.clientId Client identification string
 * @constructor
 */
function KafkaFactory(kafkaConf) {
    this.kafkaConf = kafkaConf;
}

/**
 * Creates a new kafka client.
 *
 * @returns {Client}
 */
KafkaFactory.prototype.newClient = function() {
    return new kafka.Client(this.kafkaConf.uri,
        this.kafkaConf.clientId + '-' + uuid.TimeUuid.now() + '-' + uuid.Uuid.random(),
        {}
    );
};

/**
 * Creates and initializes an new kafka producer.
 *
 * @param {Client} client a kafka client to use.
 * @param {Object} [options] producer options
 *
 * @returns {Promise<HighLevelProducer>}
 */
KafkaFactory.prototype.newProducer = function(client, options) {
    return new P(function(resolve, reject) {
        var producer = new kafka.HighLevelProducer(client, options || {});
        producer.once('ready', function() {
            resolve(P.promisifyAll(producer));
        });
        producer.once('error', reject);
    });
};

/**
 * Creates a kafka consumer.
 *
 * @param {Client} client a kafka client to use
 * @param {string} topic a topic name to consume
 * @param {string} groupId consumer group ID
 * @param {Number} [offset] an offset which to start consumption from.
 *
 * @returns {Promise} a promise that's resolved when a consumer is ready
 */
KafkaFactory.prototype.newConsumer = function(client, topic, groupId, offset) {
    var topicConf = { topic: topic };
    if (offset !== undefined) {
        topicConf.offset = offset;
    }
    return new P(function(resolve, reject) {
        var consumer = new kafka.HighLevelConsumer(client, [ topicConf ], { groupId: groupId });
        consumer.once('error', reject);
        consumer.once('rebalanced', function() {
            resolve(consumer);
        });
    });
};

module.exports = KafkaFactory;