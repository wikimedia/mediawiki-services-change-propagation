"use strict";

const kafka = require('wmf-kafka-node');
const uuid = require('cassandra-uuid');
const P = require('bluebird');

/**
 * Utility class providing high-level interfaces to kafka modules.
 */
class KafkaFactory {
    /**
     * @param {Object} kafkaConf Kafka connection configuration
     * @param {string} kafkaConf.uri Zookeper URI with host and port
     * @param {string} kafkaConf.clientId Client identification string
     * @constructor
     */
    constructor(kafkaConf) {
        this.kafkaConf = kafkaConf;
    }

    /**
     * Creates a new kafka client.
     *
     * @returns {Client}
     */
    newClient() {
        return new kafka.Client(this.kafkaConf.uri,
        `${this.kafkaConf.clientId}-${uuid.TimeUuid.now()}-${uuid.Uuid.random()}`,
        {}
        );
    }

    /**
     * Creates and initializes an new kafka producer.
     *
     * @param {Client} client a kafka client to use.
     * @param {Object} [options] producer options
     *
     * @returns {Promise<HighLevelProducer>}
     */
    newProducer(client, options) {
        return new P((resolve, reject) => {
            const producer = new kafka.HighLevelProducer(client, options || {});
            producer.once('ready', () => resolve(P.promisifyAll(producer)));
            producer.once('error', reject);
        });
    }

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
    newConsumer(client, topic, groupId, offset) {
        return new P((resolve, reject) => {
            const consumer = new kafka.HighLevelConsumer(client, [{ topic, offset }], { groupId });
            consumer.once('error', reject);
            consumer.once('rebalanced', () => resolve(consumer));
        });
    }
}

module.exports = KafkaFactory;
