"use strict";

const ChangeProp = require('../utils/changeProp');
const KafkaFactory = require('../../lib/kafka_factory');
const nock = require('nock');
const uuid = require('cassandra-uuid').TimeUuid;
const preq = require('preq');
const Ajv = require('ajv');
const assert = require('assert');
const yaml = require('js-yaml');

describe('Basic rule management', () => {
    const changeProp = new ChangeProp('config.test.yaml');
    const kafkaFactory = new KafkaFactory({
        uri: 'localhost:2181/', // TODO: find out from the config
        clientId: 'change-prop-test-suite'
    });
    let producer;
    let retrySchema;

    before(() => {
        return kafkaFactory.newProducer(kafkaFactory.newClient())
        .then((newProducer) => {
            producer = newProducer;
            return producer.createTopicsAsync([
                'test_topic_simple_test_rule',
                'change-prop.retry.test_topic_simple_test_rule',
                'test_topic_kafka_producing_rule'
            ], false)
        })
        .then(() => changeProp.start())
        .then(() => preq.get({
                uri: 'https://raw.githubusercontent.com/wikimedia/mediawiki-event-schemas/master/jsonschema/retry/1.yaml'
        }))
        .then((res) => retrySchema = yaml.safeLoad(res.body));
    });

    function eventWithMessage(message) {
        return {
            meta: {
                topic: 'test_topic_simple_test_rule',
                schema_uri: 'schema/1',
                uri: '/sample/uri',
                request_id: uuid.now(),
                id: uuid.now(),
                dt: new Date().toISOString(),
                domain: 'test_domain'
            },
            message: message
        }
    }

    it('Should call simple executor', () => {
        const service = nock('http://mock.com', {
            reqheaders: {
                test_header_name: 'test_header_value',
                'content-type': 'application/json'
            }
        })
        .post('/', {
            'test_field_name': 'test_field_value',
            'derived_field': 'test'
        }).reply({});

        return producer.sendAsync([{
            topic: 'test_topic_simple_test_rule',
            messages: [
                JSON.stringify(eventWithMessage('this_will_not_match')),
                JSON.stringify(eventWithMessage('test')) ]
        }])
        .delay(100)
        .then(() => service.done())
        .finally(() => nock.cleanAll());
    });

    it('Should support producing to topics on exec', function() {
        var service = nock('http://mock.com', {
            reqheaders: {
                test_header_name: 'test_header_value',
                'content-type': 'application/json'
            }
        })
        .post('/', {
            'test_field_name': 'test_field_value',
            'derived_field': 'Created an event'
        }).reply({});

        return producer.sendAsync([{
            topic: 'test_topic_kafka_producing_rule',
            messages: [ JSON.stringify({
                produce_to_topic: 'test_topic_simple_test_rule'
            }) ]
        }])
        .delay(100)
        .then(function() { service.done(); })
        .finally(function() { nock.cleanAll(); });
    });

    it('Should retry simple executor', () => {
        const service = nock('http://mock.com', {
            reqheaders: {
                test_header_name: 'test_header_value',
                'content-type': 'application/json'
            }
        })
        .post('/', {
            'test_field_name': 'test_field_value',
            'derived_field': 'test'
        }).reply(500, {})
        .post('/', {
            'test_field_name': 'test_field_value',
            'derived_field': 'test'
        }).reply(200, {});

        return producer.sendAsync([{
            topic: 'test_topic_simple_test_rule',
            messages: [ JSON.stringify(eventWithMessage('test')) ]
        }])
        .delay(300)
        .then(() => service.done())
        .finally(() => nock.cleanAll());
    });

    it('Should emit valid retry message', (done) => {
        // No need to emit new messages, we will use on from previous test
        return kafkaFactory.newConsumer(kafkaFactory.newClient(),
            'change-prop.retry.test_topic_simple_test_rule',
            'change-prop-test-consumer')
        .then((retryConsumer) => {
            retryConsumer.once('message', (message) => {
                try {
                    const ajv = new Ajv();
                    const validate = ajv.compile(retrySchema);
                    var valid = validate(JSON.parse(message.value));
                    if (!valid) {
                        done(new assert.AssertionError({
                            message: ajv.errorsText(validate.errors)
                        }));
                    } else {
                        done();
                    }
                } catch(e) {
                    done(e);
                }
            });
        });
    });

    it('Should retry simple executor no more than limit', () => {
        const service = nock('http://mock.com', {
            reqheaders: {
                test_header_name: 'test_header_value',
                'content-type': 'application/json'
            }
        })
        .post('/', {
            'test_field_name': 'test_field_value',
            'derived_field': 'test'
        }).times(3).reply(500, {});

        return producer.sendAsync([{
            topic: 'test_topic_simple_test_rule',
            messages: [ JSON.stringify(eventWithMessage('test')) ]
        }])
        .delay(300)
        .then(() => service.done())
        .finally(() => nock.cleanAll());
    });

    it('Should not retry if retry_on not matched', () => {
        const service = nock('http://mock.com', {
            reqheaders: {
                test_header_name: 'test_header_value',
                'content-type': 'application/json'
            }
        })
        .post('/', {
            'test_field_name': 'test_field_value',
            'derived_field': 'test'
        }).reply(404, {});

        return producer.sendAsync([{
            topic: 'test_topic_simple_test_rule',
            messages: [ JSON.stringify(eventWithMessage('test')) ]
        }])
        .delay(300)
        .then(() => service.done())
        .finally(() => nock.cleanAll());
    });

    it('Should not crash with unparsable JSON', () => {
        const service = nock('http://mock.com', {
            reqheaders: {
                test_header_name: 'test_header_value',
                'content-type': 'application/json'
            }
        })
        .post('/', {
            'test_field_name': 'test_field_value',
            'derived_field': 'test'
        }).reply(200, {});

        return producer.sendAsync([{
            topic: 'test_topic_simple_test_rule',
            messages: [ 'non-parsable-json', JSON.stringify(eventWithMessage('test')) ]
        }])
        .delay(100)
        .then(() => service.done())
        .finally(() => nock.cleanAll());
    });

    after(() => changeProp.stop());
});