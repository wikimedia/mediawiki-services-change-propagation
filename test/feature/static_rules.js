"use strict";

var ChangeProp = require('../utils/changeProp');
var KafkaFactory = require('../../lib/kafka_factory');
var nock = require('nock');

describe('Basic rule management', function() {
    var changeProp = new ChangeProp('config.test.yaml');
    var kafkaFactory = new KafkaFactory({
        uri: 'localhost:2181/', // TODO: find out from the config
        clientId: 'change-prop-test-suite'
    });
    var producer;

    before(function() {
        return kafkaFactory.newProducer(kafkaFactory.newClient())
        .then(function(newProducer) {
            producer = newProducer;
            return producer.createTopicsAsync([ 'test_topic_simple_test_rule' ], false)
        })
        .then(function() {
            return changeProp.start();
        });
    });


    it('Should call simple executor', function() {
        var service = nock('http://mock.com', {
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
                JSON.stringify({ message: 'this_will_not_match' }),
                JSON.stringify({ message: 'test' }) ]
        }])
        .delay(100)
        .then(function() { service.done(); })
        .finally(function() { nock.cleanAll(); });
    });

    it('Should retry simple executor', function() {
        var service = nock('http://mock.com', {
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
            messages: [ JSON.stringify({ message: 'test' }) ]
        }])
        .delay(300)
        .then(function() { service.done(); })
        .finally(function() { nock.cleanAll(); });
    });


    after(function() { return changeProp.stop(); });


    it('Should retry simple executor no more than limit', function() {
        var service = nock('http://mock.com', {
            reqheaders: {
                test_header_name: 'test_header_value',
                'content-type': 'application/json'
            }
        })
        .post('/', {
            'test_field_name': 'test_field_value',
            'derived_field': 'test'
        }).times(2).reply(500, {});

        return producer.sendAsync([{
            topic: 'test_topic_simple_test_rule',
            messages: [ JSON.stringify({ message: 'test' }) ]
        }])
        .delay(300)
        .then(function() { service.done(); })
        .finally(function() { nock.cleanAll(); });
    });

    it('Should not crash with unparsable JSON', function() {
        var service = nock('http://mock.com', {
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
            messages: [ 'non-parsable-json', JSON.stringify({ message: 'test' }) ]
        }])
        .delay(100)
        .then(function() { service.done(); })
        .finally(function() { nock.cleanAll(); });
    });
});