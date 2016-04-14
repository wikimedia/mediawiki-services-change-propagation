"use strict";


/**
 * restbase-mod-queue-kafka main entry point
 */


var P = require('bluebird');
var Rule = require('../lib/rule');
var KafkaFactory = require('../lib/kafka_factory');
var RuleExecutor = require('../lib/rule_executor');


function Kafka(options) {
    this.log = options.log || function() {};
    this.kafkaFactory = new KafkaFactory({
        uri: options.uri || 'localhost:2181/',
        clientId: options.client_id || 'change-propagation'
    });
    this.staticRules = options.templates || {};
    this.ruleExecutors = {};
}

Kafka.prototype.setup = function(hyper) {
    var self = this;
    return P.all(
        Object.keys(self.staticRules).map(function(ruleName) {
            return new Rule(ruleName, self.staticRules[ruleName]);
        })
        .filter(function(rule) { return !rule.noop; })
        .map(function(rule) {
            self.ruleExecutors[rule.name] = new RuleExecutor(rule,
                self.kafkaFactory, hyper, self.log);
            return self.ruleExecutors[rule.name].subscribe();
        })
    )
    .then(function() {
        self.log('info/change-prop/init', 'Kafka Queue module initialised');
        return { status: 200 };
    });

};


module.exports = function(options) {

    var kafkaMod = new Kafka(options);

    return {
        spec: {
            paths: {
                '/setup': {
                    put: {
                        summary: 'set up the kafka listener',
                        operationId: 'setup_kafka'
                    }
                }
            }
        },
        operations: {
            setup_kafka: kafkaMod.setup.bind(kafkaMod)
        },
        resources: [{
            uri: '/{domain}/sys/queue/setup'
        }]
    };

};

