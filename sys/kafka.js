"use strict";


/**
 * restbase-mod-queue-kafka main entry point
 */


const P = require('bluebird');
const Rule = require('../lib/rule');
const KafkaFactory = require('../lib/kafka_factory');
const RuleExecutor = require('../lib/rule_executor');

class Kafka {
    constructor(options) {
        this.log = options.log || function() { };
        this.kafkaFactory = new KafkaFactory({
            uri: options.uri || 'localhost:2181/',
            clientId: options.client_id || 'change-propagation'
        });
        this.staticRules = options.templates || {};
        this.ruleExecutors = {};
    }

    setup(hyper) {
        return this.kafkaFactory.newProducer(this.kafkaFactory.newClient())
        .then((producer) => {
            this.producer = producer;
            return P.all(Object.keys(this.staticRules)
            .map((ruleName) => new Rule(ruleName, this.staticRules[ruleName]))
            .filter((rule) => !rule.noop)
            .map((rule) => {
                this.ruleExecutors[rule.name] = new RuleExecutor(rule,
                    this.kafkaFactory, hyper, this.log);
                return this.ruleExecutors[rule.name].subscribe();
            }))
            .tap(() => this.log('info/change-prop/init', 'Kafka Queue module initialised'));
        })
        .thenReturn({ status: 200 });
    }

    produce(hyper, req) {
        return this.producer.sendAsync([
            {
                topic: req.body.topic,
                messages: req.body.messages.map(JSON.stringify)
            }
        ])
        .thenReturn({ status: 201 });
    }
}

module.exports = (options) => {
    const kafkaMod = new Kafka(options);
    return {
        spec: {
            paths: {
                '/setup': {
                    put: {
                        summary: 'set up the kafka listener',
                        operationId: 'setup_kafka'
                    }
                },
                '/produce': {
                    post: {
                        summary: 'produces a message the kafka topic',
                        operationId: 'produce'
                    }
                }
            }
        },
        operations: {
            setup_kafka: kafkaMod.setup.bind(kafkaMod),
            produce: kafkaMod.produce.bind(kafkaMod)
        },
        resources: [{
            uri: '/sys/queue/setup'
        }]
    };
};

