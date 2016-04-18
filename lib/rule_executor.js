"use strict";

const P = require('bluebird');

/**
 * A rule executor managing matching and execution of a single rule
 */
class RuleExecutor {
    /**
     * Creates a new instance of a rule executor
     *
     * @param {Rule} rule
     * @param {KafkaFactory} kafkaFactory
     * @param {Object} hyper
     * @param {function} log
     * @constructor
     */
    constructor(rule, kafkaFactory, hyper, log) {
        this.rule = rule;
        this.kafkaFactory = kafkaFactory;
        this.hyper = hyper;
        this.log = log;
    }

    _setConsumerLoggers(consumer, ruleName, topic) {
        consumer.on('topics_changed', (topicList) => {
            // only one topic can be subscribed to by this client
            if (topicList && topicList.length) {
                this.log(`info/subscription/${ruleName}`, {
                    rule: { name: ruleName, topic },
                    msg: `Listening to ${topicList[0]}`
                });
            } else {
                this.log(`info/subscription/${ruleName}`, {
                    rule: { name: ruleName, topic },
                    msg: `Lost ownership of ${topic}`
                });
            }
        });
        consumer.on('error', (err) => this.log(`warn/error/${ruleName}`, {
            err,
            rule: { name: ruleName, topic }
        }));
        return consumer;
    }

    _exec(event) {
        const rule = this.rule;
        if (!rule.test(event)) {
            // no match, drop the message
            this.log(`debug/${rule.name}`, { msg: 'Dropping event message', event: event });
            return P.resolve();
        }

        this.log(`trace/${rule.name}`, { msg: 'Event message received', event: event });

        const expander = {
            message: event,
            match: rule.expand(event)
        };
        return P.each(rule.exec, (tpl) => this.hyper.request(tpl.expand(expander)));
    }

    _safeParse(message) {
        try {
            return JSON.parse(message);
        } catch (e) {
            this.log(`error/${this.rule.name}`, e);
        }
    }

    subscribe() {
        const rule = this.rule;
        const client = this.kafkaFactory.newClient();
        return this.kafkaFactory.newConsumer(client, rule.topic, `change-prop-${rule.name}`)
        .then((consumer) => {
            this.consumer = this._setConsumerLoggers(consumer, rule.name, rule.topic);

            // Create a special queue for retries and continue and subscribe to it
            const retryTopicName = rule.topic + '_' + rule.name + '.retry';
            return this.kafkaFactory.newProducer(this.kafkaFactory.newClient())
            .then((producer) => {
                this.retryProducer = producer;
                return producer.createTopicsAsync([ retryTopicName ], false);
            })
            .then(() => this.kafkaFactory.newConsumer(this.kafkaFactory.newClient(),
                retryTopicName, `change-prop-${retryTopicName}`))
            .then((consumer) => {
                this.retryConsumer = this._setConsumerLoggers(consumer, rule.name, retryTopicName);
                this.retryConsumer.on('message', (msg) => {
                    const message = this._safeParse(msg.value);
                    if (!message) {
                        // Don't retry if we can't parse an event, just log.
                        return;
                    }

                    if (message.retryCount >= (this.rule.spec.retry_limit || 3)) {
                        this.log(`error/${rule.name}`, {
                            message: 'Retry count exceeded',
                            event: message
                        });
                    } else {
                        return this._exec(message.value)
                        .then(() => this.retryConsumer.commitAsync())
                        .catch(() => P.delay(this.rule.spec.retry_delay || 500)
                        .then(() => {
                            message.retryCount = message.retryCount + 1;
                            return this.retryProducer.sendAsync([{
                                topic: retryTopicName,
                                messages: [ JSON.stringify(message) ]
                            }])
                            .then(() => this.retryConsumer.commitAsync());
                        }));
                    }
                });
            })
            .then(() => this.consumer.on('message', (msg) => {
                const msgObj = this._safeParse(msg.value);
                if (!msgObj) {
                    // Don't retry if we can't parse an event, just log.
                    return;
                }

                return this._exec(msgObj)
                .then(() => this.consumer.commitAsync())
                .catch(() =>
                    P.delay(this.rule.spec.retry_delay || 500)
                    .then(() =>
                        this.retryProducer.sendAsync([{
                            topic: retryTopicName,
                            messages: [JSON.stringify({
                                retryCount: 1,
                                value: msgObj
                            })]
                        }])
                        .then(() => this.consumer.commitAsync())
                    )
                );
            }));
        });
    }
}

module.exports = RuleExecutor;