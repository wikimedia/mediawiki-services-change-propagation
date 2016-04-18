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

    _exec(event) {
        const rule = this.rule;
        const expander = {
            message: event,
            match: null
        };

        if (!rule.test(event)) {
            // no match, drop the message
            this.log(`debug/${rule.name}`, { msg: 'Dropping event message', event: event });
            return;
        }

        this.log(`trace/${rule.name}`, { msg: 'Event message received', event: event });
        expander.match = rule.expand(event);

        return P.each(rule.exec, (tpl) => this.hyper.request(tpl.expand(expander)));
    }

    subscribe() {
        const rule = this.rule;
        const logRule = { name: rule.name, topic: rule.topic };
        const client = this.kafkaFactory.newClient();
        return this.kafkaFactory.newConsumer(client, rule.topic, `change-prop-${rule.name}`)
        .then((consumer) => {
            this.consumer = consumer;
            consumer.on('message', (message) =>
                P.try(() =>
                    this._exec(JSON.parse(message.value)))
                .catch((err) =>
                    this.log(`info/${rule.name}`, err)));
            consumer.on('topics_changed', (topicList) => {
                // only one topic can be subscribed to by this client
                if (topicList && topicList.length) {
                    this.log(`info/subscription/${rule.name}`, {
                        rule: logRule,
                        msg: `Listening to ${topicList[0]}`
                    });
                } else {
                    this.log(`info/subscription/${rule.name}`, {
                        rule: logRule,
                        msg: `Lost ownership of ${rule.topic}`
                    });
                }
            });
            consumer.on('error', (err) => this.log(`warn/error/${rule.name}`, {
                err,
                rule: logRule
            }));
        });
    }

}

module.exports = RuleExecutor;