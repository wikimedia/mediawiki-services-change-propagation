"use strict";

const P = require('bluebird');
const uuid = require('cassandra-uuid').TimeUuid;

const DEFAULT_RETRY_DELAY = 500;
const DEFAULT_RETRY_LIMIT = 3;

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

    _retryTopicName() {
        return this.rule.topic + '.retry';
    }

    _isLimitExceeded(message) {
        if (message.retries_left <= 0) {
            this.log(`error/${this.rule.name}`, {
                message: 'Retry count exceeded',
                event: message
            });
            return true;
        }
        return false;
    }
    /**
     * Set's up a consumer and a producer for a retry queue
     *
     * @private
     */
    _setUpRetryTopic() {
        const retryTopicName = this._retryTopicName();

        return this.kafkaFactory.newProducer(this.kafkaFactory.newClient())
        .then((producer) => {
            this.retryProducer = producer;
            return this.kafkaFactory.newConsumer(this.kafkaFactory.newClient(),
                retryTopicName, `change-prop-${retryTopicName}-${this.rule.name}`);
        })
        .then((consumer) => {
            this.retryConsumer = this._setConsumerLoggers(consumer, this.rule.name, retryTopicName);
            this.retryConsumer.on('message', (msg) => {
                return P.try(() => {
                    const message = this._safeParse(msg.value);
                    if (!message) {
                        // Don't retry if we can't parse an event, just log.
                        return;
                    }

                    if (message.consumer_id !== this._consumerId()) {
                        // Not our business, don't care
                        return;
                    }

                    if (this._isLimitExceeded(message)) {
                        // We've don our best, give up
                        return;
                    }

                    return this._exec(message.original_event)
                    .catch((e) => {
                        if (this._shouldRetry(e)) {
                            return this._retry(this._constructRetryMessage(message.original_event,
                                e, message.retries_left - 1));
                        }
                    });
                })
                .then(() => this.retryConsumer.commitAsync());
            });
        });
    }

    _shouldRetry(errorRes) {
        if (this.rule.spec.retry_on) {
            return this.rule.spec.retry_on.indexOf(errorRes.status) !== -1;
        }
        return true;
    }

    _retry(retryMessage) {
        return P.delay(this.rule.spec.retry_delay || DEFAULT_RETRY_DELAY)
        .then(() => this.retryProducer.sendAsync([{
            topic: this._retryTopicName(),
            messages: [ JSON.stringify(retryMessage) ]
        }]));
    }

    _consumerId() {
        return 'change-prop#' + this.rule.name;
    }

    _constructRetryMessage(event, errorRes, retriesLeft) {
        const now = new Date();
        const error = {
            status: errorRes.status
        };
        if (errorRes.body) {
            error.type = errorRes.body.type;
            error.method = errorRes.body.method;
            error.uri = errorRes.body.uri;
            error.title = errorRes.body.title;
            error.detail = errorRes.body.detail;
        }

        return {
            meta: {
                topic: this._retryTopicName(),
                schema_uri: 'retry/1',
                uri: event.meta.uri,
                request_id: event.meta.request_id,
                id: uuid.fromDate(now),
                dt: now.toISOString(),
                domain: event.meta.domain
            },
            consumer_id: this._consumerId(),
            retries_left: retriesLeft === undefined ?
                (this.rule.spec.retry_limit || DEFAULT_RETRY_LIMIT) : retriesLeft,
            original_event: event,
            error: error
        };
    }

    subscribe() {
        const rule = this.rule;
        const client = this.kafkaFactory.newClient();
        return this._setUpRetryTopic()
        .then(() => {
            return this.kafkaFactory.newConsumer(client, rule.topic, `change-prop-${rule.name}`)
            .then((consumer) => {
                this.consumer = this._setConsumerLoggers(consumer, rule.name, rule.topic);
                this.consumer.on('message', (msg) => {
                    return P.try(() => {
                        const msgObj = this._safeParse(msg.value);
                        if (!msgObj) {
                            // Don't retry if we can't parse an event, just log.
                            return;
                        }
                        return this._exec(msgObj)
                        .catch((e) => {
                            if (this._shouldRetry(e)) {
                                return this._retry(this._constructRetryMessage(msgObj, e));
                            }
                        });
                    })
                    .then(() => this.consumer.commitAsync());
                });
            });
        });
    }
}

module.exports = RuleExecutor;