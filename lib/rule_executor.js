"use strict";

var P = require('bluebird');

/**
 * Creates a new instance of a rule executor
 *
 * @param {Rule} rule
 * @param {KafkaFactory} kafkaFactory
 * @param {Object} hyper
 * @param {function} log
 * @constructor
 */
function RuleExecutor(rule, kafkaFactory, hyper, log) {
    this.rule = rule;
    this.kafkaFactory = kafkaFactory;
    this.hyper = hyper;
    this.log = log;
}

RuleExecutor.prototype._exec = function(event) {
    var self = this;
    var rule = self.rule;
    var expander = {
        message: event,
        match: null
    };

    if (!rule.test(event)) {
        // no match, drop the message
        this.log('debug/' + rule.name, { msg: 'Dropping event message', event: event });
        return;
    }

    this.log('trace/' + rule.name, { msg: 'Event message received', event: event });
    expander.match = rule.expand(event);

    return P.each(rule.exec, function(tpl) {
        console.log('EXEC');
        return self.hyper.request(tpl.expand(expander));
    });
};

RuleExecutor.prototype.subscribe = function() {
    var self = this;
    var rule = self.rule;
    var logRule = { name: rule.name, topic: rule.topic };
    var client = self.kafkaFactory.newClient();
    return self.kafkaFactory.newConsumer(client, rule.topic, 'change-prop-' + rule.name)
    .then(function(consumer) {
        self.consumer = consumer;
        consumer.on('message', function(message) {
            return P.try(function() {
                return self._exec(JSON.parse(message.value));
            })
            .catch(function(err) {
                self.log('info/' + rule.name, err);
            });
        });
        consumer.on('topics_changed', function(topicList) {
            // only one topic can be subscribed to by this client
            if (topicList && topicList.length) {
                self.log('info/subscription/' + rule.name,
                { rule: logRule, msg: 'Listening to ' + topicList[0] });
            } else {
                self.log('info/subscription/' + rule.name,
                { rule: logRule, msg: 'Lost ownership of ' + rule.topic });
            }
        });
        consumer.on('error', function(err) {
            self.log('warn/error/' + rule.name, { err: err, rule: logRule });
        });
    });
};

module.exports = RuleExecutor;