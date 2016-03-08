"use strict";


/**
 * restbase-mod-queue-kafka main entry point
 */


var P = require('bluebird');
var kafka = P.promisifyAll(require('wmf-kafka-node'));
var uuid = require('cassandra-uuid');
var Rule = require('../lib/rule');


function Kafka(options) {

    this.log = options.log || function() {};
    this.conf = {
        uri: options.uri || 'localhost:2181/',
        clientId: options.client_id || 'change-propagation',
        rules: options.templates || {}
    };
    this.rules = {};

    // init the rules
    this._init();

}


Kafka.prototype._init = function() {

    var self = this;

    Object.keys(this.conf.rules).forEach(function(name) {
        var rule = new Rule(name, self.conf.rules[name]);
        if (rule.noop) { return; }
        self.rules[name] = rule;
    });

};


Kafka.prototype._execEvent = function(event, rule) {

    var self = this;
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

    return P.each(rule.exec, P.method(function(tpl) {
        return self.hyper.request(tpl.expand(expander));
    })).catch(function(err) {
        self.log('info/' + rule.name, err);
    });

};


Kafka.prototype.setup = function(hyper, req) {

    var self = this;

    self.hyper = hyper;
    self.conn = {};
    var p = Object.keys(self.rules).map(function(rule) {
        var ruleDef = self.rules[rule];
        var logRule = { name: rule, topic: ruleDef.topic };
        self.conn[rule] = {};
        self.conn[rule].client = new kafka.Client(
            self.conf.uri,
            self.conf.clientId + '-' + uuid.TimeUuid.now() + '-' + uuid.Uuid.random(),
            {}
        );
        self.conn[rule].consumer = new kafka.HighLevelConsumer(
            self.conn[rule].client,
            [{ topic: ruleDef.topic }],
            { groupId: 'change-prop-' + rule }
        );
        self.conn[rule].consumer.on('message', function(message) {
            var event = JSON.parse(message.value);
            return self._execEvent(event, ruleDef);
        });
        self.conn[rule].consumer.on('topics_changed', function(topicList) {
            // only one topic can be subscribed to by this client
            if (topicList && topicList.length) {
                self.log('info/' + rule + '/subscription',
                    { rule: logRule, msg: 'Listening to ' + topicList[0] });
            } else {
                self.log('info/' + rule + '/subscription',
                    { rule: logRule, msg: 'Lost ownership of ' + ruleDef.topic });
            }
        });
        self.conn[rule].consumer.on('error', function(err) {
            self.log('warn/' + rule + '/error', { err: err, rule: logRule });
        });
        return new P(function(resolve) {
            self.conn[rule].consumer.on('rebalanced', function() {
                resolve();
            });
        });
    });

    return P.all(p).then(function() {
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

