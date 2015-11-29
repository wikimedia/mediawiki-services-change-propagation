"use strict";


/**
 * restbase-mod-queue-kafka main entry point
 */


var P = require('bluebird');
var kafka = P.promisifyAll(require('kafka-node'));
var uuid = require('cassandra-uuid');


function Kafka(options, rules) {

    this.log = options.log || function() {};
    this.conf = {
        host: options.host || 'localhost',
        port: options.port || 2181,
        clientId: options.client_id || 'change-propagation',
        rules: rules || {}
    };
    this.topics = {};
    this.conf.connString = this.conf.host + ':' + this.conf.port;

    // init the rules
    this._init();

}


Kafka.prototype._processRule = function(ruleName, ruleSpec) {

    var idx;
    var reqs;

    // validate the rule spec
    if (!ruleSpec.topic) {
        throw new Error('No topic specified for rule ' + ruleName);
    }
    if (!ruleSpec.exec) {
        // nothing to do, ignore this rule
        return undefined;
    }
    if (!Array.isArray(ruleSpec.exec)) {
        ruleSpec.exec = [ruleSpec.exec];
    }

    reqs = ruleSpec.exec;
    for (idx = 0; idx < reqs.length; idx++) {
        var req = reqs[idx];
        if (req.constructor !== Object || !req.uri) {
            throw new Error('In rule ' + ruleName + ', request number ' + idx +
                ' must be an object and must have the "uri" property');
        }
        req.method = req.method || 'get';
        req.headers = req.headers || {};
    }

    return {
        name: ruleName,
        topic: ruleSpec.topic,
        exec: ruleSpec.exec
    };

};


Kafka.prototype._init = function() {

    Object.keys(this.conf.rules).map(function(name) {
        return this._processRule(name, this.conf.rules[name]);
    }, this).forEach(function(rule) {
        if (!rule) { return; }
        this.topics[rule.topic] = this.topics[rule.topic] || {};
        this.topics[rule.topic][rule.name] = rule.exec;
    }, this);

};


Kafka.prototype.setup = function() {

    var self = this;

    self.conn = {};
    var p = Object.keys(self.topics).map(function(topic) {
        self.conn[topic] = {};
        self.conn[topic].client = new kafka.Client(
            self.conf.connString,
            self.conf.clientId + '-' + uuid.TimeUuid.now() + '-' + uuid.Uuid.random(),
            {}
        );
        self.conn[topic].consumer = new kafka.HighLevelConsumer(
            self.conn[topic].client,
            [{ topic: topic }],
            { groupId: 'change-prop-' + topic }
        );
        self.conn[topic].consumer.on('message', function(message) {
            self.log('debug/kafka/message',
                { msg: 'Event received', event: JSON.parse(message.value) }
            );
        });
        return new P(function(resolve) {
            self.conn[topic].consumer.on('rebalanced', function() {
                self.log('debug/kafka/topic', 'Listening to topic ' + topic);
                resolve();
            });
        });
    });

    return P.all(p).then(function() {
        self.log('info/kafka/init', 'Kafka Queue module initialised');
    });

};


module.exports = function(options, rules) {

    var kafkaMod = new Kafka(options, rules);

    return kafkaMod.setup().then(function() {
        return {
            /* We currently don't expose anything to RESTBase, because only static
            * rule definitions are supported. In future, this module will expose
            * routes allowing for dynamic subscriptions.
            */
            spec: {},
            operations: {}
        };
    });

};

