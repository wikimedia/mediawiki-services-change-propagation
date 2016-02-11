"use strict";


/**
 * restbase-mod-queue-kafka main entry point
 */


var P = require('bluebird');
var kafka = P.promisifyAll(require('kafka-node'));
var uuid = require('cassandra-uuid');
var HyperSwitch = require('hyperswitch');
var Template = HyperSwitch.Template;


function Kafka(options) {

    this.log = options.log || function() {};
    this.conf = {
        host: options.host || 'localhost',
        port: options.port || 2181,
        clientId: options.client_id || 'change-propagation',
        rules: options.templates || {}
    };
    this.conf.connString = this.conf.host + ':' + this.conf.port;
    this.rules = {};

    // init the rules
    this._init();

}


Kafka.prototype._processRule = function(ruleName, ruleSpec) {

    var idx;
    var reqs;
    var templates = [];

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
        templates.push(new Template(req));
    }

    return {
        topic: ruleSpec.topic,
        exec: templates
    };

};


Kafka.prototype._init = function() {

    Object.keys(this.conf.rules).forEach(function(name) {
        var rule = this._processRule(name, this.conf.rules[name]);
        if (!rule) { return; }
        this.rules[name] = rule;
    }, this);

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
            self.conf.connString,
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
            self.log('debug/change-prop/message',
                { msg: 'Event received', event: event, rule: logRule }
            );
            return P.each(ruleDef.exec, P.method(function(tpl) {
                return self.hyper.request(tpl.expand({ message: event }));
            })).catch(function(err) {
                self.log('info/change-prop', err);
            });
        });
        self.conn[rule].consumer.on('topics_changed', function(topicList) {
            // only one topic can be subscribed to by this client
            if (topicList && topicList.length) {
                self.log('info/change-prop/subscription', { rule: logRule, msg: 'Listening to ' + topicList[0] });
            } else {
                self.log('info/change-prop/subscription', { rule: logRule, msg: 'Lost ownership of ' + ruleDef.topic });
            }
        });
        self.conn[rule].consumer.on('error', function(err) {
            self.log('warn/change-prop/error', { err: err, rule: logRule });
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

