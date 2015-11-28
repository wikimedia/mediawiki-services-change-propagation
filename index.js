"use strict";


/**
 * restbase-mod-queue-kafka main entry point
 */


var P = require('bluebird');
var kafka = P.promisifyAll(require('kafka-node'));


function Kafka(options, rules) {

    this.log = options.log || function() {};
    this.conf = {
        host: options.host || 'localhost',
        port: options.port || 2181,
        rules: rules || {},
        topics: {}
    };
    this.conf.connString = this.conf.host + ':' + this.conf.port;

    // init the rules
    this._init();

    this.log('info/queue/kafka', { msg: 'INIT OPTS', conf: this.conf });

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
        this._processRule(name, this.conf.rules[name]);
    }, this).forEach(function(rule) {
        if (!rule) { return; }
        this.conf.topics[rule.topic] = this.conf.topics[rule.topic] || {};
        this.conf.topics[rule.topic][rule.name] = rule.exec;
    }, this);

};


module.exports = function(options, rules) {

    var kafkaMod = new Kafka(options, rules);

    return {
        /* We currently don't expose anything to RESTBase, because only static
         * rule definitions are supported. In future, this module will expose
         * routes allowing for dynamic subscriptions.
         */
        spec: {},
        operations: {}
    };

};

