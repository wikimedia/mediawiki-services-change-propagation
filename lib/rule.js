'use strict';


var HyperSwitch = require('hyperswitch');
var Template = HyperSwitch.Template;


function Rule(name, spec) {

    this.name = name;
    this.spec = spec;
    this._processRule(); 

}


Rule.prototype._processRule = function() {

    var idx;
    var reqs;
    var templates = [];
    var match = {};

    // validate the rule spec
    if (!this.spec.topic) {
        throw new Error('No topic specified for rule ' + this.name);
    }
    if (!this.spec.exec) {
        // nothing to do, the rule is a no-op
        this.noop = true;
        return;
    }
    if (!Array.isArray(this.spec.exec)) {
        this.spec.exec = [this.spec.exec];
    }

    reqs = this.spec.exec;
    for (idx = 0; idx < reqs.length; idx++) {
        var req = reqs[idx];
        if (req.constructor !== Object || !req.uri) {
            throw new Error('In rule ' + this.name + ', request number ' + idx +
                ' must be an object and must have the "uri" property');
        }
        req.method = req.method || 'get';
        req.headers = req.headers || {};
        templates.push(new Template(req));
    }

    this.topic = this.spec.topic;
    this.exec = templates;

};


module.exports = Rule;

