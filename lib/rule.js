'use strict';

const stringify = require('json-stable-stringify');
const HyperSwitch = require('hyperswitch');
const Template = HyperSwitch.Template;

/**
 * Creates a JS function that verifies property equality
 *
 * @param catchDefinition the condition in the format of 'catch' and 'return_if' stanza
 * @returns {Function} a function that verifies the condition
 */
function _compileCatchFunction(catchDefinition) {
    function createCondition(catchCond, option) {
        if (catchCond === 'status') {
            var opt = option.toString();
            if (/^[0-9]+$/.test(opt)) {
                return `(res["${catchCond}"] === ${opt})`;
            }
            if (/^[0-9x]+$/.test(opt)) {
                return `Array.isArray(res["${catchCond}"].toString()
                    .match(/^${opt.replace(/x/g, "\\d")}$/))`;
            }
            throw new Error(`Invalid catch condition ${opt}`);
        } else {
            return `(stringify(res["${catchCond}"]) === '${stringify(option)}')`;
        }
    }

    var condition = [];
    var code;
    Object.keys(catchDefinition).forEach(function(catchCond) {
        if (Array.isArray(catchDefinition[catchCond])) {
            var orCondition = catchDefinition[catchCond].map(function(option) {
                return createCondition(catchCond, option);
            });
            condition.push('(' + orCondition.join(' || ') + ')');
        } else {
            condition.push(createCondition(catchCond, catchDefinition[catchCond]));
        }
    });
    code = `return (${condition.join(' && ')});`;
    /* jslint evil: true */
    return new Function('stringify', 'res', code).bind(null, stringify);
}

function _getMatchObjCode(obj) {
    let code = '{';
    if (obj.constructor !== Object) {
        return '';
    }
    Object.keys(obj).forEach((key) => {
        const field = obj[key];
        let fieldCode = key + ': ';
        if (field.constructor === Object) {
            fieldCode += _getMatchObjCode(field);
        } else {
            fieldCode += field;
        }
        if (code.length > 1) {
            code += ',';
        }
        code += fieldCode;
    });

    return code + '}';
}

function _compileMatch(obj, result, name, fieldName) {
    if (obj.constructor !== Object) {
        if (typeof obj !== 'string') {
            // not a string, so it has to match exactly
            result[fieldName] = obj;
            return `${name} === ${obj}`;
        }
        if (obj[0] !== '/' && obj[obj.length - 1] !== '/') {
            // not a regex, quote the string
            result[fieldName] = `'${obj}'`;
            return `${name} === '${obj}'`;
        }
        // it's a regex, we have to the test the arg
        result[fieldName] = `${obj}.exec(${name})`;
        return `${obj}.test(${name})`;
    }

    // this is an object, we need to split it into components
    const subObj = fieldName ? {} : result;
    const test = Object.keys(obj).map(
        (key) => _compileMatch(obj[key], subObj, `${name}['${key}']`, key))
    .join(' && ');
    if (fieldName) {
        result[fieldName] = subObj;
    }
    return test;
}

class Rule {
    constructor(name, spec) {
        this.name = name;
        this.spec = spec || {};

        this.topic = this.spec.topic;
        if (!this.topic) {
            throw new Error(`No topic specified for rule ${this.name}`);
        }

        if (this.spec.retry_on) {
            this.shouldRetry = _compileCatchFunction(this.spec.retry_on);
        } else {
            this.shouldRetry = function() {
                return true;
            };
        }
        this.exec = this._processExec(this.spec.exec);
        this._match = this._processMatch(this.spec.match);
    }

    /**
     * Tests the message against the compiled evaluation test
     *
     * @param {Object} message the message to test
     * @return true if no match is set for this rule or if the message matches
     */
    test(message) {
        return !this._match || !this._match.test || this._match.test(message);
    }

    /**
     * Expands the rule's match object with the given message's content
     *
     * @param {Object} message the message to use in the expansion
     * @return {Object} the object containing the expanded match portion of the rule
     */
    expand(message) {
        return this._match && this._match.expand ? this._match.expand(message) : {};
    }

    _processMatch(match) {
        if (!match) {
            // No particular match specified, so we
            // should accept all events for this topic
            return;
        }

        const obj = {};
        const test = _compileMatch(match, obj, 'message');
        try {
            return {
                /* jslint evil: true  */
                test: new Function('message', 'return ' + test),
                /* jslint evil: true  */
                expand: new Function('message', 'return ' + _getMatchObjCode(obj))
            };
        } catch (e) {
            throw new Error('Invalid match object given!');
        }
    }

    _processExec(exec) {
        if (!exec) {
            // nothing to do, the rule is a no-op
            this.noop = true;
            return;
        }

        if (!Array.isArray(exec)) {
            exec = [exec];
        }

        const templates = [];
        for (let idx = 0; idx < exec.length; idx++) {
            const req = exec[idx];
            if (req.constructor !== Object || !req.uri) {
                throw new Error(`In rule ${this.name}, request number ${idx}
                    must be an object and must have the "uri" property`);
            }
            req.method = req.method || 'get';
            req.headers = req.headers || {};
            templates.push(new Template(req));
        }
        return templates;
    }
}

module.exports = Rule;

