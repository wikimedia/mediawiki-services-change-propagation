'use strict';

const BaseExecutor = require('./base_executor');
const URI = require('hyperswitch').URI;

/**
 * A rule executor managing matching and execution of a single rule
 */
class RuleExecutor extends BaseExecutor {
    get subscribeTopics() {
        return this.rule.topics.map(topic => `${ this.kafkaFactory.consumeDC }.${ topic }`);
    }

    statName(event) {
        return this._hyper.metrics.normalizeName(
            `${ this.rule.name }-${ event.meta.stream.replace(/\./g, '_') }`);
    }

    /**
     * Returns a handler to be used or undefined.
     *
     * @param {Object} message the message to process
     * @return {Function|boolean}
     */
    getHandler(message) {
        if (!message) {
            // no message we are done here
            return undefined;
        }
        const handlerIndex = this._test(message);
        if (handlerIndex === -1) {
            return undefined;
        }
        return this.rule.getHandler(handlerIndex);
    }

    async processMessage(message, handler) {
        try {
            return await this._exec(message, handler);
        } catch (err) {
            const e = BaseExecutor.decodeError(err);
            const retryMessage = this._constructRetryMessage(message, e);
            return this._catch(message, retryMessage, e);
        }
    }

    _dedupeMessage(expander) {
        let dedupeKey;
        if (this.rule.topics.length === 1) {
            dedupeKey = this.rule.name;
        } else {
            dedupeKey = `${ this.rule.name }-${ expander.message.meta.stream }`;
        }
        return this._hyper.post({
            uri: new URI(`/sys/dedupe/${ dedupeKey }`),
            body: expander.message
        })
        .get('body')
        .catch({ status: 404 }, () => false);
    }
}

module.exports = RuleExecutor;
