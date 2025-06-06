'use strict';

const P = require('bluebird');
const timers = require('node:timers/promises');

const BaseExecutor = require('./base_executor');

/**
 * A rule executor managing matching and execution of a single rule
 */
class RetryExecutor extends BaseExecutor {
    get subscribeTopics() {
        return this.rule.topics.map(topic => `${ this.kafkaFactory.consumeDC }.${ this.retryStreamName(topic) }`);
    }

    statName(event) {
        return this._hyper.metrics.normalizeName(
            `${ this.rule.name }-${ event.meta.stream.replace(/\./g, '_') }_retry`);
    }

    async _delay(message) {
        const spec = this.rule.spec;
        const absoluteDelay = spec.retry_delay *
            Math.pow(spec.retry_factor, spec.retry_limit - message.retries_left);
        if (!message.meta.dt || !Date.parse(message.meta.dt)) {
            // No DT on the message, there's nothing we can do
            await timers.setTimeout(absoluteDelay);
        }
        const delayFromNow = (Date.parse(message.meta.dt) + absoluteDelay) - Date.now();
        if (delayFromNow > 0) {
            await timers.setTimeout(delayFromNow);
        }
    }

    getHandler(message) {
        if (!message) {
            // Don't retry if we can't parse an event, just log.
            return undefined;
        }

        if (message.emitter_id !== this.emitterId()) {
            // Not our business, don't care
            return undefined;
        }

        if (this._isLimitExceeded(message)) {
            // We've don our best, give up
            return undefined;
        }
        const handlerIndex = this._test(message.original_event);
        if (handlerIndex === -1) {
            return undefined;
        }
        return this.rule.getHandler(handlerIndex);
    }

    async processMessage(message, handler) {
        await this._delay(message);
        try {
            return await this._exec(
                message.original_event, handler, new Date(message.meta.dt), message
            );
        } catch (err) {
            const e = BaseExecutor.decodeError(err);

            const retryMessage = this._constructRetryMessage(message.original_event,
                e, message.retries_left - 1, message);
            /*
            JobQueue specific.
            If the DB is in readonly mode, the `x-readonly` header will be set to true
            by the job executor. In that case we want to delay a retry even more to avoid
            unnecessary load on the job runners.
             */
            if (e.headers && e.headers['x-readonly']) {
                await timers.setTimeout(Math.ceil(30000 + Math.random() * 30000));
            }
            return this._catch(message, retryMessage, e);
        }
    }

    // Don't deduplicate retries
    _dedupeMessage(expander) {
        return P.resolve(false);
    }

}

module.exports = RetryExecutor;
