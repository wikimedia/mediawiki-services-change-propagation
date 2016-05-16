"use strict";

/**
 * Represents a task for TaskQueue
 */
class Task {
    /**
     * Constructs a new instance of the task
     *
     * @param {HighLevelConsumer} consumer the consumer which the message was emitted by
     * @param {Object} message the event message
     * @param {Function} executor function to call to execute the task
     * @param {Function} catcher function to call if execution errors
     */
    constructor(consumer, message, executor, catcher) {
        this._consumer = consumer;
        this._message = message;
        this._executor = executor;
        this._catcher = catcher;
    }

    /**
     * A consumer which the message was emitted by
     *
     * @returns {HighLevelConsumer}
     */
    get consumer() {
        return this._consumer;
    }

    /**
     * A topic which the message belongs to
     * @returns {string}
     */
    get topic() {
        return this._message.topic;
    }

    /**
     * A partition which the message belongs to
     *
     * @returns {number}
     */
    get partition() {
        return this._message.partition;
    }

    /**
     * An offset of the message which created this task
     *
     * @returns {number}
     */
    get offset() {
        return this._message.offset;
    }

    /**
     * A consumer group which the task belongs to
     *
     * @returns {string}
     */
    get consumerGroup() {
        return this._consumer.options.groupId;
    }

    /**
     * Executes the task
     */
    exec() {
        return this._executor();
    }

    /**
     * Executes a catcher
     *
     * @param {Error} e an error that caused a catch
     */
    catch(e) {
        return this._catcher(e);
    }
}

module.exports = Task;
