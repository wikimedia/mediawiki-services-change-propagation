'use strict';


const EventEmitter = require('events');
const P = require('bluebird');


/** @const {number} the dafault queue size */
const DEFAULT_SIZE = 100;


/**
 * A class representing the task queue used in processing events.
 */
class TaskQueue extends EventEmitter {

    /**
     * Creates a new task queue instance
     *
     * @param {Object} [options={}]
     * @param {number} [options.size=DEFAULT_SIZE] the maximum number of tasks in the queue
     *
     * @constructor
     */
    constructor(options) {
        super();
        options = options || {};
        this._max_size = options.size || DEFAULT_SIZE;
        this._queue = [];
        this._waiting = [];
        this._consumers = [];
        this.on('task_completed', this._deferred.bind(this));
    }

    /**
     * Adds a consumer to the queue's list of consumers, which are used to pause
     * and resume them
     *
     * @param {HighLevelConsumer} consumer
     * @returns true if the consumer has been added, false if it has already been
     *               present in the list of consumers
     */
    addConsumer(consumer) {
        if (this._consumers.some((item) => item === consumer)) {
            return false;
        }
        this._consumers.push(consumer);
        return true;
    }

    /**
     * Enqueues a task on the queue. If the queue is full, the task will be placed on a waiting
     * list until enough tasks have been dequeued.
     *
     * @param {Object} task the task to enqueue
     * @param {number} task.id the message ID of the event
     * @param {HighLevelConsumer} task.consumer the Kafka consumer object used for offset commit
     * @param {Function} task.exec the promise to execute
     * @param {Function} task.catch the promise to execute in case the task's execution fails
     * @returns {Boolean} true if the task has been enqueued, false if it has been put on the
     *                    waiting list
     */
    enqueue(task) {
        if (this._queue.length >= this._max_size) {
            // too many tasks running already, defer this one
            this._defer(task);
            return false;
        }
        // ok, the execution can start right away
        this._start(task);
        return true;
    }

    _start(task) {
        this._queue.push(task);
        task._p = P.try(() => {
            return task.exec().finally(() => {
                this._queue = this._queue.filter((item) => item !== task);
                this.emit('task_completed');
            }).catch(task.catch);
        }).then(task.consumer.commitAsync.bind(task.consumer));
    }

    _defer(task) {
        // put the task in the waiting queue and pause the consumers
        this._waiting.push(task);
        this._consumers.forEach((consumer) => consumer.pause());
    }

    _deferred() {
        // enqueue any outstanding tasks
        while (this._queue.length < this._max_size && this._waiting.length > 0) {
            this._start(this._waiting.shift());
        }
        // resume the consumers if the queue has empty slots
        if (this._queue.length < this._max_size) {
            this._consumers.filter((cons) => cons.paused).forEach((cons) => cons.resume());
        }
    }

}


module.exports = TaskQueue;
