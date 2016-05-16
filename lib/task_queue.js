'use strict';


const EventEmitter = require('events');
const P = require('bluebird');
const KafkaFactory = require('./kafka_factory');


/** @const {number} the dafault queue size */
const DEFAULT_SIZE = 100;
const COMMIT_TIMEOUT = 100;


/**
 * A class representing the task queue used in processing events.
 */
class TaskQueue extends EventEmitter {

    /**
     * Creates a new task queue instance
     *
     * @param {Object} [options={}]
     * @param {number} [options.size=DEFAULT_SIZE] the maximum number of tasks in the queue
     * @param {number} [options.commitTimeout=COMMIT_TIMEOUT] the maximum delay to commit the offset
     * @constructor
     */
    constructor(options) {
        super();
        options = options || {};
        this._max_size = options.size || DEFAULT_SIZE;
        this._commit_timeout = options.commitTimeout || COMMIT_TIMEOUT;
        this._queue = [];
        this._waiting = [];
        this._consumers = [];
        this._commitScheduled = false;
        this._pendingCommits = new Map();

        this.on('task_completed', this._deferred.bind(this));
        this.on('task_completed', this._commitOffsets.bind(this));
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
     * @param {Task} task the task to enqueue
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

    /**
     * Finds the lowest offset that is safe to commit after some task finished.
     *
     * @param {Task} task just finished task
     * @private
     */
    _updatePendingCommits(task) {
        const current = this._pendingCommits.get(task.consumer)
            && this._pendingCommits.get(task.consumer)[task.topic]
            && this._pendingCommits.get(task.consumer)[task.topic][task.partition];
        const pendingPartitionTasks = this._queue
        .filter(task.isTaskForSamePartition.bind(task))
        .map((item) => item.offset);
        let newOffset = Math.min.apply(null, pendingPartitionTasks);
        if (newOffset !== task.offset) {
            // In case we have unfinished tasks with offsets lower then
            // the current finished task offset, decrement it since this
            // means the task with newOffset is still panding, but the one with
            // a lower offset is already finished.
            newOffset--;
        }
        if (current && current >= newOffset) {
            // A higher offset is already scheduled for commit
            return;
        }

        if (!this._pendingCommits.has(task.consumer)) {
            this._pendingCommits.set(task.consumer, {});
        }
        this._pendingCommits.get(task.consumer)[task.topic] =
            this._pendingCommits.get(task.consumer)[task.topic] || {};
        this._pendingCommits.get(task.consumer)[task.topic][task.partition] = newOffset;
    }

    /**
     * Begins the task execution
     *
     * @param {Task} task
     * @private
     */
    _start(task) {
        this._queue.push(task);
        task._p = P.try(() => {
            return task.exec().finally(() => {
                this._updatePendingCommits(task);
                this._queue = this._queue.filter((item) => item !== task);
                this.emit('task_completed');
            }).catch(task.catch.bind(task));
        });
    }

    /**
     * Defers the task execution, puts it into the waiting queue
     *
     * @param {Task} task
     * @private
     */
    _defer(task) {
        // put the task in the waiting queue and pause the consumers
        this._waiting.push(task);
        this._consumers.forEach((consumer) => consumer.pause());
    }

    /**
     * Begins execution of the deferred tasks
     *
     * @private
     */
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

    /**
     * Schedules a commit for pending commit requests
     *
     * @private
     */
    _commitOffsets() {
        if (this._commitScheduled) {
            return;
        }

        setTimeout(() => {
            for (let entry of this._pendingCommits.entries()) {
                const consumer = entry[0];
                const topicsOffsets = entry[1];
                Object.keys(topicsOffsets).forEach((topic) => {
                    KafkaFactory.commit(consumer, topic, topicsOffsets[topic]);
                });
            }
            this._pendingCommits = new Map();
            this._commitScheduled = false;
        }, this._commit_timeout);
        this._commitScheduled = true;
    }
}


module.exports = TaskQueue;
