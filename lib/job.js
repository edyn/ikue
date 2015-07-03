'use strict';

var EventEmitter = require('events').EventEmitter;
var _ = require('lodash');
var uuid = require('node-uuid');

exports = module.exports = Job;


/**
 * Default job priority map.
 */

var priorities = exports.priorities = {
    low: 10, normal: 7, medium: 5, high: 3, critical: 0
};


/**
 * Initialize a new `Job` with the given `type` and `data`.
 *
 * @param {String} type
 * @param {Object} data
 * @api public
 */

function Job(type, data) {
    this.type = type;
    this.data = data || {};
    this._max_attempts = 1;
    this._attempts = 0;
    this.id = uuid.v1();
    this._sent_at = 0;
    this.backoff('fixed');

    this.priority('normal');
    this.on("error", function(err){});// prevent uncaught exceptions on failed job errors
}

/**
 * Inherit from `EventEmitter.prototype`.
 */

Job.prototype.__proto__ = EventEmitter.prototype;

/**
    If this job was created from a work queue, push it to that work queue
*/
Job.prototype.send = function (done) {
    if (!this._workQueue) {
        throw new Error("Can't submit work to work queue, this job is not associated with a queue");
    };

    this.workQueue().pushJob(this, done);
};

/**
 * Associate the job to a work queue or return the current one.
 *
 * @param {WorkQueue} queue
 * @return {Job} for chaining
 * @api public
 */

Job.prototype.workQueue = function (queue) {
    if (0 == arguments.length) return this._workQueue;

    this._workQueue = queue;
    return this;
};

Job.prototype.toJSON = function () {
    return {
          id: this.id
        , type: this.type
        , data: this.data
        , priority: this._priority
        , created_at: this.created_at
        , updated_at: this.updated_at
        , failed_at: this.failed_at
        , delay: this._delay
        , attempts: {
            made: Number(this._attempts) || 0
          , remaining: this._attempts ? this._max_attempts - this._attempts : Number(this._max_attempts)||1
          , max: Number( this._max_attempts ) || 1
        }
    };
};

Job.prototype.delay = function (ms) {
    if (0 == arguments.length) return this._delay;
    this._delay = ms;
    return this;
};

Job.prototype.backoff = function (param) {
    if (0 == arguments.length) return this._backoff;
    this._backoff = param;
    return this;
};

Job.prototype._getBackoffImpl = function() {
    var supported_backoffs = {
        fixed: function(delay){
            return function( attempts ){
                return delay;
            };
        }
        ,exponential: function(delay){
            return function( attempts ) {
                console.log("attempts : "+attempts)
                return Math.round( delay * 0.5 * ( Math.pow(2, attempts) - 1) );
            };
        }
    };

    if( _.isPlainObject( this._backoff ) ) {
        return supported_backoffs[ this._backoff.type ]( this._backoff.delay || this._delay );
    } else {
        return supported_backoffs[ this._backoff ]( this._delay );
    }
};

/**
 * Set or get the priority `level`, which is one
 * of "low", "normal", "medium", and "high", or
 * a number in the range of -10..10.
 *
 * @param {String|Number} level
 * @return {Job|Number} for chaining
 * @api public
 */

Job.prototype.priority = function (level) {
    if (0 == arguments.length) return this._priority;
    this._priority = null == priorities[level]
        ? level
        : priorities[level];
    return this;
};

/**
 * Set number of attempts to `n`.
 *
 * @param {Number} n
 * @return {Job} for chaining
 * @api public
 */

Job.prototype.attempts = function (n) {
    if (0 == arguments.length) return this._attempts;

    this._attempts = n;
    return this;
};

Job.prototype.sentAt = function (n) {
    if (0 == arguments.length) return this._sent_at;

    this._sent_at = n;
    return this;
};

/**
 * Set max attempts to `n`.
 *
 * @param {Number} n
 * @return {Job} for chaining
 * @api public
 */

Job.prototype.maxRetry = function (n) {
    if (0 == arguments.length) return this._max_attempts;

    this._max_attempts = n;
    return this;
};