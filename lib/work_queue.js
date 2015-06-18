'use strict';

var amqp = require('amqp');
var vasync = require('vasync');
var util = require('util');
var merge = require('merge');
var EventEmitter = require('events').EventEmitter;
var _ = require('lodash');
var uuid = require('node-uuid');
var assert = require('assert');
var rest = require('restler');
var mod_url = require('url');

var Job = require('./job');
var EventBus = require('./event_bus');

var AMQP_URL = process.env.AMQP_URL || "amqp://guest:guest@localhost:5672";

var logger;
var ConsoleLogger = require('./logger');

var AMQP_DEFAULT_DIRECT_EXCHANGE = 'amq.direct';


var WorkQueue = function WorkQueue(queueName, queueManager){
  
  var self = this; 

  this.workQueue = null;
  this.queueManager = queueManager;
  this.name = queueName;

  this.eventBus = new EventBus(queueName);

  // IF the triggers list is empty all works will be delivered
  this.triggers = [];

  logger = queueManager.options.logger;

  var queuePrefix = queueManager.exchangeName + '.' + queueManager.options.component + "_" + this.name;

  this.queueName = queuePrefix + '.pending';
  this.retryQueueName = queuePrefix + '.waiting';
  this.routingKey = this.name + '.key';
  this.failedQueueName = queuePrefix + '.failed';

  this.workQueue = null;
  this.retryQueue = null;
  this.failedQueue = null;
  this.retryExchange = null;

  self.start = function(){

    logger.info("Creating the work queue for worker : " + queuePrefix);
    
    _.bind(createQueues, self)(function(err, results){
      if (err) {
        self.emit('error');

        return;
      }

      self.emit('ready');

    });
  }


  this.queueManager.on('ready', function(){
    this.connection = queueManager.connection;
    this.workerExchange = queueManager.workerExchange;
    this.retryExchange = queueManager.retryExchange;
  }.bind(this));


  this.stop = function() {
    this.workQueue.unsubscribe(this._consumerTag);
    this.workQueue = null;
    this.workerExchange = null;
    this.retryExchange = null;
    this.connection = null;
  };

} 

util.inherits(WorkQueue, EventEmitter);

function listQueueBindings(arg, callback){
  var urlOpts = mod_url.parse(this.queueManager.options.amqpOpts.url);

  var amqpHttpPort = this.queueManager.options.amqpOpts.api_port;

  var vhost = urlOpts.pathname;
  urlOpts.protocol = 'http:';

  urlOpts.path = '/';
  urlOpts.port = amqpHttpPort;
  urlOpts.host = null;
  urlOpts.pathname = '/api/queues' + vhost + '/' + this.queueName + '/bindings';

  var url = mod_url.format(urlOpts);

  rest.get(url, this.httpOptions)
    .on('complete', function(result, response){
      if (result instanceof Error) {
        callback(result);
      } else {
        var bindings = JSON.parse(response.rawEncoded);

        var currentTriggers = [];

        _.each(bindings, function(binding){
          if (binding.source === this.queueManager.exchangeName && binding.arguments.worker_id) {
            currentTriggers.push(binding.arguments.worker_id);
          }
        }.bind(this));

        callback(null, currentTriggers);
      }

    }.bind(this));
}

function bindWorkerQueues(existingBindings, callback) {
  var self = this;

  var diffTriggers = _.difference(existingBindings, this.triggers);
  // remove all binding that are not relevant anymore
  _.each(diffTriggers, function(trigger){
    this.workQueue.unbind_headers(this.queueManager.exchangeName, {
      worker_id: trigger,
      'x-match': 'any',
    });
  }.bind(this));

  if (this.triggers.length == 0) {

    assert.ok(false, "Catch all trigger is not implemented yet");

    return;
  }

  // Catch all message pusblished in that queue with any of the job triggers
  _.each(this.triggers, function(trigger){
    this.workQueue.bind_headers(this.queueManager.exchangeName, {
      worker_id: trigger,
      'x-match': 'any',
    });
  }.bind(this));

  callback(null);

  // Bind to receive direct messages sent from the retry queue
  this.workQueue.bind(AMQP_DEFAULT_DIRECT_EXCHANGE, this.queueName);
  
  // Bind to receive direct messages sent to the failed queue
  this.failedQueue.bind(AMQP_DEFAULT_DIRECT_EXCHANGE, this.failedQueueName);

  // Bind retry queue exchange
  this.retryQueue.bind(this.queueManager.retryExchangeName, this.retryQueueName);

  // Receive messages
  this.workQueue.subscribe({ack: true, prefetchCount: 100}, function (message, headers, deliveryInfo, messageObject) {
    var workerId = headers.worker_id;

    // Print messages to logs
    logger.trace("On new work message : " + workerId, {
      message: message,
      headers: headers
    });

    if (!workerId) {
      console.log("Discarding message")
      this.discardInvalidWorkMsg(messageObject);

      messageObject.acknowledge(false);

      return;
    }

    this.emit('new_work', {
      message: message,
      headers: headers
    });

    // UGLY, need improvement

    message.ikue_headers = headers;

    this.eventBus.trigger(workerId, message, function(err, res){

      if (err) {

        logger.error("Job failed to run : " + err);
        var func = _.bind(retryOrDiscard, self);

        process.nextTick(function(){
          func(message, messageObject, deliveryInfo);
        });

        return;
      };


      logger.trace("acknowledge job : " + headers.jobId);

      messageObject.acknowledge(false);
    })
  }.bind(this))
  .addCallback(function(ok) { 
    this._consumerTag = ok.consumerTag; 
  }.bind(this));

  logger.info("Created and bound queues for '" + this.queueName + "'");
}

function createQueues(done) {
  vasync.waterfall([
    function func1(callback) {
      setImmediate(function () {
          callback(null, null);
      });
    },
    createWorkerQueues.bind(this),
    listQueueBindings.bind(this),
    bindWorkerQueues.bind(this),
  ], function(err, results){
    if (err) {
      logger.error('Failled to initialize the exchanges : ' + err);

      this.emit('error');

      return;
    }

    this.emit('ready');

  }.bind(this));
};

function createWorkerQueues(arg, callback){
  vasync.pipeline({
    funcs: [
      function(_, done){
        this.connection.exchange(AMQP_DEFAULT_DIRECT_EXCHANGE, {
            durable: true,
            confirm: true
          }, function (exchange) {

          if (!exchange) {
            done(new Error("Unable to open default direct exchange"));
            return;
          }

          this.directExchange = exchange;

          done(null, exchange);

        }.bind(this));
      }.bind(this),
      _.bind(createQueue, this, {
        name: this.queueName,
        property: 'workQueue'
      }),
      _.bind(createQueue, this, {
        name: this.retryQueueName,
        property: 'retryQueue',
        arguments: {
          "x-dead-letter-exchange": AMQP_DEFAULT_DIRECT_EXCHANGE,
          "x-dead-letter-routing-key": this.queueName,
        }
      }),
      _.bind(createQueue, this, {
        name: this.failedQueueName,
        property: 'failedQueue'
      }),
    ]
  }, function(err, results){
    if (err) {
      logger.error("Unable to createWorkerQueues : " + err);
    };

    callback(err, null);
  });
}

function createQueue(params, undefined, done){

  // Because we are using _.bind() to wrap this function there is actually 3 params
  // The second one is not used
  assert.ok(arguments.length === 3);

  var queueName = params.name;
  var assignToProp = params.property;

  assert.ok(assignToProp, 'A created queue should be assigned to a property');

  logger.info("Creating (" + queueName + ") ...");

  var timeoutReg = setTimeout(function(){
    done("Creating queue : " + queueName + " took too long");
  }, 1000);

  var args = params.arguments || {};

  this[assignToProp] = this.connection.queue(queueName , {
    durable: true,
    autoDelete: false,
    arguments: args
  }, function(q){
    clearTimeout(timeoutReg);

    if(q){
      done();

      return;
    }
  });
}

WorkQueue.prototype.createJob = function(type, data){
  var job = new Job(type, data);
  job.workQueue(this);

  return job;
}


WorkQueue.prototype.pushJob = function(job, done){
  var params = job.data || {};

  var delay = job.delay() || 100;

  if (job.sentAt() === 0) {
    job.sentAt(Math.round(new Date()));
  }

  var messageOptions = {
    contentType: 'application/json',
    messageId: uuid.v1(),
    headers: {
      jobId: job.id,
      worker_id: job.type,
      priority: job.priority(),
      backoff: job.backoff(),
      delay: delay,
      max_attempts: job.maxRetry(),
      made_attempts: job.attempts(),
      sent_at: job.sentAt(),
    }
  };


  if (!done) {
    // Make sure we have at least a noop set for the callback
    done = function(err){
      if (err) {
        throw err;
      }
    };
  };

  if (this.queueManager.connected === true) {
    try{
      this.workerExchange.publish(this.routingKey, params, messageOptions, function(err){
        if (err) {
          done(new Error('Unable to schedule work job : ' + job.worker_id));

          return;
        }

        done(null, messageOptions.messageId);
      });
    } catch(err){
      process.nextTick(function(){
        done(err);
      });
    }
  } else {
    done(new Error('The amqp client is not currently connected'));
  }
  
}

function retryOrDiscard(message, messageObject, deliveryInfo){
  var self = this;

  var headers = messageObject.headers;

  var job = this.createJob(headers.worker_id, message);
  job.maxRetry(headers.max_attempts)
    .attempts(headers.made_attempts)
    .backoff(headers.backoff)
    .delay(headers.delay)
    .sentAt(headers.sent_at);

  var madeAttempts = Number(headers.made_attempts) + 1;

  var delayFunc = job._getBackoffImpl();

  var expiration = delayFunc(madeAttempts);

  headers.made_attempts = madeAttempts;

  var messageOptions = {
    messageId: messageObject.messageId,
    appId: messageObject.appId || this.name + 'App',
    contentType: messageObject.contentType,
    headers: headers,
    expiration: expiration + ''
  };

  if (madeAttempts <= headers.max_attempts) {

    if (!messageOptions.messageId) {
      logger.warn("A work without a messageId found in the work queue, forcing one");
      messageOptions.messageId = uuid.v1();
    };

    this.retryExchange.publish(this.retryQueueName, message, messageOptions, function(err){
      logger.info("Added to retry queue ("+self.retryQueueName+"): "+ (!err))

      messageObject.acknowledge(false);

      logger.info('# '+madeAttempts + ' attempt for job : ' + messageOptions.messageId);
    });

  } else {
    logger.info('Tried the job too many times : DISCARDING IT to : '+this.failedQueueName);

    delete messageOptions.expiration;

    this.directExchange.publish(this.failedQueueName, message, messageOptions);

    messageObject.acknowledge(false);
  }
}


function discardInvalidWorkMsg(msg){
  logger.warn("WorkerQueue '" + this.name + "' have received an invalid job that doesn't have a workId " + JSON.stringify(msg));
}


module.exports = WorkQueue;
