'use strict';

var amqp = require('amqp');
var vasync = require('vasync');
var util = require('util');
var merge = require('merge');
var EventEmitter = require('events').EventEmitter;
var _ = require('lodash');
var uuid = require('node-uuid');

var Job = require('./job');

var AMQP_URL = process.env.AMQP_URL || "amqp://guest:guest@localhost:5672";

var logger;
var ConsoleLogger = require('./logger');

var AMQP_DEFAULT_DIRECT_EXCHANGE = 'amq.direct';

var defaultOptions = {
  logger: new ConsoleLogger(),
  name: 'default',
  component: 'default'
}


var WorkQueue = function WorkQueue(eventBus, options){
  
  var self = this; 

  this.options = merge(defaultOptions, options || {});

  logger = this.options.logger;

  this.workerExchange = null;
  this.workQueue = null;
  this.connected = false;
  this.eventBus = eventBus;
  this.name = this.options.name;

  this.exchangeName = this.name + '.workers';
  this.waitExchangeName = this.name + '.workers.wait.'+this.options.component;
  this.queueName = this.name + '.workers_queue.'+this.options.component;
  this.waitQueueName = this.name + '.workers_queue.wait.'+this.options.component;
  this.routingKey = this.name + '.key';
  this.failedQueueName = this.name + '.workers_queue.failed.'+this.options.component;

  self.start = function(){

    logger.info("Creating the AMQP connection");

    var amqpOpts = self.options.amqp || {};
    amqpOpts = merge({
      url: AMQP_URL,
      clientProperties: {
        applicationName: 'Worker Queue : ' + self.name
      }
    }, amqpOpts);

    console.log(amqpOpts)

    this.connection = amqp.createConnection(amqpOpts);

    // Wait for connection to become established.
    self.connection.on('ready', self.onConnectionReady);

    self.connection.on('error', function(err){
      logger.error(err.stack);
    })

    self.connection.on('connect', function(err){
      logger.info(self.name + ' AMQP connect event');
      this.connected = true;
    })

    self.connection.on('close', function(err){
      logger.info(self.name + ' AMQP close event');
      this.connected = false;
    })
  }


  self.onWorkQueueReady = function (q, callback) {
    // Catch all message pusblished in that queue
    q.bind(self.exchangeName, self.routingKey);

    // Bind to receive direct messages sent from the retry queue
    q.bind(AMQP_DEFAULT_DIRECT_EXCHANGE, self.queueName);
    
    // Bind to receive direct messages sent from the retry queue
    self.failedQueue.bind(AMQP_DEFAULT_DIRECT_EXCHANGE, self.failedQueueName);

    // Receive messages
    q.subscribe({ack: true, prefetchCount: 1}, function (message, headers, deliveryInfo, messageObject) {
      var workerId = headers.worker_id;

      // Print messages to logs
      logger.trace("On new work message : " + workerId, {
        message: message,
        headers: headers
      });

      if (!workerId) {
        console.log("Discarding message")
        self.discardInvalidWorkMsg(messageObject);

        messageObject.acknowledge(false);

        return;
      }

      self.emit('new_work', {
        message: message,
        headers: headers
      });

      self.eventBus.trigger(workerId, message, function(err){
        if (err) {

          var func = _.bind(retryOrDiscard, self);

          func(message, messageObject, deliveryInfo);

          return;
        };

        logger.trace("acknowledge message");

        messageObject.acknowledge(false);
      })
    });

    logger.info("Created and bound the worker queue '"+self.queueName+"'");

    callback(null);
  }

  self.onConnectionReady = function(){
    logger.info("AMQP connection ready for queue :" + self.name);

    vasync.pipeline({
      'funcs': [
        _.bind(function(arg, callback){
          this.directExchange = this.connection.exchange(AMQP_DEFAULT_DIRECT_EXCHANGE, {
            durable: true,
            confirm: true
          }, function(exchange){
            callback(null);
          });
        }, self),
        _.bind(createWorkersExchange, self),
        _.bind(createWaitExchange, self)
      ]
    }, function(err, results){
      if (err) {
        logger.error('Failled to initialize the exchanges : ' + err);
        return;
      }

      self.emit('ready');

    });
  }
}

util.inherits(WorkQueue, EventEmitter);

WorkQueue.prototype.createJob = function(type, data){
  var job = new Job(type, data);
  job.workQueue(this);

  return job;
}


WorkQueue.prototype.pushJob = function(job, done){
  var params = job.data || {};

  var delay = job.delay() || 100;

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
      made_attempts: job.attempts()
    }
  };


  if (!done) {
    // Make sure we have at least a noop set for the callback
    done = function(){};
  };

  if (this.connection.connected) {
    this.workerExchange.publish(this.routingKey, params, messageOptions, function(err){
      if (err) {
        done(new Error('Unable to schedule work job : '+job.worker_id));

        return;
      }

      done(null, messageOptions.messageId);
    });
  } else {
    done(new Error('The amqp client is not currently connected'));
  }
  
}

function retryOrDiscard(message, messageObject, deliveryInfo){
  var headers = messageObject.headers;

  var job = this.createJob(headers.worker_id, message);
  job.maxRetry(headers.max_attempts)
    .attempts(headers.made_attempts)
    .backoff(headers.backoff)
    .delay(headers.delay);

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

    //console.log(messageObject.messageId);
    //console.log(this.waitExchange);
    //console.log(messageObject);

    this.waitExchange.publish(this.routingKey, message, messageOptions, function(err){
      logger.info("Added to retry queue : "+ (!err))
    });

    messageObject.acknowledge(false);

    logger.info('# '+madeAttempts + ' attempt for job : ' + messageOptions.messageId);

  } else {
    logger.info('Tried too many time the job : DISCARDING IT to : '+this.failedQueueName);

    delete messageOptions.expiration;

    this.directExchange.publish(this.failedQueueName, message, messageOptions);

    messageObject.acknowledge(false);
  }
}

function createWorkersExchange(p, callback){
  var self = this;

  this.workerExchange = this.connection.exchange(this.exchangeName, {
    type: 'fanout',
    durable: true,
    confirm: true
  }, _.bind(function (exchange) {
    
    logger.info("Exchange (" + exchange.name + ") is ready");

    self.workQueue = this.connection.queue(self.queueName, {
      durable: true,
      autoDelete: false
    }, function(workQueue){
      self.failedQueue = this.connection.queue(self.failedQueueName , {
        durable: true,
        autoDelete: false
      }, function(failedQueue){
        //console.log(workQueue)
        self.onWorkQueueReady(workQueue, callback);
      });
    });

  }, this));
}

function createWaitExchange(p, callback){

  var self = this;
  this.waitExchange = this.connection.exchange(this.waitExchangeName, {
    type: 'direct',
    durable: true,
    confirm: true
  }, _.bind(function (exchange) {
    
    logger.info("Wait Exchange (" + exchange.name + ") is ready with routing key : " + this.queueName);

    this.waitQueue = this.connection.queue(this.waitQueueName, {
      durable: true,
      autoDelete: false,
      arguments: {
        "x-dead-letter-exchange": AMQP_DEFAULT_DIRECT_EXCHANGE,
        "x-dead-letter-routing-key": self.queueName,
      }
    }, function(waitQueue){
      logger.info("Binding wait queue with routing key : "+ self.routingKey);
      
      waitQueue.bind(exchange.name, self.routingKey);

      logger.info("Created and bound the wait queue : "+ waitQueue.name +" to the echange : "+exchange.name);

      callback(null);
    });

  }, this));
}


function discardInvalidWorkMsg(msg){
  logger.warn("WorkerQueue '" + this.name + "' have received an invalid work that doesn't have a workId " + JSON.stringify(msg));
}

if (process.env.PRODUCER) {
  var EventBus = require('./event_bus');
  var eventBus = new EventBus("Workers");

  var workQueue = new WorkQueue(eventBus, {component: 'producer'});

  console.log(workQueue.createJob);

  var job = workQueue.createJob('say_hello', {name: "Diallo"});

  var sendMessage = function(){
    job.maxRetry(30)
      .backoff('fixed')
      .delay(20000);

    workQueue.pushJob(job, function(err, messageId){
      logger.info("Done sending job : " + messageId);
    });

    setImmediate(function(){
      sendMessage();
    });
  }

  workQueue.on('ready', function(){
    sendMessage();
  });

  workQueue.start();
} else if(process.env.CONSUMER){
  var EventBus = require('./event_bus');
  var eventBus = new EventBus("Workers");

  var workQueue = new WorkQueue(eventBus, {component: 'consumer'});

  eventBus.on("say_hello", function(params, done){
    //console.log("Job Failled")
    done(new Error('intentional'));
    //done();
  });

  workQueue.start();
}


module.exports = WorkQueue;
