'use strict';

var amqp = require('amqp');
var vasync = require('vasync');
var util = require('util');
var merge = require('merge');
var assert = require('assert');
var EventEmitter = require('events').EventEmitter;
var _ = require('lodash');

var WorkQueue = require('./work_queue');

var AMQP_URL = process.env.AMQP_URL || "amqp://guest:guest@localhost:5672/test";

var logger;
var ConsoleLogger = require('./logger');

var defaultOptions = {
  logger: new ConsoleLogger("ikue"),
  name: 'default',
  component: 'default',
}


var WorkQueueMgr = function WorkQueueMgr(options){
  
  var self = this; 

  this.options = merge(defaultOptions, options || {});

  logger = this.options.logger;

  this.workerExchange = null;
  this.workQueue = null;
  this.connected = false;
  this.name = this.options.name;

  this.retryExchangeName =  this.name + '.retry';

  this.exchangeName = this.name + '.workers';
  this.routingKey = this.name + '.key';

  var amqpOpts = self.options.amqp || {};
  amqpOpts = merge({
    url: AMQP_URL,
    api_port: 15672,
    clientProperties: {
      applicationName: 'Worker Queue : ' + self.name
    }
  }, amqpOpts);

  this.options.amqpOpts = amqpOpts;

  //console.log(amqpOpts)

  this.connect = function(){

    logger.info("Creating the AMQP connection");

    self.connection = amqp.createConnection(amqpOpts);

    self.connection.on('error', function(err){
      logger.info(self.name + ' AMQP error event '+ err);
      logger.error(err.stack);
    })

    self.connection.on('connect', function(err){
      logger.info(self.name + ' AMQP connect event');

      self.connected = true;
    }.bind(self))

    self.connection.on('close', function(err){
      logger.info(self.name + ' AMQP close event');

      self.connected = false;
    });

    // Wait for connection to become established.
    self.connection.on('ready', self.onConnectionReady);
  }

  this.onConnectionReady = function(){
    logger.info("AMQP connection ready for queue :" + this.name);

    vasync.pipeline({
      'funcs': [
        _.bind(createWorkersExchange, self),
        _.bind(createRetryExchange, self)
      ]
    }, function(err, results){
      if (err) {
        logger.error('Failled to initialize the exchanges : ' + err);

        self.emit('error', err);

        return;
      }

      self.emit('ready');

    }.bind(this));
  }.bind(this);
}

function createRetryExchange(p, callback){

  var self = this;
  this.retryExchange = this.connection.exchange(this.retryExchangeName, {
    type: 'direct',
    durable: true,
    confirm: true
  }, _.bind(function (exchange) {
    
    logger.info("Retry Exchange (" + exchange.name + ") is ready with routing key : " + this.queueName);

    callback(null);

  }, this));
}

util.inherits(WorkQueueMgr, EventEmitter);

WorkQueueMgr.prototype.createQueue = function(queueName) {
  return new WorkQueue(queueName, this);
};

function createWorkersExchange(p, callback){
  var self = this;

  this.workerExchange = self.connection.exchange(this.exchangeName, {
    type: 'headers',
    durable: true,
    confirm: true,
    autoDelete: false
  }, _.bind(function (exchange) {
    if (!exchange) {
      callback(new Error('Unable to open Exchange : ' + this.exchangeName));

      return;
    }

    logger.info("Exchange (" + exchange.name + ") is ready");

    callback(null);

  }, this));
}


module.exports = WorkQueueMgr;
