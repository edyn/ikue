'use strict';

var util = require('util');
var vasync = require('vasync');
var EventEmitter = require('events').EventEmitter;

/**
  Work dispatcher accept a eventbus it will listen to in order
  to convert them to specific work job to be added to the workQueue
*/
var WorkDispatcher = function(eventbus, workQueue, mapping){
  this.eventbus = eventbus;
  this.workQueue = workQueue;

  var events = mapping.events;

  Object.keys(events).forEach(function(key){
    var work = events[key];

    eventbus.on(key, function(params, done){

      // When we get that event, push a new work in the workQueue
      // using the done callback to make sure it has been pushed
      workQueue.pushJob({
        worker_id: work.name,
        params: params,
        priority: work.priority
      }, done);
    });
  });
}

util.inherits(EventBus, EventEmitter);

module.exports = WorkDispatcher;