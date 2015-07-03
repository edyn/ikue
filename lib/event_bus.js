'use strict';

var util = require('util');
var vasync = require('vasync');
var EventEmitter = require('events').EventEmitter;

/**
  Trigger function to use in order to emit an event and wait for all listener
  to confirm that they processed it in the background
*/
var triggerFunc = function triggerFunc(event, params, done){
  var listeners = this.listeners(event);

  var funcs = [];

  var err = null;

  listeners.forEach(function(func){

    if (func.length < 2 && typeof(done) == 'function') {
      err = new Error('Illegal use of the event_bus, this event bus should async listener. So it should take 3 params');
      done(err);

      return;
    }

    // Wrap the function call to inject the parametter
    funcs.push(func);

  });

  if (!err) {
    vasync.pipeline({
      'funcs': funcs,
      'arg': params,
    }, function (err, results) {
      done(err);
    });
  }
}

var EventBus = function EventBus(name){
  this.name = name;

  this.trigger = triggerFunc;

  this.listenerCount = function(event){
    return EventEmitter.listenerCount(bus, event);
  }
}

util.inherits(EventBus, EventEmitter);

module.exports = EventBus;