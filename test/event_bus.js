'use strict';

var chai = require('chai'); // assertion library
var expect = chai.expect;
var should = chai.should;
var sinon = require('sinon');
var sinonChai = require("sinon-chai");
chai.use(sinonChai);

var EventBus = require('../index').EventBus;

var EventEmitter = require('events').EventEmitter;

// This a default thest that we add here just in order to thes that
// "mocha" and "should" are installed correctly
describe('EventBus', function () {
  var bus;

  var event1Listener;
  var event2Listener;
  var event3Listener;

  var success1Async;
  var success2Async;
  var failNonAsync;
  var failAsync;

  beforeEach(function(){
    bus = new EventBus("NAME");

    event1Listener = sinon.stub();
    event2Listener = sinon.stub();
    event3Listener = sinon.stub();

    success1Async = sinon.spy(function(param, done){
      process.nextTick(function(){
        done(null);
      });
    });

    success2Async = sinon.spy(function(param, done){
      process.nextTick(function(){
        done(null);
      });
    });

    failNonAsync = sinon.spy(function(param){
      done(null);
    });

    failAsync = sinon.spy(function(param, done){
      process.nextTick(function(){
        done(new Error('Fail on purpose'));
      });
    });

  });

  it('should have a correct name', function () {
    expect(bus.name).to.eql("NAME");
  });
  
  it('should accept listenners', function () {
    bus.addListener('event1', event1Listener);

    bus.emit('event1', "hello");

    var count = EventEmitter.listenerCount(bus, 'event1');
    expect(count).to.eql(1);

    expect(event1Listener).to.have.been.calledOnce
  });

  it('should emit events to listening only when listenning', function () {
    bus.addListener('event1', event1Listener);
    bus.addListener('event2', event2Listener);
    bus.addListener('event3', event3Listener);

    bus.emit('event1', "hello");

    expect(event1Listener).to.have.been.calledOnce
    expect(event2Listener).to.not.have.been.called
  });

  it('should work triggering asynchronous listener', function (done) {
    bus.addListener('event1', success1Async);
    bus.addListener('event1', success2Async);

    bus.trigger('event1', "hello", function(err, res){
      expect(success1Async).to.have.been.calledOnce
      expect(success2Async).to.have.been.calledOnce

      expect(err).not.to.exist

      done();
    });
  });

  it('should fail when contains an non asynchronous listener', function (done) {
    bus.addListener('event1', success1Async);
    bus.addListener('event1', success2Async);
    bus.addListener('event1', failNonAsync);

    bus.trigger('event1', "hello", function(err, res){
      expect(success1Async).to.not.have.been.calledOnce
      expect(success2Async).to.not.have.been.calledOnce

      expect(err).to.exist

      done();
    });
  });

  it('should fail when one asynchronous listener fail', function (done) {
    bus.addListener('event1', success1Async);
    bus.addListener('event1', success2Async);
    bus.addListener('event1', failAsync);

    bus.trigger('event1', "hello", function(err, res){
      expect(success1Async).to.have.been.calledOnce
      expect(success2Async).to.have.been.calledOnce

      expect(err).to.exist

      done();
    });
  });

});