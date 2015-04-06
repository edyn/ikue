'use strict';

var vasync = require('vasync');
var util = require('util');
var merge = require('merge');
var mod_url = require('url');
var _ = require('lodash');

var rest = require('restler');

var logger;
var ConsoleLogger = require('../../lib/logger');

function throwIfErr(err){
  if(err) throw err;
}
/**
  A utility to convert rest 'complete' callbacks to regular nodejs'
*/
function wrapOnComplete(done, statusCodes){
  var positiveStatusCode = [200, 201, 202, 203, 204];

  if (!_.isUndefined(statusCodes)) {
    statusCodes = [_.parseInt(statusCodes)];
  }

  if (_.isArray(statusCodes)) {
    positiveStatusCode = _.union(positiveStatusCode, statusCodes);
  }

  var bindedCallback = function bindedCallback(result, response){
    if (result instanceof Error) {
      done(result, response);
      return;
    }

    if (!_.contains(positiveStatusCode, response.statusCode)) {
      done(new Error('Invalid status code : '+response.statusCode), response);
      
      return;
    }

    done(null, response);
  }

  return bindedCallback;
}

function RabbitManager(options){
  var defaultOpts = {
    logger: new ConsoleLogger(),
  };

  this.options = _.defaults(options, defaultOpts);

  this.baseUrlObj = mod_url.parse(this.options.url);

  logger = this.options.logger;
}

RabbitManager.prototype.makeUrl = function(pathname){
  var urlOpts = _.clone(this.baseUrlObj);
  urlOpts.pathname += pathname;

  var url = mod_url.format(urlOpts);

  return url;
}

RabbitManager.prototype.removeVhost = function(vhost, done){
  var url = this.makeUrl('/vhosts/' + vhost);
  var callback = wrapOnComplete(done, 404);
  rest.del(url, this.options.http).on('complete', callback);
}

RabbitManager.prototype.createVhost = function(vhost, done){
  var self = this;

  var url = self.makeUrl('/vhosts/' + vhost);
  var permUrl = self.makeUrl('/permissions/' + vhost + '/'+self.options.http.username);
  var permissions = {"configure":".*","write":".*","read":".*"};

  var newHost;

  vasync.waterfall([
    function(callback){
      var onRestOperationComplete = function onRestOperationComplete(err, response){
        newHost = new Vhost(url, {name: vhost});
        newHost.httpOptions = self.options.http;

        newHost.rabbitManager = self;

        callback(err, newHost);
      }

      rest.putJson(url, {}, self.options.http)
        .on('complete', wrapOnComplete(onRestOperationComplete));
    },

    function(vhost, callback){
      rest.putJson(permUrl, permissions, self.options.http)
        .on('complete', wrapOnComplete(callback));
    }
  ], function(err, results){
    done(err, newHost);
  });
}

function Vhost(url, options){
  this.baseUrl = url;
  this.options = options;
  
  this._name = options.name;
  this.name = _.bind(_.property('_name'), this, this);
}

Vhost.prototype.purgeQueue = function(queue, done){
  var url = this.rabbitManager.makeUrl('queues/'+ this.name() +'/'+ queue +'/contents');
  var callback = wrapOnComplete(done);
  rest.del(url, this.httpOptions).on('complete', callback);
}

Vhost.prototype.getQueue = function(queue, done){
  
}

Vhost.prototype.listQueues = function(done){
  var url = this.rabbitManager.makeUrl('queues/'+ this.name());
  var callback = wrapOnComplete(function(err, response, result){
    done(err, result, response);
  });

  rest.get(url, this.httpOptions).on('complete', callback);
}

Vhost.prototype.listQueueBindings = function(queue, done){
  
}

Vhost.prototype.createQueue = function(queue, options, done){
  
}

Vhost.prototype.getQueue = function(queue, done){
  
}

Vhost.prototype.getQueueContents = function(queue, options, done){
  if (_.isFunction(options)) {
    done = options;
    options = {};
  }

  var url = this.rabbitManager.makeUrl('queues/'+ this.name() + '/' + queue + '/get');
  var callback = wrapOnComplete(function(err, response){
    done(err, JSON.parse(response.rawEncoded));
  });

  var defaultOpts = {
    "count":50,
    "requeue":false,
    "encoding":"auto",
    "truncate":50000
  };

  console.log(url)
  var optionsParam = _.defaults(options, defaultOpts);

  rest.postJson(url, 
    optionsParam,
    this.httpOptions)
  .on('complete', callback);
}

Vhost.prototype.toString = function(){
  return JSON.stringify(this);
}

if (module.parent == null) {
  var amqpManager = new RabbitManager({
    url: 'http://localhost:15672/api/',
    http: {
      username: 'guest',
      password: 'guest',
    }
  });

  amqpManager.createVhost('test-vhost', function(err, vhost){
    throwIfErr(err);
    console.log("Created virtual host :" + vhost.name());

    var purgeCallback = wrapOnComplete(function(err, res){
      throwIfErr(err);
      console.log("Purged queue virtual host :" + 'test');
    });

    var listQueuesCallback = wrapOnComplete(function(err, res){
      throwIfErr(err);
      console.log("Listed queues in virtual host :" + 'test');
    });

    //vhost.purgeQueue('test', purgeCallback);
    //vhost.listQueues(listQueuesCallback);
  })
};
module.exports = RabbitManager;
