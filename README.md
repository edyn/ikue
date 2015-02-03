AMQP Based work dispatching
===================

ikue allows you to easily dispatch works to differents parts of your infrastructure. 

----------


Create a job
-------------

    var EventBus = require('./event_bus');
	var eventBus = new EventBus("Producer");
	
	var workQueue = new WorkQueue(eventBus, {component: 'producer'});
	
	var job = workQueue.createJob('say_hello', {name: "Diallo"})
	  .maxRetry(30)
      .backoff('fixed')
      .delay(20000);

	job.send();


Execute a job
-------

    var EventBus = require('./event_bus');
	var eventBus = new EventBus("Workers");
	
	var workQueue = new WorkQueue(eventBus, {component: 'consumer'});
	
	eventBus.on("say_hello", function(params, done){
	    // Do what you have to do, like saying hello
		console.log('Hello ' + params.name);

		// You can pass an error here, if the job fail,
		// it will be retried after the backoff wait pass
	    done();
	  });
