AMQP Based work dispatching
===================

**ikue** is a job dispatching library built on top of [RabbitMQ](http://rabbitmq.com) and [nodejs](https://nodejs.com).

**ikue** allows you to easily dispatch works to differents parts of your infrastructure. 

----------

Features
-------
 - Simple api to create and submit jobs
 - Retry failled job
 - Support two backoff strategy for failed job
   - *'fixed'*: will retry the job at a fixed interval
   - *''exponential*: will retry failed job doubling the wait time after each failure.
 - Pluggable **logger**: By default ikue log to *stdout* but you can easily plug your own logger.
 - Selective job reception: If you don't want to receive jobs with a specific name in your *workQueue*, you can selectively specify a whitelist.

Requirements
-------

 - **RabbitMQ >= 3.5.0** (This is what we use in production, it might work well on older version of RabbitMQ, so if you are able to run it on a previous version let me know).
 - **nodejs > 0.10.0** any node version greater than 0.10.0 should work without any problem.

Getting started
=======

Installation
-------

To install the latest release:

    $ npm install ikue

To install the Edge version of ikue from github

    $ npm install ecstasy2/ikue

Initialize ikue
-------------
initializing ikue consist of creating a *WorkQueueManager* and creating at least a *WorkQueue* from it.

    var WorkQueueMgr = require('ikue').WorkQueueMgr;
    
    var queueMgr = new WorkQueueMgr({
	  component: 'consumer', 
	  amqp: {
	    url: "amqp://guest:guest@localhost:5672/bench"
	  },
	  name: 'Benchmark'
	});
	
	// Create a workQueue, this is the object we will be 
	// actually using to send/receive jobs
	var workQueue = queueMgr.createQueue('Queue1');
	queueMgr.on('ready', function(){
		workQueue.triggers = ['jobName'];

		// Start the work queue, will emit 'ready' when it succeed.
	    workQueue.start();

	    workQueue.on('ready', function(){
			// The work queue is ready and we can start using it
		});
      
	});
	
	// Connect the queue manager, this will emit the 'ready' event when it can be used.
	queueMgr.connect();

  * **WorkQueueManager:** The *queueMgr* is responsible for handling the connection with the AMQP server and for coordinating the job dispatching mechanism.

	ikue was built to fit in a distributed environment; with eventually many different apps using *ikue* for job/event dispatching. You can run any number of workers/apps completely in isolation on the same AMQP virtual host.
	
    * **Options:**
      * *name*: This should be a unique name across all your projects. **Note that** project here doesn't necessarily mean a unit of deployment. So if you have an *"project"* that consist of a *producer* component and a *consumer* component, those two app should use **EXACTLY** the same name.

	  * *amqp*: This object will be passed as is to [node-amqp](https://github.com/postwait/node-amqp) when creating the connection to RabbitMQ.
		
		 The *url* is the connection string containing all the credential to access the RabbitMQ server.
		  
      * *component*: Use this field to compartmentalize different deployment unit (namely app) for the same project.

  * **WorkQueue:** To create a WorkQueue you just need to provide a *queueName*. A work queue is the actually the object you will be interacting with. Upon initialization you can configure a *workQueue* object to receive all jobs with specific names. This feature allows you to specify the list of *job name* it will be receiving.
Right now you need to explicitly set this list. One of the upcoming feature is the ability to receive all jobs by default.

Create and submit a Job
-------------


	// We create the job we want to send for processing.
	// We need to provide the job a unique *name* and a hash of parameters.
	var job = workQueue.createJob('say_hello', {name: "Diallo"})
	  .maxRetry(30)
      .backoff('fixed')
      .delay(20000);

	job.send();

* *maxRetry(count)*: The maximum number of time this job will be retried in case of failure.
* *backoff(strategy)*: Which backoff strategy to use for failed jobs.


Receive and handle a Job
-------

	    workQueue.on('ready', function(){
		  // The work queue is ready and we can start using it
		});
		
		// Will be called whenever there is a job named 'job_name' to be handled.
		// @param contains the hash object that were sent with the job
		// @param done is a callback function that should be called when the job complete.
		//    passing it an error as first parameter will mark this job as failed.  
		workQueue.on('job_name', function(params, done){
		  // 
		});