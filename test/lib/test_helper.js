var vasync = require('vasync');
var _ = require('lodash');

/**
  Utility function to wait for a specific job id to be received on all queues
*/
exports.waitForJobWithId = function(jobId, queues, callback){
  queues = !_.isArray(queues) ? [queues] : queues;

  if (!_.isArray(queues)) {
    throw new Error("invalid use of waitForJobWithId(), queues should be an array of WorkQueues or a WorkQueue object");
  }
  
  vasync.forEachParallel({
    'func': function waitForMessage(queue, done){
      var onEvent = function(work){
        var message = work.message;
        var headers = work.headers;

        if (headers.jobId == jobId) {

          queue.removeListener('new_work', onEvent);

          done(null, work);
        }
      };

      queue.on('new_work', onEvent)
    },
    'inputs': queues
  }, function (err, results) {
    callback(err, results.successes);
  });
}