

var logLevels = ['info', 'warn', 'error', 'info', 'trace', 'debug'];
  
logLevels.forEach(function(level){
  ConsoleLogger.prototype[level] = function(msg){
    if (!process.env.NO_DEBUG) return;

    console.log(new Date()+ ' # '+ level + " : "+msg);
  }
});

function ConsoleLogger(){
  
}

module.exports = ConsoleLogger