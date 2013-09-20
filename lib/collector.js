// redis-collector.js
// redis collector
// allows for job output to be put together into other jobs

var redis = require('redis');
var async = require('async');
var _ = require('underscore');
var redisClient = null;
var flushTimeout = null;

var _defaults = {
  collectorList: 'forerunner_collector_list',
  count: 100,
  timeout: 60,
  outputKey: 'input'
};

function constructor(inputJob, outputJob, opts) {
  if (!opts) {
    opts = {};
  }
  opts = _.defaults(opts, _defaults);
  this.opts = opts;
  this.inputJob = inputJob;
  this.outputJob = outputJob;
  this.outputKey = opts.outputKey;

  // milliseconds to seconds
  this.opts.timeout = this.opts.timeout * 1000;

  // start the client
  redisClient = redis.createClient(opts.port, opts.host, opts.redis);
  this.multifn = generateMultiPop(this.opts.count);
  return this;
}
module.exports = constructor;

constructor.prototype.postHooks = function() {
  var self = this;
  var postHooksOut = {};

  flushTimeout = setTimeout(function() {
    console.log('timeout');
    self.flushCollection();
  }, this.opts.timeout);

  postHooksOut[this.inputJob] = function(jobId, jobResult) {
    var jsonToStore = {
      id: jobId,
      results: jobResult
    };
    redisClient.rpush(self.opts.collectorList, JSON.stringify(jsonToStore), function(err, count) {
      if (err) {
        console.error('Failed to push job onto collector stack. Job id: ' + jobId);
        console.error(JSON.stringify(err, null, 2));
      } else {
        clearTimeout(flushTimeout);
        if (count >= self.opts.count) {
          console.log('limit reached');
          self.flushCollection();
        }
        // restart the timeout counter
        flushTimeout = setTimeout(function() {
          console.log('timeout');
          self.flushCollection();
        }, self.opts.timeout);
      }
    });
  };
  return postHooksOut;
};


constructor.prototype.flushCollection = function() {
  console.log('flushing queue');
  var self = this;
  this.multifn.exec(function(err, jobData) {
    if (jobData.length > 0) {
      var parsedArray = [];
      _.each(jobData, function(rawData) {
        try {
          parsedArray.push(JSON.parse(rawData));
        } catch (err) {
          console.error('Failed to parse stored job data in collector');
          console.error(err);
          return;
        }
      });

      var newJobData = _.pluck(parsedArray, 'results');

      // the forerunner object is set by forerunner itself
      var input = {};
      input[self.outputKey] = newJobData;

      console.log('assining new job');
      self.forerunner.assignJob(self.outputJob, input);
    }
  });
};

// util function
function generateMultiPop(count, key) {
  var multiArray = [];
  for (var i = 0; i < count; i++) {
    multiArray.push(['lpop', key]);
  }
  return client.multi(multiArray);
}
