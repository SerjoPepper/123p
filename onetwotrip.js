#!/usr/bin/env node

var cp = require('child_process');
var config = require('config');
var _ = require('lodash');
var rclient = require('redis').createClient(config.redis);
if (config.redis.db) {
  rclient.select(config.redis.db);
}

var argv = require('minimist')(process.argv.slice(1));

var workers = [];
var action = argv.action || 'generator';

if (action === 'generator') {
  rclient.ltrim(config.prefix + 'errors', -1, 0);
  var parent = process.id + '_' + Date.now();
  _.times(config.workers).forEach(function spawn (i) {
    workers[i] = cp.spawn(process.execPath, ['./worker'], {count: argv.count || config.count, parent: parent});
    workers[i].on('exit', spawn.bind(null, i));
    workers[i].on('message', function (msg) {
      if (msg === 'done') {
        console.log('DONE');
        process.exit(0);
      }
    })
  });
}
else if (action === 'errors') {
  rclient.lrange(config.prefix + 'errors', 0, -1, function (res) {
    if (res.length) {
      res.forEach(err) {
        console.log('Error: ', err);
      };
    }
    process.exit(0);
  });
}

rclient.config('SET', 'notify-keyspace-events', 'Ex');