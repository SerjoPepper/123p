#!/usr/bin/env node

var cp = require('child_process');
var config = require('./config');
var _ = require('lodash');
var argv = require('minimist')(process.argv.slice(1));
_.extend(config, argv);
var workers = [];
var action = argv.action || 'generator';
var client = require('redis').createClient(config.redis);
if (config.redis.db) {
  client.select(config.redis.db);
}

if (action === 'generator') {
  client.ltrim(config.prefix + 'errors', -1, 0);
  var parent = process.pid + '_' + Date.now();
  _.times(config.workers).forEach(function spawn (i) {
    workers[i] = cp.fork('./worker', {
      env: {
        config: JSON.stringify(config),
        parent: parent
      }
    });
    console.log('spawn worker');
    workers[i].on('exit', spawn.bind(null, i));
    workers[i].on('error', function (err) {
      console.error(err);
    });
    workers[i].on('message', function (msg) {
      if (msg === 'done') {
        console.log('DONE');
        process.exit(0);
      }
    })
  });
}
else if (action === 'errors') {
  client.lrange(config.prefix + 'errors', 0, -1, function (err, res) {
    if (err) {
      console.error(err);
    }
    if (res && res.length) {
      res.forEach(function (err) {
        console.log('Error: ', err);
      });
    } else {
      console.log('No errors')
    }
    process.exit(0);
  });
}

// включаем нотификации
client.config('SET', 'notify-keyspace-events', 'Ex');