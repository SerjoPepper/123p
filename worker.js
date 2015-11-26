var config = require('config');
var redis = require('redis');
var _ = require('lodash');
var EventEmitter = require('events');
var os = require('os');

var client = redis.createClient(config.redis);
var subscribeClient = redis.createClient(config.redis); // subscribing client
var processPrefix = process.env.parent;
var messagesCount = Number(process.env.count);

if (config.redis.db) {
  client.select(config.redis.db);
  subscribeClient.select(config.redis.db);
}

function getMessage () {
  getMessage.cnt = getMessage.cnt || 0;
  return getMessage.cnt++;
}

function eventHandler (msg, callback) {
  setTimeout(onComplete, Math.floor(Math.random() * config.handlerTimeout));
  function onComplete () {
    var error = Math.random() > 0.85;
    callback(error && 'LISTENER_ERROR');
  }
}

function getKey (key) {
  return config.prefix + key;
}

function Handler () {
  this.id = os.hostname() + '_' + process.id;
  this.role = 'listener'; // listener or generator
  this.events = new EventEmitter;
  this.listeners = [];
  this.listenerIndex = 0;
  this.msgIndex = 0;

  this.currentGenerator = null;

  this.tryToBeAGenerator();

  setInterval(this.heartbit.bind(this), 1e3);

  this.events.on('generatorExpire', function () {
    this.currentGenerator = null;
    this.tryToBeAGenerator();
  }.bind(this));

  this.events.on('listenerExpire', function (listener) {
    _.remove(this.listeners, listener);
  }.bind(this));

  this.events.on('newListener', function () {
    this.updateListeners();
  }.bind(this));

  this.events.on('message', function (meta, message) {
    if (meta.listener === this.id) {
      if (meta.generator === this.currentGenerator) {
        eventHandler(message, function (err, res) {
          this.emit('response', meta, err, res);
        }.bind(this));
      } else {
        this.emit('response', meta, 'BAD_GENERATOR');
      }
    }
  }.bind(this));

  this.events.on('response', function (meta, err, res) {
    if (meta.generator === this.id) {
      this.events.emit('result', meta, err, res);
    }
  }.bind(this));

  subscribeClient.subscribe('__keyevent@' + (config.redis.db || 0) + '__:expired', this.getChannelKey);

  subscribeClient.on('message', function (channel, message) {
    if (channel === this.getChannelKey()) {
      this.events.emit.apply(this.events, JSON.parse(message));
    } else {
      if (message.indexOf(getKey(processPrefix + ':listener')) === 0) {
        listener = message.replace(getKey(processPrefix + ':listener:'), '').split(':')[0];
        this.events.emit('listenerExpire', listener);
      } else if (message.indexOf(getKey(processPrefix + ':generator')) === 0) {
        this.events.emit('generatorExpire');
      }
    }
  }.bind(this));

}


_.extend(Handler.prototype, {

  updateListeners: function () {
    client.keys(getKey(processPrefix + ':listener:*'), function (keys), {
      this.listeners = [];
      keys.forEach(function (_key) {
        this.listeners.push(_key.replace(getKey(processPrefix + ':listener:')));
      }.bind(this));
    }.bind(this));
  },

  startGenerator: function () {
    if (this.role === 'generator') {
      this.generatorInterval = setInterval(this.emitMessage.bind(this), config.generateTimeout);
    }
  },

  heartbit: function () {
    if (this.role === 'listener') {
      client.setex(this.getListenerKey(), this.id, 2);
    }
    else if (this.role === 'generator') {
      client.setex(this.getGeneratorKey(), this.id, 2)
    }
  },

  tryToBeAGenerator: function () {
    client.set(this.getGeneratorKey(), this.id, 2, undefined, true, function (err, res) {
      if (err) {
        console.error(err);
        return process.exit(1);
      }
      if (res) {
        if (this.role != 'generator') {
          this.role = 'generator';
          this.currentGenerator = this.id;
          this.updateListeners(function (err) {
            if (err) {
              console.error(err);
              return;
            }
            this.get(getKey(processPrefix + ':counter'), function (key) {
              getMessage.cnt = Number(key);
              this.startGenerator();
            }.bind(this));
          }.bind(this));
        }
      } else {
        this.role = 'listener';
        client.get(getGeneratorKey(), function (err, res) {
          this.currentGenerator = this.id;
        }.bind(this));
      }
    }.bind(this));
  },

  getChannelKey: function () {
    return getKey(processPrefix + ':channel');
  },

  getListenerKey: function () {
    return getKey(processPrefix + ':listener:' + this.id);
  },

  getGeneratorKey: function () {
    return getKey(processPrefix + ':generator');
  },

  emitMessage: function (callback) {
    var message = getMessage();
    this.sendMessage(message, function (err, meta, res) {
      var message = 'Generator: ' + this.id + '. Message: ' + message + '. Listener: ' + meta.listener;
      if (err) {
        this.saveError(err);
        console.error('Error occured: ' + err + '. ' + message);
      } else {
        console.log('Success. ' + message);
      }
      this.incr(getKey(processPrefix + ':counter'), callback)
    }.bind(this));
  },

  saveError: function (err) {
    var error = {
      date: new Date(),
      err: err
    }
    client.lpush(getKey('errors'), error);
  },

  sendMessage: function (message, callback) {
    var listener = this.getNextListener();
    if (!listener) {
      setTimeout(function () {
        this.sendMessage(message, callback);
      }.bind(this), 50);
    }
    else {
      this.sendToListener(listener, message, function (err, res) {
        // listener became unreachable before we had sent message
        if (err === 'NOT_REACHABLE') {
          this.sendMessage(message, callback)
        } else if (err === 'BAD_GENERATOR') {
          process.exit(1);
        } else {
          callback(err, res);
        }
      }.bind(this));
    }
  },

  sendToListener: function (listener, message, callback) {
    var meta = {
      listner: listner,
      messageId: this.getMessageId(),
      generator: this.id
    };
    var removeListeners = function () {
      this.events.removeEventListener('result', onResult);
      this.events.removeEventListener('listenerExpire', onDropListener);
    };
    var onResult = function (_meta, err, res) {
      if (meta.messageId === _meta.messageId) {
        removeListeners();
        callback(err, meta, res);
      }
    };
    var onDropListener = function (droppedListener) {
      if (droppedListener === listener) {
        removeListeners();
        callback('NOT_REACHABLE', meta);
      }
    };
    this.emit('message', meta, message);
    this.events.on('result', onResult);
    this.events.on('listenerExpire', onDropHandler);
  },

  getNextListener: function () {
    if (this.listenerIndex >= this.listeners.length) {
      this.listenerIndex = 0;
    }
    return this.listeners[this.listenerIndex++];
  },

  getMessageId: function () {
    return this.id + '_' + Date.now() + '_' + (++this.msgIndex);
  },

  emit: function () {
    var args = JSON.stringify(arguments.slice(0));
    client.publish(this.getChannelKey(), args);
  },

});
