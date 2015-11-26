var config = JSON.parse(process.env.config);
var redis = require('redis');
var _ = require('lodash');
var EventEmitter = require('events');
var os = require('os');

var client = redis.createClient(config.redis);
var subscribeClient = redis.createClient(config.redis); // subscribing client
var processPrefix = process.env.parent + ':';

if (config.redis.db) {
  client.select(config.redis.db);
  subscribeClient.select(config.redis.db);
}

function logErr (err) {
  if (err) {
    console.error(err);
  }
}

// функция из задания, без изменений
function getMessage () {
  getMessage.cnt = getMessage.cnt || 0;
  return ++getMessage.cnt;
}

// функция из задания, без изменений
function eventHandler (msg, callback) {
  setTimeout(onComplete, Math.floor(Math.random() * config.listenerDelay));
  function onComplete () {
    var error = Math.random() > 0.85;
    callback(error && 'LISTENER_ERROR');
  }
}

function getKey (key) {
  return config.prefix  + processPrefix + key;
}

function Handler () {
  // идентификатор текущего обработчика
  this.id = os.hostname() + '_' + process.pid;
  this.role = 'listener'; // listener or generator
  this.events = new EventEmitter;
  this.events.setMaxListeners(0);
  this.listeners = [];
  this.listenerIndex = 0;
  // итератор отсылаемых сообщений данным воркером, не связан с getMessage()
  this.msgIndex = 0;
  // мы должны знать, какой генератор у нас текущий. Т.к. теоретически могут быть коллизии,
  // когда один генератор отвалится, а потом начнет слать сообщения
  this.currentGenerator = null;
}


_.extend(Handler.prototype, {

  init: function () {
    this.tryToBeAGenerator();

    // чекаем, что соединение в порядке, иначе никак :(
    setInterval(this.heartbeat.bind(this), 1e3);

    // сдох генератор
    this.events.on('generatorExpire', function () {
      this.currentGenerator = null;
      this.tryToBeAGenerator();
    }.bind(this));

    // сдох один из слушателей, больше не отправляем ему сообщения
    this.events.on('listenerExpire', function (listener) {
      _.remove(this.listeners, listener);
    }.bind(this));

    // пришел новый слушатель, обновим список с ними
    this.events.on('newListener', function () {
      if (this.role === 'generator') {
        this.updateListeners(logErr);
      }
    }.bind(this));

    // пришло сообщение от генератора
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

    // пришел ответ от слушателя
    this.events.on('response', function (meta, err, res) {
      if (meta.generator === this.id) {
        this.events.emit('result', meta, err, res);
      }
    }.bind(this));

    // подписываемся на протухание ключей (для heartbeat) и на сообщения от генератора/слушателя
    subscribeClient.subscribe('__keyevent@' + (config.redis.db || 0) + '__:expired', this.getChannelKey());

    subscribeClient.on('message', function (channel, message) {
      if (channel === this.getChannelKey()) {
        this.events.emit.apply(this.events, JSON.parse(message));
      } else {
        if (message.indexOf(getKey('listener')) === 0) {
          listener = message.replace(getKey('listener:'), '').split(':')[0];
          this.events.emit('listenerExpire', listener);
        } else if (message.indexOf(getKey('generator')) === 0) {
          this.events.emit('generatorExpire');
        }
      }
    }.bind(this));
  },

  // держим список обработчиков, чтобы
  updateListeners: function (callback) {
    client.keys(getKey('listener*'), function (err, keys) {
      if (err) {
        return callback(err);
      }
      this.listeners = [];
      if (keys) {
        keys.forEach(function (_key) {
          this.listeners.push(_key.replace(getKey('listener:'), ''));
        }.bind(this));
      }
      callback();
    }.bind(this));
  },

  // запускаем генератор
  runGenerator: function () {
    setTimeout(function () {
      if (config.messages > (getMessage.cnt || 0)) {
        this.emitMessage();
        this.runGenerator();
      } else {
        process.send('done');
      }
    }.bind(this), config.generateDelay);
  },

  heartbeat: function () {
    if (this.role === 'listener') {
      client.set(this.getListenerKey(), this.id, 'EX', 2);
    }
    else if (this.role === 'generator') {
      client.set(this.getGeneratorKey(), this.id, 'EX', 2)
    }
  },

  // воркер пытается стать генератором
  tryToBeAGenerator: function () {
    client.set(this.getGeneratorKey(), this.id, 'EX', 2, 'NX', function (err, res) {
      if (err) {
        return logErr(err);
      }
      if (res) {
        if (this.role != 'generator') {
          this.role = 'generator';
          this.currentGenerator = this.id;
          this.updateListeners(function (err) {
            if (err) {
              return logErr(err);
            }
            client.get(getKey('counter'), function (err, key) {
              if (err) {
                return logErr(err);
              }
              getMessage.cnt = Number(key);
              this.runGenerator();
            }.bind(this));
          }.bind(this));
        }
      } else {
        this.role = 'listener';
        client.set(this.getListenerKey(), this.id, 'EX', 2, function (err) {
          if (err) {
            return logErr(err);
          }
          this.emit('newListener');
        }.bind(this));
        client.get(this.getGeneratorKey(), function (err, res) {
          if (err) {
            return logErr(err);
          }
          this.currentGenerator = res;
        }.bind(this));
      }
    }.bind(this));
  },

  getChannelKey: function () {
    return getKey('channel');
  },

  getListenerKey: function () {
    return getKey('listener:' + this.id);
  },

  getGeneratorKey: function () {
    return getKey('generator');
  },

  emitMessage: function (callback) {
    var message = getMessage();
    this.sendMessage(message, function (err, meta, res) {
      var msg = 'Generator: ' + this.id + '. Message: ' + message + '. Listener: ' + meta.listener;
      if (err) {
        this.saveError(err);
        logErr('Error occured: ' + err + '. ' + msg);
      } else {
        console.log('Success. ' + msg);
      }
      client.incr(getKey('counter'), callback)
    }.bind(this));
  },

  saveError: function (err) {
    var error = {
      date: new Date(),
      err: err
    }
    client.lpush(config.prefix + 'errors', JSON.stringify(error), logErr);
  },

  sendMessage: function (message, callback) {
    // выбираем следующего получателя через round-robin
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

  // отправляем сообщение выбранному слушателю
  sendToListener: function (listener, message, callback) {
    var meta = {
      listener: listener,
      messageId: this.getMessageId(),
      generator: this.id
    };
    var removeListeners = function () {
      this.events.removeListener('result', onResult);
      this.events.removeListener('listenerExpire', onListenerExpire);
    }.bind(this);
    var onResult = function (_meta, err, res) {
      if (meta.messageId === _meta.messageId) {
        removeListeners();
        callback(err, meta, res);
      }
    };
    var onListenerExpire = function (droppedListener) {
      if (droppedListener === listener) {
        removeListeners();
        callback('NOT_REACHABLE', meta);
      }
    };
    this.emit('message', meta, message);
    this.events.on('result', onResult);
    this.events.on('listenerExpire', onListenerExpire);
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
    client.publish(this.getChannelKey(), JSON.stringify([].slice.call(arguments)));
  },

});


new Handler().init();