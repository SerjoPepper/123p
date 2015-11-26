module.exports = {

  redis: {
    host: '127.0.0.1',
    port: '6379',
    db: 2
  },

  workers: 10,
  messages: 10, // messages count

  generateDelay: 200,
  listenerDelay: 200,

  prefix: '123p:' // redis keys prefix

};