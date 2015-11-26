module.exports = {

  redis: {
    host: '127.0.0.1',
    port: '6379',
    db: 2
  },

  workers: 10,
  count: 500, // messages count

  generateTimeout: 500,
  listenerTimeout: 500,

  prefix: '123p:' // redis keys prefix

};