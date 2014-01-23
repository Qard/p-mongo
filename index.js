var MongoClient = require('mongodb').MongoClient
var Connection = require('mongodb').Connection
var Server = require('mongodb').Server
var Db = require('mongodb').Db
var Q = require('q')

var HOST = process.env.MONGO_NODE_DRIVER_HOST || 'localhost'
var PORT = process.env.MONGO_NODE_DRIVER_PORT || Connection.DEFAULT_PORT

var cache = {}

// 
// Wrap the 
// 
exports.connect = function (path) {
  var args = Array.prototype.slice.call(arguments)
  return cache[path] || (cache[path] = connect(args))
}


exports.db = function (dbName, server, port) {
  if (typeof server === 'string') {
    server = new Server(server || HOST, port || PORT, {})
  }
  if ( ! (server instanceof Server)) {
    throw new Error('db() must receieve a Server instance of a host string')
  }
  var key = dbName + ':' + server.host + ':' + server.port
  return cache[key] || (cache[key] = createDatabase(dbName, server))
}


exports.collection = function (db, name) {
  return wrapCollection(
    Q.fcall(function () {
      return db
    })
    .then(function (db) {
      return Q.ninvoke(db, 'collection', name)
    })
    .then(function (collection) {
      if (collection) return collection
      return Q.ninvoke(db, 'createCollection', name)
    })
  )
}

/****************************** HELPER METHODS ********************************/

function connect (args) {
  return wrapDatabase(Q.npost(MongoClient, 'connect', args))
}

function createDatabase (name, server) {
  var db = new Db(name, server)
  return wrapDatabase(Q.ninvoke(db, 'open'))
}

function wrapDatabase (dbPromise) {
  wrapPromiseWithMethods(dbPromise, [
    'createCollection',
    'renameCollection'
  ], wrapCollection)

  // Nothing in response to wrap
  wrapPromiseWithMethods(dbPromise, [
    'open',
    'close',
    'admin',
    'dropCollection',
    'collectionNames',
    'collections',
    'eval',
    'dereference',
    'logout',
    'authenticate',
    'addUser',
    'removeUser',
    'command',
    'lastError',
    'previousErrors',
    'resetErrorHistory',
    'createIndex',
    'ensureIndex',
    'cursorInfo',
    'dropIndex',
    'reIndex',
    'indexInformation',
    'dropDatabase',
    'stats'
  ])

  wrapPromiseWithMethods(dbPromise, [
    'collectionsInfo'
  ], wrapCursor)

  dbPromise.collection = function (collectionName) {
    return wrapCollection(dbPromise.then(function (db) {
      return exports.collection(db, collectionName)
    }))
  }

  return dbPromise
}

function wrapCollection (collectionPromise) {
  wrapPromiseWithMethods(collectionPromise, [
    'insert',
    'remove',
    'rename',
    'save',
    'update',
    'distinct',
    'count',
    'drop',
    'findAndModify',
    'findAndRemove',
    'findOne',
    'createIndex',
    'ensureIndex',
    'indexInformation',
    'dropIndex',
    'dropAllIndexes',
    'reIndex',
    'mapReduce',
    'group',
    'options',
    'isCapped',
    'indexExists',
    'geoNear',
    'geoHaystackSearch',
    'indexes',
    'aggregate',
    'stats'
  ])

  collectionPromise.find = function () {
    var args = Array.prototype.slice.call(arguments)
    return wrapCursor(collectionPromise.then(function (collection) {
      return collection.find.apply(collection, args)
    }))
  }

  return collectionPromise
}

// Wrap a promise expected to return a cursor
function wrapCursor (cursorPromise) {
  wrapPromiseWithMethods(cursorPromise, [
    'toArray',
    'count',
    'sort',
    'limit',
    'setReadPreference',
    'skip',
    'batchSize',
    'nextObject',
    'explain',
    'close',
  ])

  return cursorPromise
}

// Wrap a promise with a set of methods to invoke on the response
function wrapPromiseWithMethods (promise, methods, wrapper) {
  methods.forEach(function (method) {
    promise[method] = function () {
      var args = Array.prototype.slice.call(arguments)
      var p = promise.then(function (obj) {
        return Q.npost(obj, method, args)
      })
      return wrapper ? wrapper(p) : p
    }
  })
}
