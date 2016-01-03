/*!
 *      Copyright (C) 2015 SequoiaDB Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

'use strict';

// native modules
var EventEmitter = require('events');
var util = require('util');

// third modules
var Long = require('long');
var debug = require('debug')('sequoiadb:client');
var Code = require('bson').BSONPure.Code;

// file modules
var common = require('./common');
var helper = require('./helper');
var constants = require('./const');
var Message = require('./message');
var Pool = require('./pool');
var Domain = require('./domain');
var SDBError = require('./error').SDBError;
var errors = require('./error').errors;
var CollectionSpace = require('./collection_space');
var Cursor = require('./cursor');
var ReplicaGroup = require('./replica_group');
var getNextRequstID = require('./requestid');

var callbackWrap = function (resolve, reject) {
  return function (err) {
    if (err) {
      return reject(err);
    }
    resolve();
  };
};

var Client = function (port, host, opts) {
  EventEmitter.call(this);
  debug('create client for %s:%s, with %j', host, port, opts);
  opts || (opts = {});
  opts.port = port;
  opts.host = host;
  this.pool = new Pool(opts);
  Object.defineProperty(this, 'isBigEndian', {
    get: function () {
      return this.pool.isBigEndian;
    },
    enumerable: false
  });
};
util.inherits(Client, EventEmitter);

Client.prototype._send = function (buff, message, state, callback) {
  debug('send message is %j', message);
  debug('buff is %j', buff);
  var conn = this.pool.allocate();
  if (conn) {
    conn.request(buff, message, state, callback);
    return;
  }

  // TODO: no connection
};

Client.prototype.send = function (buff, message, callback) {
  return this._send(buff, message, 'Request', callback);
};

Client.prototype.sendLob = function (buff, message, callback) {
  return this._send(buff, message, 'LobRequest', callback);
};

Client.prototype.isValid = function () {
  return new Promise((resolve) => {
    if (!this.pool) {
      resolve(false);
      return;
    }
    var message = new Message(constants.Operation.OP_KILL_CONTEXT);
    message.ContextIDList = [Long.NEG_ONE];
    var buff = helper.buildKillCursorMessage(message, this.isBigEndian);
    this.send(buff, message, function (err) {
      resolve(!err);
    });
  });
};

Client.prototype.ready = function () {
  return new Promise((resolve) => {
    if (this.pool.readyState) {
      resolve();
    } else {
      this.pool.once('ready', resolve);
    }
  });
};

/**
 * send disconnect request to db, then disconnect with server
 */
Client.prototype.disconnect = function () {
  return new Promise((resolve, reject) => {
    var buff = helper.buildDisconnectRequest(this.isBigEndian);
    this.pool.disconnect(buff, callbackWrap(resolve, reject));
    this.pool = null;
  });
};

/**
 * Set the attributes of the session.
 * @param options The configuration options for session.The options are as below:
 *
 * PreferedInstance : indicate which instance to respond read request in current session.
 * eg:{"PreferedInstance":"m"/"M"/"s"/"S"/"a"/"A"/1-7},
 * prefer to choose "read and write instance"/"read only instance"/"anyone instance"/instance1-insatance7,
 * default to be {"PreferedInstance":"A"}, means would like to choose anyone instance to respond read request such as query.
 */
Client.prototype.setSessionAttr = function (options) {
  if (!options || !options[constants.FIELD_PREFERED_INSTANCE]) {
    throw new SDBError('SDB_INVALIDARG');
  }
  var attr = {};
  var value = options[constants.FIELD_PREFERED_INSTANCE];
  if (typeof value === "number") {
    if (value < 1 || value > 7) {
      throw new SDBError('SDB_INVALIDARG');
    }
    attr[constants.FIELD_PREFERED_INSTANCE] = value;
  } else if (typeof value === "string") {
    var val = constants.PreferInstanceType.INS_TYPE_MIN;
    if (value === "M" || value === "m") {
      val = constants.PreferInstanceType.INS_MASTER;
    } else if (value === "S" || value === "s" ||
      value === "A" || value === "a") {
      val = constants.PreferInstanceType.INS_SLAVE;
    } else {
      throw new SDBError('SDB_INVALIDARG');
    }
    attr[constants.FIELD_PREFERED_INSTANCE] = val;
  } else {
    throw new SDBError('SDB_INVALIDARG');
  }
  var command = constants.ADMIN_PROMPT + constants.SETSESS_ATTR;
  return new Promise((resolve, reject) => {
    this.sendAdminCommand(command, attr, {}, {}, {}, callbackWrap(resolve, reject));
  });
};

Client.prototype.sendAdminCommand = function (command, matcher, selector, orderBy, hint, callback) {
  this.sendAdminCommand2(command, matcher, selector, orderBy, hint,
    Long.ZERO, Long.NEG_ONE, 0, callback);
};

Client.prototype.sendAdminCommand2 = function (command, matcher, selector, orderBy, hint, skipRows, returnRows, flag, callback) {
  var message = new Message(constants.Operation.OP_QUERY);
  message.CollectionFullName = command;
  message.Version = constants.DEFAULT_VERSION;
  message.W = constants.DEFAULT_W;
  message.Padding = 0;
  message.Flags = flag;
  message.NodeID = constants.ZERO_NODEID;
  message.RequestID = getNextRequstID(); // 0
  message.SkipRowsCount = skipRows;
  message.ReturnRowsCount = returnRows;
  // matcher
  message.Matcher = matcher || {};
  // selector
  message.Selector = selector || {};
  // orderBy
  message.OrderBy = orderBy || {};
  // hint
  message.Hint = hint || {};

  var buff = helper.buildQueryRequest(message, this.isBigEndian);
  this.send(buff, message, callback);
};

var mapping = {};
// list type
mapping[constants.SDB_LIST_CONTEXTS] = constants.CONTEXTS;
mapping[constants.SDB_LIST_CONTEXTS_CURRENT] = constants.CONTEXTS_CUR;
mapping[constants.SDB_LIST_SESSIONS] = constants.SESSIONS;
mapping[constants.SDB_LIST_SESSIONS_CURRENT] = constants.SESSIONS_CUR;
mapping[constants.SDB_LIST_COLLECTIONS] = constants.COLLECTIONS;
mapping[constants.SDB_LIST_COLLECTIONSPACES] = constants.COLSPACES;
mapping[constants.SDB_LIST_STORAGEUNITS] = constants.STOREUNITS;
mapping[constants.SDB_LIST_GROUPS] = constants.GROUPS;
mapping[constants.SDB_LIST_STOREPROCEDURES] = constants.PROCEDURES;
mapping[constants.SDB_LIST_DOMAINS] = constants.DOMAINS;
mapping[constants.SDB_LIST_TASKS] = constants.TASKS;
mapping[constants.SDB_LIST_CS_IN_DOMAIN] = constants.CS_IN_DOMAIN;
mapping[constants.SDB_LIST_CL_IN_DOMAIN] = constants.CL_IN_DOMAIN;
// snapshot type
var snapshotType = {};
snapshotType[constants.SDB_SNAP_CONTEXTS] = constants.CONTEXTS;
snapshotType[constants.SDB_SNAP_CONTEXTS_CURRENT] = constants.CONTEXTS_CUR;
snapshotType[constants.SDB_SNAP_SESSIONS] = constants.SESSIONS;
snapshotType[constants.SDB_SNAP_SESSIONS_CURRENT] = constants.SESSIONS_CUR;
snapshotType[constants.SDB_SNAP_COLLECTIONS] = constants.COLLECTIONS;
snapshotType[constants.SDB_SNAP_COLLECTIONSPACES] = constants.COLSPACES;
snapshotType[constants.SDB_SNAP_DATABASE] = constants.DATABASE;
snapshotType[constants.SDB_SNAP_SYSTEM] = constants.SYSTEM;
snapshotType[constants.SDB_SNAP_CATALOG] = constants.CATA;

Client.prototype.getList = function (type, matcher, selector, orderBy) {
  var command = constants.ADMIN_PROMPT + constants.LIST_CMD + ' ';
  if (!mapping.hasOwnProperty(type)) {
    throw new Error('未知类型：' + type);
  }
  command += mapping[type];

  if (arguments.length === 1) {
    matcher = {};
    selector = {};
    orderBy = {};
  }

  var that = this;
  return new Promise((resolve, reject) => {
    this.sendAdminCommand(command, matcher, selector, orderBy, {}, function (err, response) {
      if (!err) {
        return resolve(new Cursor(response, that));
      }
      if (err.flags === constants.SDB_DMS_EOC) {
        resolve(null);
      } else {
        reject(err);
      }
    });
  });
};

/**
 * Create collection space
 * @param {String} name collection space name
 */
Client.prototype.createCollectionSpace = function (name, options) {
  var matcher = {};
  matcher[constants.FIELD_NAME] = name;

  // createCollectionSpace(name);
  if (!options) {
    matcher[constants.FIELD_PAGESIZE] = constants.SDB_PAGESIZE_DEFAULT;
  } else if (typeof options === 'object') {
    // createCollectionSpace(name, options);
    Object.assign(matcher, options);
  } else {
    // createCollectionSpace(name, pageSize);
    matcher[constants.FIELD_PAGESIZE] = options;
  }

  var pageSize = matcher[constants.FIELD_PAGESIZE];
  if (pageSize) {
    if (pageSize !== constants.SDB_PAGESIZE_4K &&
      pageSize !== constants.SDB_PAGESIZE_8K &&
      pageSize !== constants.SDB_PAGESIZE_16K &&
      pageSize !== constants.SDB_PAGESIZE_32K &&
      pageSize !== constants.SDB_PAGESIZE_64K &&
      pageSize !== constants.SDB_PAGESIZE_DEFAULT) {
      throw new Error('Invalid args');
    }
  }

  var that = this;
  var command = constants.ADMIN_PROMPT + constants.CREATE_CMD + ' ' + constants.COLSPACE;

  return new Promise((resolve, reject) => {
    this.sendAdminCommand(command, matcher, {}, {}, {}, function (err) {
      if (err) {
        return reject(err);
      }
      resolve(new CollectionSpace(that, name));
    });
  });
};

/**
 * Drop collection space
 * @param {String} name collection space name
 */
Client.prototype.dropCollectionSpace = function (name) {
  var command = constants.ADMIN_PROMPT + constants.DROP_CMD + ' ' + constants.COLSPACE;
  var matcher = {};
  matcher[constants.FIELD_NAME] = name;
  return new Promise((resolve, reject) => {
    this.sendAdminCommand(command, matcher, {}, {}, {}, callbackWrap(resolve, reject));
  });
};

/**
 * Whether the collection space exist
 * @param {String} name collection space name
 */
Client.prototype.isCollectionSpaceExist = function (name) {
  var command = constants.ADMIN_PROMPT + constants.TEST_CMD + ' ' + constants.COLSPACE;
  var matcher = {};
  matcher[constants.FIELD_NAME] = name;

  return new Promise((resolve, reject) => {
    this.sendAdminCommand(command, matcher, {}, {}, {}, function (err) {
      if (!err) {
        return resolve(true);
      }
      if (err.flags === errors.SDB_DMS_CS_NOTEXIST) {
        resolve(false);
      } else {
        reject(err);
      }
    });
  });
};

/**
 * Get collection space by name
 * @param {String} name collection space name
 */
Client.prototype.getCollectionSpace = function (name) {
  return this.isCollectionSpaceExist(name).then((exist) => {
    if (exist) {
      return Promise.resolve(new CollectionSpace(this, name));
    }
    return Promise.reject(new SDBError('SDB_DMS_CS_NOTEXIST'));
  }, () => {
    return Promise.reject(new SDBError('SDB_DMS_CS_NOTEXIST'));
  });
};

Client.prototype.getCollectionSpaces = function () {
  return this.getList(constants.SDB_LIST_COLLECTIONSPACES);
};

/**
 * Create a store procedure.
 * @param code The code of store procedure
 * @exception com.sequoiadb.exception.BaseException
 */
Client.prototype.createProcedure = function (code) {
  if (!code) {
    throw new SDBError('SDB_INVALIDARG');
  }

  var matcher = {};
  matcher[constants.FIELD_FUNCTYPE] = new Code(code);
  matcher[constants.FMP_FUNC_TYPE] = constants.FMP_FUNC_TYPE_JS;
  var command = constants.ADMIN_PROMPT + constants.CRT_PROCEDURES_CMD;

  return new Promise((resolve, reject) => {
    this.sendAdminCommand2(command, matcher, null, null, null,
                          Long.ZERO, Long.ZERO, -1,
                          callbackWrap(resolve, reject));
  });
};

/**
 * Remove a store procedure.
 * @param name The name of store procedure to be removed
 */
Client.prototype.removeProcedure = function (name) {
  if (!name) {
    throw new SDBError('SDB_INVALIDARG', name);
  }

  var matcher = {};
  matcher[constants.FIELD_FUNCTYPE] = name;
  var command = constants.ADMIN_PROMPT + constants.RM_PROCEDURES_CMD;

  return new Promise((resolve, reject) => {
    this.sendAdminCommand2(command, matcher, null, null, null,
                          Long.ZERO, Long.ZERO, -1,
                          callbackWrap(resolve, reject));
  });
};

/**
 * List the store procedures.
 * @param condition The condition of list eg: {"name":"sum"}
 */
Client.prototype.getProcedures = function (matcher) {
  return this.getList(constants.SDB_LIST_STOREPROCEDURES, matcher, {}, {});
};

/**
 * Eval javascript code.
 * @param code The javasript code
 * @return The result of the eval operation, including the return value type,
 *         the return data and the error message. If succeed to eval, error message is null,
 *         and we can extract the eval result from the return cursor and return type,
 *         if not, the return cursor and the return type are null, we can extract
 *         the error mssage for more detail.
 */
Client.prototype.evalJS = function (code) {
  if (!code){
    throw new SDBError('SDB_INVALIDARG');
  }

  var matcher = {};
  matcher[constants.FIELD_FUNCTYPE] = new Code(code);
  matcher[constants.FMP_FUNC_TYPE] = constants.FMP_FUNC_TYPE_JS;

  var that = this;
  var command = constants.ADMIN_PROMPT + constants.EVAL_CMD;

  return new Promise((resolve, reject) => {
    this.sendAdminCommand2(command, matcher, null, null, null, Long.ZERO,
      Long.ZERO, -1,
      function (err, response) {
        if (err) {
          return reject(err);
        }

        var result = {};
        var typeValue = response.NumReturned;
        result.returnType = typeValue;
        result.cursor = new Cursor(response, that);
        resolve(result);
      });
  });
};

/**
 * Execute sql in database.
 * @param sql the SQL command
 * @return the DBCursor of the result
 */
Client.prototype.exec = function (sql) {
  var message = new Message(constants.Operation.OP_SQL);
  message.RequestID = Long.ZERO; // getNextRequstID();
  message.NodeID = constants.ZERO_NODEID;
  var buff = helper.buildSQLMessage(message, sql, this.isBigEndian);
  var that = this;

  return new Promise((resolve, reject) => {
    this.send(buff, message, function (err, response) {
      if (err) {
        if (err.flags === constants.SDB_DMS_EOC) {
          resolve(null);
        } else {
          reject(new SDBError(err.flags, sql));
        }
        return;
      }

      if (!response || response.ContextIDList.length !== 1 ||
        response.ContextIDList[0] === -1) {
        return resolve(null);
      }
      return resolve(new Cursor(response, that));
    });
  });
};

/**
 * Execute sql in database.
 * @param {String} sql the SQL command.
 */
Client.prototype.execUpdate = function (sql) {
  var message = new Message(constants.Operation.OP_SQL);
  message.RequestID = Long.ZERO; // getNextRequstID();
  message.NodeID = constants.ZERO_NODEID;
  var buff = helper.buildSQLMessage(message, sql, this.isBigEndian);

  return new Promise((resolve, reject) => {
    this.send(buff, message, callbackWrap(resolve, reject));
  });
};

/**
 * Create user in database.
 * @param {String} username the username
 * @param {String} password the password
 */
Client.prototype.createUser = function (username, password, callback) {
  if (!username || !password) {
    throw new SDBError('SDB_INVALIDARG');
  }

  var message = new Message(constants.Operation.MSG_AUTH_CRTUSR_REQ);
  message.RequestID = Long.ZERO;
  var pass = helper.md5(password);
  var buff = helper.buildAuthMessage(message, username, pass, this.isBigEndian);
  this.send(buff, message, callbackWrap(callback));
};

/**
 * Remove user from database.
 * @param {String} username the username
 * @param {String} password the password
 */
Client.prototype.removeUser = function (username, password, callback) {
  if (!username || !password) {
    throw new SDBError('SDB_INVALIDARG');
  }

  var message = new Message(constants.Operation.MSG_AUTH_DELUSR_REQ);
  message.RequestID = Long.ZERO;
  var pass = helper.md5(password);
  var buff = helper.buildAuthMessage(message, username, pass, this.isBigEndian);
  this.send(buff, message, callbackWrap(callback));
};

/**
 * Begin transaction
 */
Client.prototype.beginTransaction = function (callback) {
  var message = new Message(constants.Operation.OP_TRANS_BEGIN);
  message.RequestID = getNextRequstID();
  var buff = helper.buildTransactionRequest(message, this.isBigEndian);
  this.send(buff, message, callbackWrap(callback));
};

/**
 * Commit transaction
 */
Client.prototype.commitTransaction = function (callback) {
  var message = new Message(constants.Operation.OP_TRANS_COMMIT);
  message.RequestID = getNextRequstID();
  var buff = helper.buildTransactionRequest(message, this.isBigEndian);
  this.send(buff, message, callbackWrap(callback));
};

/**
 * rollback transaction
 */
Client.prototype.rollbackTransaction = function (callback) {
  var message = new Message(constants.Operation.OP_TRANS_ROLLBACK);
  message.RequestID = getNextRequstID();
  var buff = helper.buildTransactionRequest(message, this.isBigEndian);
  this.send(buff, message, callbackWrap(callback));
};

/**
 * Get all the collections
 * @return dbCursor of all collecions
 */
Client.prototype.getCollections = function () {
  return this.getList(constants.SDB_LIST_COLLECTIONS);
};

/**
 * Get all the collecion space names
 * @return A list of all collecion space names
 */
Client.prototype.getCollectionNames = function () {
  return this.getCollections().then(function (cursor) {
    if (!cursor) {
      return Promise.resolve([]);
    }

    return cursor.all().then(function (items) {
      var names = items.map(function (item) {
        return item.Name;
      });
      return Promise.resolve(names);
    });
  });
};

/**
 * Get all the collecion space names
 * @return A list of all collecion space names
 */
Client.prototype.getCollectionSpaceNames = function () {
  return this.getCollectionSpaces().then(function (cursor) {
    if (!cursor) {
      return Promise.resolve([]);
    }

    return cursor.all().then(function (items) {
      var names = items.map(function (item) {
        return item.Name;
      });
      return Promise.resolve(names);
    });
  });
};

/**
 * Get all the collecion space names
 * @return A list of all collecion space names
 */
Client.prototype.getStorageUnits = function () {
  return this.getList(constants.SDB_LIST_STORAGEUNITS).then(function (cursor) {
    if (!cursor) {
      return Promise.resolve([]);
    }

    return cursor.all().then(function (items) {
      return Promise.resolve(items.map(function (item) {
        return item.Name;
      }));
    });
  });
};

/**
 * List domains.
 * @param matcher The matching rule, return all the documents if null
 * @param selector The selective rule, return the whole document if null
 * @param orderBy The ordered rule, never sort if null
 * @return the cursor of the result.
 */
Client.prototype.getDomains = function (matcher, selector, orderBy, hint, callback) {
  this.getList(constants.SDB_LIST_DOMAINS, matcher, selector, orderBy, callback);
};

/**
 * Get the specified domain.
 * @param domainName the name of the domain
 * @return the Domain instance
 *            If the domain not exit, throw BaseException with the error type "SDB_CAT_DOMAIN_NOT_EXIST"
 */
Client.prototype.getDomain = function (domainName, callback) {
  var that = this;
  this.isDomainExist(domainName, function (err, exist) {
    if (err) {
      return callback(err);
    }

    if (exist) {
      callback(null, new Domain(that, domainName));
    } else {
      callback(null, null);
    }
  });
};

/**
 * Verify the existence of domain.
 * @param domainName the name of domain
 * @return True if existed or False if not existed
 */
Client.prototype.isDomainExist = function (domainName, callback) {
  if (!domainName) {
    throw new SDBError('SDB_INVALIDARG');
  }

  var matcher = {};
  matcher[constants.FIELD_NAME] = domainName;
  this.getList(constants.SDB_LIST_DOMAINS, matcher, null, null, function (err, cursor) {
    if (err) {
      return callback(err);
    }
    if (!cursor) {
      return callback(null, false);
    }
    cursor.current(function (err, item) {
      if (err) {
        return callback(err);
      }
      callback(null, !!item);
    });
  });
};

/**
 * Create a domain.
 * @param domainName The name of the creating domain
 * @param options The options for the domain. The options are as below:
 * - Groups: the list of the replica groups' names which the domain is going to contain.
 *                 eg: { "Groups": [ "group1", "group2", "group3" ] }
 *                 If this argument is not included, the domain will contain all replica groups in the cluster.
 * - AutoSplit    : If this option is set to be true, while creating collection(ShardingType is "hash") in this domain,
 *                    the data of this collection will be split(hash split) into all the groups in this domain automatically.
 *                    However, it won't automatically split data into those groups which were add into this domain later.
 *                    eg: { "Groups": [ "group1", "group2", "group3" ], "AutoSplit: true" }
 * @return the newly created collection space object
 */
Client.prototype.createDomain = function (domainName, options, callback) {
  if (!domainName) {
    throw new SDBError('SDB_INVALIDARG');
  }

  if (typeof options === 'function') {
    callback = options;
    options = null;
  }

  var that = this;
  this.isDomainExist(domainName, function (err, exist) {
    if (err) {
      return callback(err);
    }

    if (exist) {
      return callback(new SDBError("SDB_CAT_DOMAIN_EXIST"));
    }

    var matcher = {};
    matcher[constants.FIELD_NAME] = domainName;
    if (options) {
      matcher[constants.FIELD_OPTIONS] = options;
    }
    var command = constants.ADMIN_PROMPT + constants.CREATE_CMD + ' ' + constants.DOMAIN;
    that.sendAdminCommand(command, matcher, null, null, null, function (err, response) {
      if (err) {
        return callback(err);
      }
      callback(null, new Domain(that, domainName));
    });
  });
};

Client.prototype.dropDomain = function (name, callback) {
  if (!name) {
    throw new SDBError('SDB_INVALIDARG');
  }
  var matcher = {};
  matcher[constants.FIELD_NAME] = name;

  var command = constants.ADMIN_PROMPT + constants.DROP_CMD + ' ' + constants.DOMAIN;
  this.sendAdminCommand(command, matcher, null, null, null, callbackWrap(callback));
};

/**
 * Flush the options to configuration file
 * @param param
 *            The param of flush, pass {"Global":true} or {"Global":false}
 *            In cluster environment, passing {"Global":true} will flush data's and catalog's configuration file,
 *            while passing {"Global":false} will flush coord's configuration file
 *            In stand-alone environment, both them have the same behaviour
 */
Client.prototype.flushConfigure = function (matcher, callback) {
  var command = constants.ADMIN_PROMPT + constants.EXPORT_CONFIG_CMD;
  this.sendAdminCommand2(command, matcher, null, null, null,
                         Long.ZERO, Long.NEG_ONE, -1, callbackWrap(callback));
};

/**
 * Reset the snapshot.
 */
Client.prototype.resetSnapshot = function (callback) {
  var command = constants.ADMIN_PROMPT + constants.SNAP_CMD + ' ' + constants.RESET;
  this.sendAdminCommand2(command, null, null, null, null,
                         Long.ZERO, Long.NEG_ONE, -1, callbackWrap(callback));
};

/**
 * Get snapshot of the database.
 * @param snapType The snapshot types are as below:
 * - Sequoiadb.SDB_SNAP_CONTEXTS   : Get all contexts' snapshot
 * - Sequoiadb.SDB_SNAP_CONTEXTS_CURRENT        : Get the current context's snapshot
 * - Sequoiadb.SDB_SNAP_SESSIONS        : Get all sessions' snapshot
 * - Sequoiadb.SDB_SNAP_SESSIONS_CURRENT        : Get the current session's snapshot
 * - Sequoiadb.SDB_SNAP_COLLECTIONS        : Get the collections' snapshot
 * - Sequoiadb.SDB_SNAP_COLLECTIONSPACES        : Get the collection spaces' snapshot
 * - Sequoiadb.SDB_SNAP_DATABASE        : Get database's snapshot
 * - Sequoiadb.SDB_SNAP_SYSTEM        : Get system's snapshot
 * - Sequoiadb.SDB_SNAP_CATALOG        : Get catalog's snapshot
 * - Sequoiadb.SDB_LIST_GROUPS        : Get replica group list ( only applicable in sharding env )
 * - Sequoiadb.SDB_LIST_STOREPROCEDURES           : Get stored procedure list ( only applicable in sharding env )
 * @param matcher the matching rule, match all the documents if null
 * @param selector the selective rule, return the whole document if null
 * @param orderBy the ordered rule, never sort if null
 * @return {Cursor} the Cursor instance of the result
 */
Client.prototype.getSnapshot = function (type, matcher, selector, orderBy, callback) {
  if (!snapshotType.hasOwnProperty(type)) {
    throw new SDBError('SDB_INVALIDARG');
  }

  var command = constants.ADMIN_PROMPT + constants.SNAP_CMD + ' ' + snapshotType[type];

  var that = this;
  this.sendAdminCommand2(command, matcher, selector, orderBy, null,
                         Long.ZERO, Long.NEG_ONE, -1, function (err, response) {
    if (!err) {
      callback(null, new Cursor(response, that));
      return;
    }
    if (err.flags === constants.SDB_DMS_EOC) {
      callback(null, null);
    } else {
      callback(err);
    }
  });
};

Client.prototype.getReplicaGroups = function (callback) {
  this.getList(constants.SDB_LIST_GROUPS, callback);
};

Client.prototype.createReplicaGroup = function (name) {
  if (!name) {
    throw new SDBError('SDB_INVALIDARG');
  }
  var command = constants.ADMIN_PROMPT + constants.CREATE_CMD + ' ' +
    constants.GROUP;

  var matcher = {};
  matcher[constants.FIELD_GROUPNAME] = name;

  var that = this;

  return new Promise((resolve, reject) => {
    this.sendAdminCommand(command, matcher, {}, {}, {}, function (err) {
      if (err) {
        return reject(err);
      }
      that.getReplicaGroupByName(name, common.make(resolve, reject));
    });
  });
};

/**
 * Create the Replica Catalog Group with given options
 * @param hostName The host name
 * @param port The port
 * @param dbpath The database path
 * @param configure The configure options
 */
Client.prototype.createReplicaCataGroup = function (hostname, port, dbpath, configure, callback) {
  if (!hostname || !port || !dbpath) {
    throw new SDBError('SDB_INVALIDARG');
  }

  var command = constants.ADMIN_PROMPT + constants.CREATE_CMD + ' ' +
    constants.CATALOG + ' ' + constants.GROUP;
  var condition = {};
  condition[constants.FIELD_HOSTNAME] = hostname;
  condition[constants.SVCNAME] = '' + port;
  condition[constants.DBPATH] = dbpath;
  if (configure) {
    var keys = Object.keys(configure);
    for (var i = 0; i < keys.length; i++) {
      var key = keys[i];
      if (key === constants.FIELD_HOSTNAME ||
        key === constants.SVCNAME ||
        key === constants.DBPATH) {
        condition[key] = '' + configure[key];
      }
    }
  }
  this.sendAdminCommand(command, condition, {}, {}, {}, callback);
};

/**
 * Remove ReplicaGroup by name
 * @param name The group name
 */
Client.prototype.removeReplicaGroup = function (name, callback) {
  if (!name) {
    throw new SDBError('SDB_INVALIDARG');
  }
  var command = constants.ADMIN_PROMPT + constants.REMOVE_CMD + ' ' +
    constants.GROUP;

  var matcher = {};
  matcher[constants.FIELD_GROUPNAME] = name;

  this.sendAdminCommand(command, matcher, {}, {}, {}, callbackWrap(callback));
};

/**
 * Get the ReplicaGroup by name
 * @param groupName The group name
 * @return The fitted ReplicaGroup or null
 */
Client.prototype.getReplicaGroupByName = function (group, callback) {
  var matcher = {};
  matcher[constants.FIELD_GROUPNAME] = group;
  var that = this;
  this.getList(constants.SDB_LIST_GROUPS, matcher, {}, {}, function (err, cursor) {
    if (err) {
      return callback(err);
    }

    if (!cursor) {
      return callback(new SDBError("SDB_SYS"));
    }

    cursor.next(function (err, detail) {
      if (err) {
        return callback(err);
      }
      if (detail) {
        var groupId = detail[constants.FIELD_GROUPID];
        if (typeof groupId !== "number") {
          return callback(new SDBError('SDB_SYS'));
        }
        callback(null, new ReplicaGroup(that, group, groupId));
      } else {
        callback(null, null);
      }
    });
  });
};

/**
 * Get the ReplicaGroup by name
 * @param groupName The group name
 * @return The fitted ReplicaGroup or null
 */
Client.prototype.getReplicaGroupById = function (groupId, callback) {
  var matcher = {};
  matcher[constants.FIELD_GROUPID] = groupId;
  var that = this;
  this.getList(constants.SDB_LIST_GROUPS, matcher, {}, {}, function (err, cursor) {
    if (err) {
      return callback(err);
    }

    if (!cursor) {
      return callback(new SDBError("SDB_SYS"));
    }

    cursor.next(function (err, detail) {
      if (err) {
        return callback(err);
      }
      if (detail) {
        var group = detail[constants.FIELD_GROUPNAME];
        if (typeof group !== "string") {
          return callback(new SDBError('SDB_SYS'));
        }
        callback(null, new ReplicaGroup(that, group, groupId));
      } else {
        callback(null, null);
      }
    });
  });
};

Client.prototype.activateReplicaGroup = function (name, callback) {
  this.getReplicaGroupByName(name, function (err, group) {
    if (err) {
      return callback(err);
    }

    group.start(function (err, result) {
      if (err) {
        return callback(err);
      }
      result ? callback(null, group) : callback(null, null);
    });
  });
};

Client.prototype.getTasks = function (matcher, selector, orderBy, hint, callback) {
  var command = constants.ADMIN_PROMPT + constants.LIST_TASK_CMD;
  var that = this;
  this.sendAdminCommand(command, matcher, selector, orderBy, hint, function (err, response) {
    if (err) {
      return callback(err);
    }
    return callback(null, new Cursor(response, that));
  });
};

Client.prototype.waitTasks = function (taskIds, callback) {
  if (!taskIds || taskIds.length === 0) {
    throw new SDBError('SDB_INVALIDARG');
  }
  var matcher = {};

  matcher[constants.FIELD_TASKID] = {
    "$in": taskIds
  };
  var command = constants.ADMIN_PROMPT + constants.WAIT_TASK_CMD;
  this.sendAdminCommand(command, matcher, {}, {}, {}, callback);
};

Client.prototype.cancelTask = function (taskId, isAsync, callback) {
  if (taskId <= 0) {
    throw new SDBError('SDB_INVALIDARG');
  }
  var matcher = {};
  matcher[constants.FIELD_TASKID] = taskId;
  matcher[constants.FIELD_ASYNC] = isAsync;
  var command = constants.ADMIN_PROMPT + constants.CANCEL_TASK_CMD;
  this.sendAdminCommand(command, matcher, {}, {}, {}, callback);
};

/*
 * @param options Contains a series of backup configuration infomations.
 *         Backup the whole cluster if null. The "options" contains 5 options as below.
 *         All the elements in options are optional.
 *         eg: {"GroupName":["rgName1", "rgName2"], "Path":"/opt/sequoiadb/backup",
 *             "Name":"backupName", "Description":description, "EnsureInc":true, "OverWrite":true}
 * - GroupName   : The replica groups which to be backuped
 * - Path        : The backup path, if not assign, use the backup path assigned in configuration file
 * - Name        : The name for the backup
 * - Description : The description for the backup
 * - EnsureInc   : Whether excute increment synchronization, default to be false
 * - OverWrite   : Whether overwrite the old backup file, default to be false
 */
Client.prototype.backupOffline = function (options) {
  if (!options) {
    throw new SDBError('SDB_INVALIDARG');
  }
  var names = Object.keys(options);
  // if names.Length equals 0, use default options by engine
  //if (names.length === 0) {
  //  throw new SDBError('SDB_INVALIDARG');
  //}

  for (var i = 0; i < names.length; i++) {
    var name = names[i];
    if (name !== constants.FIELD_GROUPNAME &&
      name !== constants.FIELD_NAME &&
      name !== constants.FIELD_PATH &&
      name !== constants.FIELD_DESP &&
      name !== constants.FIELD_ENSURE_INC &&
      name !== constants.FIELD_OVERWRITE) {
      throw new SDBError('SDB_INVALIDARG');
    }
  }

  var command = constants.ADMIN_PROMPT + constants.BACKUP_OFFLINE_CMD;

  return new Promise((resolve, reject) => {
    this.sendAdminCommand(command, options, {}, {}, {}, common.make(resolve, reject));
  });
};

/**
 * List the backups.
 * @param options Contains configuration infomations for remove backups, list all the backups in the default backup path if null.
 *         The "options" contains 3 options as below. All the elements in options are optional.
 *         eg: {"GroupName":["rgName1", "rgName2"], "Path":"/opt/sequoiadb/backup", "Name":"backupName"}
 * - GroupName   : Assign the backups of specifed replica groups to be list
 * - Path        : Assign the backups in specifed path to be list, if not assign, use the backup path asigned in the configuration file
 * - Name        : Assign the backups with specifed name to be list
 *
 * @param matcher The matching rule, return all the documents if null
 * @param selector The selective rule, return the whole document if null
 * @param orderBy The ordered rule, never sort if null
 * @return the DBCursor of the backup or null while having no backup infonation
 */
Client.prototype.getBackups = function (options, matcher, selector, orderBy) {
  if (!options) {
    throw new SDBError('SDB_INVALIDARG');
  }

  var names = Object.keys(options);
  for (var i = 0; i < names.length; i++) {
    var name = names[i];
    if (name !== constants.FIELD_GROUPNAME &&
      name !== constants.FIELD_NAME &&
      name !== constants.FIELD_PATH) {
      throw new SDBError('SDB_INVALIDARG');
    }
  }

  var command = constants.ADMIN_PROMPT + constants.LIST_BACKUP_CMD;
  var that = this;
  return new Promise((resolve, reject) => {
    this.sendAdminCommand(command, matcher, selector, orderBy, options, function (err, response) {
      if (!err) {
        return resolve(new Cursor(response, that));
      }
      if (err.flags === constants.SDB_DMS_EOC) {
        resolve(null);
      } else {
        reject(err);
      }
    });
  });
};

/**
 * Remove the backups.
 * @param options Contains configuration infomations for remove backups, remove all the backups in the default backup path if null.
 *                 The "options" contains 3 options as below. All the elements in options are optional.
 *                 eg: {"GroupName":["rgName1", "rgName2"], "Path":"/opt/sequoiadb/backup", "Name":"backupName"}
 * - GroupName   : Assign the backups of specifed replica grouops to be remove
 * - Path        : Assign the backups in specifed path to be remove, if not assign, use the backup path asigned in the configuration file
 * - Name        : Assign the backups with specifed name to be remove
 */
Client.prototype.removeBackup = function (matcher) {
  if (!matcher) {
    throw new SDBError('SDB_INVALIDARG');
  }

  var names = Object.keys(matcher);
  for (var i = 0; i < names.length; i++) {
    var name = names[i];
    if (name !== constants.FIELD_GROUPNAME &&
      name !== constants.FIELD_NAME &&
      name !== constants.FIELD_PATH) {
      throw new SDBError('SDB_INVALIDARG');
    }
  }

  var command = constants.ADMIN_PROMPT + constants.REMOVE_BACKUP_CMD;

  return new Promise((resolve, reject) => {
    this.sendAdminCommand(command, matcher, {}, {}, {}, common.make(resolve, reject));
  });
};

module.exports = Client;
