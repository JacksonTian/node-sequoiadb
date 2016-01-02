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

var Long = require("long");
var bson = require('bson');

var constants = require('./const');
var Message = require('./message');
var helper = require('./helper');
var SDBError = require('./error').SDBError;

var Lob = function (collection) {
  this.collection = collection;
  Object.defineProperty(this, 'client', {
    value: collection.client,
    enumerable: false
  });
  this.id = null;
  this.mode = -1;
  this.size = Long.ZERO;
  this.readOffset = Long.NEG_ONE;
  this.createTime = Long.ZERO;
  this.isOpen = false;
  this.contextID = Long.NEG_ONE;
};

/**
 * SDB_LOB_SEEK_SET 0
 * Change the position from the beginning of lob
 */
Lob.SDB_LOB_SEEK_SET = 0;

/**
 * SDB_LOB_SEEK_CUR 1
 * Change the position from the current position of lob
 */
Lob.SDB_LOB_SEEK_CUR = 1;

/**
 * SDB_LOB_SEEK_END 2
 * Change the position from the end of lob
 */
Lob.SDB_LOB_SEEK_END = 2;

/**
 * SDB_LOB_CREATEONLY 0x00000001
 * Open a new lob only
 */
Lob.SDB_LOB_CREATEONLY = 0x00000001;

/**
 * SDB_LOB_READ 0x00000004
 * Open an existing lob to read
 */
Lob.SDB_LOB_READ = 0x00000004;

// the max lob data size to send for one message
var SDB_LOB_MAX_DATA_LENGTH = 1024 * 1024;
var SDB_LOB_DEFAULT_OFFSET = Long.NEG_ONE;
var SDB_LOB_DEFAULT_SEQ = 0;

/**
 * Open an exist lob, or create a lob
 * @param {ObjectID} id   the lob's id
 * @param {Number} mode available mode is SDB_LOB_CREATEONLY or SDB_LOB_READ.
 *              SDB_LOB_CREATEONLY
 *                  create a new lob with given id, if id is null, it will
 *                  be generated in this function;
 *              SDB_LOB_READ
 *                  read an exist lob
 */
Lob.prototype.open = function (id, mode, callback) {
  if (this.isOpen) {
    return callback(new SDBError('SDB_LOB_HAS_OPEN'));
  }

  if (Lob.SDB_LOB_CREATEONLY !== mode && Lob.SDB_LOB_READ !== mode) {
    throw new SDBError("SDB_INVALIDARG");
  }

  if (Lob.SDB_LOB_READ === mode && !id) {
    throw new SDBError("SDB_INVALIDARG");
  }
  // gen oid
  this.id = id;
  if (Lob.SDB_LOB_CREATEONLY === mode && !this.id) {
    this.id = bson.ObjectID.createPk();
  }
  // mode
  this.mode = mode;
  this.readOffset = Long.ZERO;
  var that = this;
  // open
  this._open(function (err) {
    if (err) {
      return callback(err);
    }
    that.isOpen = true;
    callback(null);
  });
};

/**
 * Close the lob
 * @return void
 */
Lob.prototype.close = function () {
  var message = new Message();
  // build message
  // MsgHeader
  message.OperationCode = constants.Operation.MSG_BS_LOB_CLOSE_REQ;
  message.NodeID = constants.ZERO_NODEID;
  message.RequestID = Long.ZERO;
  // the rest part of _MsgOpLOb
  message.Version = constants.DEFAULT_VERSION;
  message.W = constants.DEFAULT_W;
  message.Padding = 0;
  message.Flags = constants.DEFAULT_FLAGS;
  message.ContextIDList = [this.contextID];
  message.BsonLen = 0;

  var that = this;
  // build send msg
  var buff = helper.buildCloseLobRequest(message, this.client.isBigEndian);

  return new Promise((resolve, reject) => {
    this.client.send(buff, message, function (err) {
      if (err) {
        return reject(err);
      }
      that.isOpen = false;
      resolve();
    });
  });
};

/**
 * Reads up to b.length bytes of data from this
 *               lob into an array of bytes.
 * @param       b   the buffer into which the data is read.
 * @return      the total number of bytes read into the buffer, or
 *               <code>-1</code> if there is no more data because the end of
 *               the file has been reached, or <code>0<code> if
 *               <code>b.length</code> is Zero.
 */
Lob.prototype.read = function (len) {
  if (!this.isOpen) {
    throw new SDBError('SDB_LOB_NOT_OPEN');
  }

  if (typeof len === 'undefined') {
    throw new SDBError('SDB_INVALIDARG');
  }

  if (0 === len) {
    return Promise.resolve(new Buffer(0));
  }

  return new Promise((resolve, reject) => {
    this._read(len, function (err, data) {
      if (err) {
        return reject(err);
      }
      resolve(data);
    });
  });
};

Lob.prototype.write = function (buff) {
  if (!this.isOpen) {
    throw new SDBError('SDB_LOB_NOT_OPEN');
  }

  if (!buff) {
    throw new SDBError('SDB_INVALIDARG');
  }

  if (buff.length <= SDB_LOB_MAX_DATA_LENGTH) {
    return this._write(buff);
  }

  var that = this;
  var write = function () {
    var len = Math.min(buff.length, SDB_LOB_MAX_DATA_LENGTH);
    var _buff = buff.slice(0, len);
    return that._write(_buff).then(function () {
      buff = buff.slice(len);
      if (buff.length <= 0) {
        return Promise.resolve();
      } else {
        return write();
      }
    }, function (err) {
      return Promise.reject(err);
    });
  };

  /* if b.length is more then SDB_LOB_MAX_DATA_LENGTH. we will split
   * the data to pieces with length=SDB_LOB_MAX_DATA_LENGTH.
   * besides, data copy is a must in this case.
   */
  return write();
};

/**
 * Writes specified bytes to this lob.
 * @param {Buffer} buff the data
 */
Lob.prototype._write = function (buff) {
  var message = new Message(constants.Operation.MSG_BS_LOB_WRITE_REQ);
  // MsgHeader
  message.NodeID = constants.ZERO_NODEID;
  message.RequestID = Long.ZERO;
  // the rest part of _MsgOpLOb
  message.Version = constants.DEFAULT_VERSION;
  message.W = constants.DEFAULT_W;
  message.Padding = 0;
  message.Flags = constants.DEFAULT_FLAGS;
  message.ContextIDList = [this.contextID];
  message.BsonLen = 0;
  // MsgLobTuple
  message.LobLen = buff.length;
  message.LobSequence = SDB_LOB_DEFAULT_SEQ;
  message.LobOffset = SDB_LOB_DEFAULT_OFFSET;

  var that = this;
  // build send msg
  var bytes = helper.buildWriteLobRequest(message, buff, this.client.isBigEndian);

  return new Promise((resolve, reject) => {
    this.client.send(bytes, message, function (err) {
      if (err) {
        return reject(err);
      }
      that.size = that.size.add(Long.fromNumber(buff.length));
      resolve();
    });
  });
};

/**
 * Change the read position of the lob. The new position is
 * obtained by adding `size` to the position
 * specified by `seekType`. If `seekType`
 * is set to SDB_LOB_SEEK_SET, SDB_LOB_SEEK_CUR, or SDB_LOB_SEEK_END,
 * the offset is relative to the start of the lob, the current
 * position of lob, or the end of lob.
 * @param {} size the adding size.
 * @param seekType  SDB_LOB_SEEK_SET/SDB_LOB_SEEK_CUR/SDB_LOB_SEEK_END
 * @return void
 */
Lob.prototype.seek = function (size, seekType) {
  if (!this.isOpen) {
    throw new SDBError("SDB_LOB_NOT_OPEN");
  }

  if (this.mode !== Lob.SDB_LOB_READ) {
    throw new SDBError("SDB_INVALIDARG");
  }
  var _size = Long.fromNumber(size);

  if (Lob.SDB_LOB_SEEK_SET === seekType) {
    // FIXME: the size is long type
    if (_size.lessThan(0) || _size.greaterThan(this.size)) {
      throw new SDBError("SDB_INVALIDARG");
    }
    this.readOffset = _size;
  } else if (Lob.SDB_LOB_SEEK_CUR === seekType) {
    // FIXME: the size is long type
    var added = this.readOffset.add(_size);
    if (added.greaterThan(this.size) || added.lessThan(0)) {
      throw new SDBError("SDB_INVALIDARG");
    }
    this.readOffset = added;
  } else if (Lob.SDB_LOB_SEEK_END === seekType) {
    // FIXME: the size is long type
    if (_size.lessThan(0) || _size.greaterThan(this.size)) {
      throw new SDBError( "SDB_INVALIDARG");
    }
    // FIXME: the size is long type
    this.readOffset = this.size.subtract(_size);
  } else {
    throw new SDBError("SDB_INVALIDARG");
  }
};

/**
 * Test whether lob has been closed or not
 * @return true for lob has been closed, false for not
 */
Lob.prototype.isClosed = function () {
  return !this.isOpen;
};

/**
 * Get the lob's id
 * @return the lob's id
 */
Lob.prototype.getID = function () {
  return this.id;
};

/**
 * Get the size of lob
 * @return the lob's size
 */
Lob.prototype.getSize = function () {
  return this.size;
};

/**
 * get the create time of lob
 * @return The lob's create time
 */
Lob.prototype.getCreateTime = function () {
  return this.createTime;
};

Lob.prototype._open = function (callback) {
  // add info into object
  var matcher = {};
  matcher[constants.FIELD_COLLECTION] = this.collection.collectionFullName;
  matcher[constants.FIELD_LOB_OID] = this.id;
  matcher[constants.FIELD_LOB_OPEN_MODE] = this.mode;

  var message = new Message(constants.Operation.MSG_BS_LOB_OPEN_REQ);
  // build message
  message.NodeID = constants.ZERO_NODEID;
  message.RequestID = Long.ZERO;
  message.Version = constants.DEFAULT_VERSION;
  message.W = constants.DEFAULT_W;
  message.Padding = 0;
  message.Flags = constants.DEFAULT_FLAGS;
  message.ContextIDList = [constants.DEFAULT_CONTEXTID];
  message.Matcher = matcher;

  var that = this;
  // build send msg
  var buff = helper.buildOpenLobRequest(message, this.client.isBigEndian);
  this.client.send(buff, message, function (err, response) {
    if (err) {
      return callback(err);
    }
    var list = response.ObjectList;
    var obj = list[0];
    if (!obj || obj[constants.FIELD_LOB_SIZE] === undefined ||
      obj[constants.FIELD_LOB_CREATTIME] === undefined) {
      return callback(new SDBError("SDB_SYS"));
    }

    that.size = Long.fromNumber(obj[constants.FIELD_LOB_SIZE]);
    that.createTime = Long.fromNumber(obj[constants.FIELD_LOB_CREATTIME]);
    that.contextID = response.ContextIDList[0];
    callback(null);
  });
};

Lob.prototype._read = function (len, callback) {
  var message = new Message(constants.Operation.MSG_BS_LOB_READ_REQ);
  // MsgHeader
  message.NodeID = constants.ZERO_NODEID;
  message.RequestID = Long.ZERO;
  // the rest part of _MsgOpLOb
  message.Version = constants.DEFAULT_VERSION;
  message.W = constants.DEFAULT_W;
  message.Padding = 0;
  message.Flags = constants.DEFAULT_FLAGS;
  message.ContextIDList = [this.contextID];
  message.BsonLen = 0;
  // MsgLobTuple
  message.LobLen = len;
  message.LobSequence = SDB_LOB_DEFAULT_SEQ;
  message.LobOffset = this.readOffset;

  var that = this;

  // build send msg
  var bytes = helper.buildReadLobRequest(message, this.client.isBigEndian);
  // send msg
  this.client.sendLob(bytes, message, function (err, response) {
    if (!err) {
      var buff = response.LobBuff;
      // FIXME: readOffset is long
      that.readOffset = that.readOffset.add(Long.fromNumber(buff.length));
      return callback(null, buff);
    }
    if (err.flags === constants.SDB_DMS_EOC) {
      callback(null, -1);
    } else {
      callback(err);
    }
  });
};

module.exports = Lob;
