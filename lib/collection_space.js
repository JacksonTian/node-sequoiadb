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

var util = require('util');
var constants = require('./const');
var errors = require('./error').errors;
var Collection = require('./collection');

var CollectionSpace = function (client, name) {
  this.client = client;
  this.name = name;
};

CollectionSpace.prototype.getCollection = function (name) {
  return this.isCollectionExist(name).then((has) => {
    return Promise.resolve(has ? new Collection(this, name) : null);
  });
};

CollectionSpace.prototype.isCollectionExist = function (name) {
  var command = constants.ADMIN_PROMPT + constants.TEST_CMD + ' ' + constants.COLLECTION;
  var matcher = {};
  matcher[constants.FIELD_NAME] = this.name + "." + name;

  return new Promise((resolve, reject) => {
    this.client.sendAdminCommand(command, matcher, {}, {}, {}, (err, response) => {
      if (!err) {
        return resolve(true);
      }

      if (err.flags === errors.SDB_DMS_NOTEXIST) {
        resolve(false);
      } else {
        reject(err);
      }
    });
  });
};

/**
 * Create the named collection in current collection space
 * @param collectionName The collection name
 * @return The DBCollection handle
 */
CollectionSpace.prototype.createCollection = function (name, options) {
  var command = constants.ADMIN_PROMPT + constants.CREATE_CMD + ' ' + constants.COLLECTION;
  var matcher = {};
  matcher[constants.FIELD_NAME] = this.name + "." + name;
  if (options) {
    util._extend(matcher, options);
  }

  return new Promise((resolve, reject) => {
    this.client.sendAdminCommand(command, matcher, {}, {}, {}, (err, response) => {
      if (err) {
        return reject(err);
      }
      resolve(new Collection(this, name));
    });
  });
};

CollectionSpace.prototype.dropCollection = function (name, callback) {
  var command = constants.ADMIN_PROMPT + constants.DROP_CMD + ' ' + constants.COLLECTION;

  var matcher = {};
  matcher[constants.FIELD_NAME] = this.name + "." + name;

  return new Promise((resolve, reject) => {
    this.client.sendAdminCommand(command, matcher, {}, {}, {}, (err, response) => {
      if (err) {
        return reject(err);
      }
      resolve();
    });
  });
};

module.exports = CollectionSpace;
