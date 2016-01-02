/**
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

var Client = require('../lib/client');
// x86
var ip = '192.168.20.63';
var dbpath = '/home/users/lz/database/';
// power pc
//var ip = "192.168.30.162";
//var dbpath = "/opt/sequoiadb/database/"

var ip = '123.56.143.17';

exports.createClient = function () {
  return new Client(11810, ip, {
    user: '',
    pass: ''
  });
};

exports.ip = ip;
exports.dbpath = dbpath;

exports.ensureCollectionSpace = function* (client, spaceName) {
  var space;
  try {
    space = yield client.createCollectionSpace(spaceName);
  } catch (ex) {
    space = yield client.getCollectionSpace(spaceName);
  }

  return space;
};
