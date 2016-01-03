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

var expect = require('expect.js');
var common = require('./common');

describe('Connection Backup', function () {
  var client = common.createClient();

  before(function* () {
    this.timeout(8000);
    yield client.ready();
  });

  after(function* () {
    yield client.disconnect();
  });

  it('getBackups should ok', function* () {
    var options = {
      // "Path": "/opt/sequoiadb/backup"
    };
    var cursor = yield client.getBackups(options, null, null, null);
    var item = yield cursor.current();
    expect(item).to.be(null);
  });

  it('backupOffline should ok', function* () {
    var options = {
      // "GroupName": ["rgName1", "rgName2"],
      // "Path": "/opt/sequoiadb/backup",
      // "Name": "backupName",
      // "Description": "description",
      // "EnsureInc": true,
      // "OverWrite": true
    };
    yield client.backupOffline(options);
  });

  it('getBackups should ok with items', function* () {
    var options = {};
    var cursor = yield client.getBackups(options, null, null, null);
    var item = yield cursor.current();
    expect(item).to.be.ok();
  });

  it('removeBackup should ok with items', function* () {
    var options = {};
    yield client.removeBackup(options);
  });

  it('getBackups should ok with zero', function* () {
    var options = {
      // "Path": "/opt/sequoiadb/backup"
    };
    var cursor = yield client.getBackups(options, null, null, null);
    var item = yield cursor.current();
    expect(item).to.be(null);
  });
});
