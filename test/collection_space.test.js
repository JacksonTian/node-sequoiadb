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
var Cursor = require('../lib/cursor');

describe('CollectionSpace', function () {
  var client = common.createClient();
  var CollectionSpace = require('../lib/collection_space');

  before(function* () {
    this.timeout(8000);
    yield client.ready();
  });

  after(function* () {
    yield client.disconnect();
  });

  var spaceName = 'spaceName' + Math.floor(Math.random() * 100);
  it('createCollectionSpace should ok', function* () {
    var space = yield client.createCollectionSpace(spaceName);
    expect(space).to.be.a(CollectionSpace);
    expect(space.name).to.be(spaceName);
  });

  it('isCollectionSpaceExist should ok', function* () {
    var exist = yield client.isCollectionSpaceExist(spaceName);
    expect(exist).to.be(true);
  });

  it('getCollectionSpace should ok', function* () {
    var space = yield client.getCollectionSpace(spaceName);
    expect(space).to.be.a(CollectionSpace);
    expect(space.name).to.be(spaceName);
  });

  it('getCollectionSpace inexist should ok', function* () {
    try {
      client.getCollectionSpace('inexist');
    } catch (err) {
      expect(err).to.be.ok();
      expect(err.message).to.be('Collection space does not exist');
      return;
    }
  });

  it('dropCollectionSpace should ok', function* () {
    yield client.dropCollectionSpace(spaceName);
  });

  it('isCollectionSpaceExist should ok', function* () {
    var exist = yield client.isCollectionSpaceExist(spaceName);
    expect(exist).to.be(false);
  });

  it('getCollectionSpaces should ok', function* () {
    var cursor = yield client.getCollectionSpaces();
    expect(cursor).to.be.a(Cursor);
  });
});
