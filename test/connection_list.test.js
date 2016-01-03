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

describe('Connection List', function () {
  var client = common.createClient();
  var spaceName = 'spaceName' + Math.floor(Math.random() * 100);
  var collectionName = 'test_coll';

  before(function* () {
    this.timeout(8000);
    yield client.ready();
    var space = yield client.createCollectionSpace(spaceName);
    expect(space.name).to.be(spaceName);
    yield space.createCollection(collectionName);
  });

  after(function* () {
    yield client.dropCollectionSpace(spaceName);
    yield client.disconnect();
  });

  it('getCollectionSpaces should ok', function* () {
    var cursor = yield client.getCollectionSpaces();
    var item = yield cursor.current();
    expect(item).to.be.ok();
  });

  it('getCollectionSpaceNames should ok', function* () {
    var names = yield client.getCollectionSpaceNames();
    //expect(names.length).to.be(1);
    expect(names.length).to.be.above(0);
  });

  it('getCollections should ok', function* () {
    var cursor = yield client.getCollections();
    var item = yield cursor.current();
    expect(item.Name).to.be.ok();
  });

  it('getCollectionNames should ok', function* () {
    var names = yield client.getCollectionNames();
    expect(names.length).to.be.above(0);
  });

  it('getStorageUnits should ok', function* () {
    var names = yield client.getStorageUnits();
    expect(names.length).to.be(0);
  });
});
