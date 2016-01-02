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
var Collection = require('../lib/collection');
var CollectionSpace = require('../lib/collection_space');

describe('Collection index', function () {
  var client = common.createClient();
  var collection;

  var spaceName = 'foo6';
  var collectionName = 'bar5';

  before(function* () {
    this.timeout(8000);

    yield client.ready();

    var space = yield common.ensureCollectionSpace(client, spaceName);
    expect(space).to.be.a(CollectionSpace);
    expect(space.name).to.be(spaceName);

    collection = yield space.createCollection(collectionName);
    expect(collection).to.be.a(Collection);
  });

  after(function* () {
    yield client.dropCollectionSpace(spaceName);
    yield client.disconnect();
  });

  it('set read from master first', function* () {
    var option = {'PreferedInstance':'M'};
    yield client.setSessionAttr(option);
  });

  it('createIndex should ok', function* () {
    var key = {
      'Last Name': 1,
      'First Name': 1
    };
    yield collection.createIndex('index name', key, false, false);
    var cursor = yield collection.getIndex('index name');
    var index = yield cursor.current();
    expect(index.IndexDef.name).to.be('index name');
    expect(index.IndexDef.key).to.eql({ 'Last Name': 1, 'First Name': 1 });
  });

  it('getIndex without name should ok', function* () {
    var cursor = yield collection.getIndex();
    var idIndex = yield cursor.next();
    expect(idIndex.IndexDef.name).to.be('$id');
    var index = yield cursor.next();
    expect(index.IndexDef.name).to.be('index name');
  });

  it('getIndexes should ok', function* () {
    var cursor = yield collection.getIndexes();
    var index = yield cursor.next();
    expect(index.IndexDef.name).to.be('$id');
    index = yield cursor.next();
    expect(index.IndexDef.name).to.be('index name');
  });

  it('dropIndex should ok', function* () {
    yield collection.dropIndex('index name');
    var cursor = yield collection.getIndexes();
    var index = yield cursor.next();
    expect(index.IndexDef.name).to.be('$id');
    index = yield cursor.next();
    expect(index).to.be(null);
  });
});
