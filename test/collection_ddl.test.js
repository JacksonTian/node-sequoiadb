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
var CollectionSpace = require('../lib/collection_space');

describe('Collection DDL', function () {
  var Collection = require('../lib/collection');
  var client = common.createClient();
  var collectionSpace;
  var spaceName = 'spacename' + Math.floor(Math.random() * 100);
  before(function* () {
    this.timeout(8000);
    yield client.ready();
    var space = yield client.createCollectionSpace(spaceName);
    expect(space).to.be.a(CollectionSpace);
    expect(space.name).to.be(spaceName);
    collectionSpace = space;
  });

  after(function* () {
    yield client.dropCollectionSpace(spaceName);
    collectionSpace = null;
    yield client.disconnect();
  });

  var collectionName = "collection";

  it('isCollectionExist should ok', function* () {
    var exist = yield collectionSpace.isCollectionExist(collectionName);
    expect(exist).to.be(false);
  });

  it('getCollection for inexist should ok', function* () {
    var collection = yield collectionSpace.getCollection('inexist');
    expect(collection).to.be(null);
  });

  it('createCollection should ok', function* () {
    var collection = yield collectionSpace.createCollection(collectionName);
    expect(collection).to.be.a(Collection);
    var exist = yield collectionSpace.isCollectionExist(collectionName);
    expect(exist).to.be(true);
  });

  it('getCollection should ok', function* () {
    var collection = yield collectionSpace.getCollection(collectionName);
    expect(collection).to.be.a(Collection);
  });

  it('dropCollection should ok', function* () {
    yield collectionSpace.dropCollection(collectionName);
    var exist = yield collectionSpace.isCollectionExist(collectionName);
    expect(exist).to.be(false);
  });
});
