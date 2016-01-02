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
var Lob = require('../lib/lob');
var Long = require('long');

describe('Collection Lob', function () {
  var client = common.createClient();
  var collection;

  var spaceName = 'foo7';
  var collectionName = 'bar_' + Math.floor(Math.random() * 10);

  before(function* () {
    this.timeout(8000);
    yield client.ready();

    var space = yield common.ensureCollectionSpace(client, spaceName);
    expect(space).to.be.a(CollectionSpace);
    expect(space.name).to.be(spaceName);

    collection = yield space.createCollection(collectionName);
    expect(collection).to.be.ok();
  });

  after(function* () {
    yield client.dropCollectionSpace(spaceName);
    yield client.disconnect();
  });

  it('getLobs should ok with empty list', function* () {
    var cursor = yield collection.getLobs();
    expect(cursor).to.be.ok();
    var item = yield cursor.current();
    expect(item).to.be(null);
  });

  var lob;

  it('createLob should ok', function* () {
    lob = yield collection.createLob();
  });

  it('Lob.write should ok', function* () {
    yield lob.write(new Buffer('0123456789abcdef'));
    expect(Long.fromNumber(16).equals(lob.size)).to.be.ok();
  });

  it('Lob.write(bigbuff) should ok', function* () {
    this.timeout(25000);
    var bigsize = 1024 * 1024 + 1;
    yield lob.write(new Buffer(bigsize));
    expect(Long.fromNumber(bigsize + 16).equals(lob.size)).to.be.ok();
  });

  it('Lob.close should ok', function* () {
    yield lob.close();
  });

  it('Lob.isClosed should ok', function () {
    expect(lob.isClosed()).to.be(true);
  });

  var currentLob;
  it('set read from master first', function* () {
    var option = {'PreferedInstance': 'M'};
    yield client.setSessionAttr(option);
  });

  it('Lob.open should ok', function* () {
    currentLob = yield collection.openLob(lob.id);
    expect(currentLob.getID()).to.be(currentLob.id);
    expect(currentLob.getSize()).to.be(currentLob.size);
    expect(currentLob.getCreateTime()).to.be(currentLob.createTime);
  });

  it('Lob.read should ok', function* () {
    var buff = yield currentLob.read(16);
    expect(Long.fromNumber(16).equals(currentLob.readOffset)).to.be.ok();
    expect(buff).to.eql(new Buffer('0123456789abcdef'));
  });

  it('Lob.seek should ok', function () {
    currentLob.seek(1, Lob.SDB_LOB_SEEK_SET);
    expect(Long.isLong(currentLob.readOffset)).to.be.ok();
    expect(Long.fromNumber(1).equals(currentLob.readOffset)).to.be.ok();
    currentLob.seek(1, Lob.SDB_LOB_SEEK_CUR);
    expect(Long.isLong(currentLob.readOffset)).to.be.ok();
    expect(Long.fromNumber(2).equals(currentLob.readOffset)).to.be.ok();
    currentLob.seek(1, Lob.SDB_LOB_SEEK_END);
    expect(Long.isLong(currentLob.readOffset)).to.be.ok();
    var totalSize = Long.fromNumber(1024 * 1024 + 17 - 1);
    expect(totalSize.equals(currentLob.readOffset)).to.be.ok();
  });

  it('Lob.close should ok', function* () {
    yield currentLob.close();
  });

  it('getLobs should ok with item', function* () {
    var option = {'PreferedInstance':'M'};
    yield client.setSessionAttr(option);
    var cursor = yield collection.getLobs();
    expect(cursor).to.be.ok();
    var item = yield cursor.current();
    expect(item).to.be.ok();
  });

  it('removeLob should ok', function* () {
    yield collection.removeLob(lob.id);
    var cursor = yield collection.getLobs();
    expect(cursor).to.be.ok();
    var item = yield cursor.current();
    expect(item).to.be(null);
  });
});
