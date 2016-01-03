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
var Node = require('../lib/node');

describe('Collection split', function () {
  var client = common.createClient();
  var collection;
  var space;

  var srcGroup;
  var dstGroup;

  var spaceName = 'foo5';
  var collectionName = 'bar5';

  before(function* () {
    this.timeout(8000);
    yield client.ready();
  });

  after(function* () {
    yield client.dropCollectionSpace(spaceName);
    yield client.disconnect();
  });

  it('create collection space should ok', function* (){
    space = yield common.ensureCollectionSpace(client, spaceName);
    expect(space).not.to.be(null);
    expect(space.name).to.be(spaceName);
  });

  it('create source group should ok', function* () {
    srcGroup = yield client.createReplicaGroup('source');
    expect(srcGroup).not.to.be(null);
  });

  it('source group create node should ok', function* () {
    this.timeout(8000);
    var host = common.ip;
    var port = 22000;
    var dbpath = common.dbpath + 'data/22000';
    var node = yield srcGroup.createNode(host, port, dbpath, {});
    expect(node).to.be.a(Node);
  });

  it('activate source group should ok', function* () {
    this.timeout(20000);
    yield client.activateReplicaGroup('source');
  });

  it('create collection on source group should ok', function* (){
    var options = {
      ShardingKey: {'age': 1},
      ShardingType: 'hash',
      Partition: 4096,
      Group: 'source'
    };
    collection = yield space.createCollection(collectionName, options);
    expect(collection).to.be.a(Collection);
  });

  it('create dest group should ok', function* (){
    dstGroup = yield client.createReplicaGroup('dest');
    expect(dstGroup).not.to.be(null);
  });

  it('create node for dest group should ok', function* () {
    this.timeout(8000);
    var host = common.ip;
    var port = 22010;
    var dbpath = common.dbpath + 'data/22010';
    var node = yield dstGroup.createNode(host, port, dbpath, {});
    expect(node).to.be.a(Node);
  });

  it('wait for 10s', function* () {
    this.timeout(11000);
    yield common.sleep(10000);
  });

  it('activate dest group should ok', function* () {
    this.timeout(20000);
    yield client.activateReplicaGroup('dest');
  });

  it('split should ok', function* () {
    this.timeout(8000);
    var splitCondition = {age: 30};
    var splitEndCondition = {age: 60};
    yield collection.split('source', 'dest', splitCondition, splitEndCondition);
  });

  it('wait for 10s', function* () {
    this.timeout(11000);
    yield common.sleep(10000);
  });

  it('splitByPercent should ok', function* () {
    this.timeout(8000);
    yield collection.splitByPercent('source', 'dest', 50);
  });

  it('wait for 10s', function* () {
    this.timeout(11000);
    yield common.sleep(10000);
  });

  it('splitAsync should ok', function* () {
    this.timeout(8000);
    var splitCondition = {age: 10};
    var splitEndCondition = {age: 30};
    yield collection.splitAsync('source', 'dest', splitCondition, splitEndCondition);
  });

  it('splitByPercentAsync should ok', function* () {
    this.timeout(8000);
    yield collection.splitByPercentAsync('source', 'dest', 50);
  });

  it('drop collection space should ok', function* () {
    yield client.dropCollectionSpace(spaceName);
  });

  it('remove source group should ok', function* () {
    this.timeout(10000);
    yield client.removeReplicaGroup('source');
  });

  it('remove dest group should ok', function* () {
    this.timeout(10000);
    yield client.removeReplicaGroup('dest');
  });
});
