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
var Collection = require('../lib/collection');
var Query = require('../lib/query');
var Long = require('long');

describe('Collection DML', function () {
  var client = common.createClient();

  var space;
  var spaceName = 'foo6';

  var collection;
  var collectionName = 'bar6';

  before(function* () {
    this.timeout(8000);
    yield client.ready();

    space = yield common.ensureCollectionSpace(client, spaceName);
    expect(space).to.be.a(CollectionSpace);
    expect(space.name).to.be(spaceName);

    collection = yield space.createCollection(collectionName);
    expect(collection).to.be.a(Collection);
  });

  after(function* () {
    yield client.dropCollectionSpace(spaceName);
    yield client.disconnect();
  });

  it('query should ok', function* () {
    var cursor = yield collection.query();
    expect(cursor).to.be.ok();
    var item = yield cursor.current();
    expect(item).to.be(null);
  });

  it('insert should ok', function* () {
    yield collection.insert({'name':'sequoiadb'});
    var cursor = yield collection.query();
    expect(cursor).to.be.ok();
    var item = yield cursor.current();
    expect(item.name).to.be('sequoiadb');
  });

  it('update should ok', function* () {
    var query = new Query();
    query.Matcher = {name: 'sequoiadb'};
    query.Modifier = {'$set': {age: 25}};
    yield collection.update(query);

    var cursor = yield collection.query();
    expect(cursor).to.be.ok();
    var item = yield cursor.current();
    expect(item.name).to.be('sequoiadb');
    expect(item.age).to.be(25);
  });

  it('update(matcher, modifier, hint) should ok', function* () {
    yield collection.update({name: 'sequoiadb'}, {'$set': {age: 26}}, {});
  });

  it('delete should ok', function* () {
    yield collection.delete({name: 'sequoiadb'});
  });

  it('delete all should ok', function* () {
    yield collection.delete();
    var cursor = yield collection.query({}, {}, {}, {});
    expect(cursor).to.be.ok();
    var item = yield cursor.current();
    expect(item).to.be(null);
  });

  it('query should ok with Query', function* () {
    var query = new Query();
    var cursor = yield collection.query(query);
    var item = yield cursor.current();
    expect(item).to.be(null);
  });

  it('upsert(matcher, modifier, hint) should ok', function* () {
    yield collection.upsert({name: 'sequoiadb'}, {'$set': {age: 26}}, {});
  });

  it('bulkInsert should ok', function* () {
    var insertors = [
      {name: 'hi'},
      {name: 'jack'}
    ];
    yield collection.bulkInsert(insertors, 0);
  });

  it('aggregate should ok', function* () {
    var insertors = [
      {'$match': {status: 'A'}},
      {'$group': {'_id': '$cust_id', total: {'$sum': '$amount'}}}
    ];
    yield collection.aggregate(insertors);
  });

  it('getQueryMeta should ok', function* () {
    var matcher = {};
    var orderBy = {'Indexblocks': 1};
    var hint = {'': 'ageIndex'};
    var cursor = yield collection.getQueryMeta(matcher, orderBy, hint, Long.ZERO, Long.NEG_ONE);
    expect(cursor).to.be.ok();
    var item = yield cursor.current();
    expect(item).to.be.ok();
  });

  it('count should ok', function* () {
    var count = yield collection.count();
    expect(count).to.be(3);
  });

  it('explain', function* () {
    var indexName = 'QueryExpalinIndex';

    yield collection.createIndex(indexName, {'age': 1}, false, false);

    // matcher, selector, orderBy, hint,
    // skipRows, returnRows, flag, options, callback
    var matcher = {
      'age': {
        '$gt': 50
      }
    };
    var selector = {'age': '' };
    var orderBy = {'age': -1 };
    var hint = {'': indexName };
    var options = {'Run': true };

    var cursor = yield collection.explain(matcher, selector, orderBy, hint, Long.fromNumber(47), Long.fromNumber(3), 0, options);
    expect(cursor).to.be.ok();

    var item = yield cursor.next();
    expect(item.IndexRead).to.be(1);
    expect(item.DataRead).to.be(0);
    expect(item.IndexName).to.be('QueryExpalinIndex');
  });

  describe('xxtach collection', function () {
    var mcl, scl;
    before('create sub collection', function* () {
      var options = {
        'IsMainCL': true,
        'ShardingKey': {'id': 1},
        'ReplSize': 0
      };
      mcl = yield space.createCollection('mcl', options);
      expect(mcl).to.be.ok();
    });

    it('createSubCollection', function* () {
      var options = {
        'ReplSize': 0
      };
      scl = yield space.createCollection('scl', options);
      expect(scl).to.be.ok();
    });

    it('attachCollection should ok', function* () {
      var options = {
        'LowBound': {id: 0},
        'UpBound': {id: 10}
      };
      yield mcl.attachCollection(scl.collectionFullName, options);
    });

    it('detachCollection should ok', function* () {
      yield mcl.detachCollection(scl.collectionFullName);
    });
  });

  it('alter should ok', function* () {
    var options = {
      'ReplSize': 0,
      'ShardingKey': {'a': 1},
      'ShardingType': 'hash',
      'Partition': 4096
    };
    yield collection.alter(options);
  });
});
