/**
 *      Copyright (C) 2015 SequoiaDB Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the 'License');
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an 'AS IS' BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

'use strict';

var expect = require('expect.js');
var common = require('./common');
var Collection = require('../lib/collection');
var CollectionSpace = require('../lib/collection_space');

describe('Connection js', function () {
  var client = common.createClient();
  var collection;

  var spaceName = 'foo6';
  var collectionName = 'barxyz';

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

  it('execUpdate should ok', function* () {
    // insert English
    var sql = 'INSERT INTO ' + spaceName + '.' + collectionName +
                ' ( c, d, e, f ) values( 6.1, \'8.1\', \'aaa\', \'bbb\')';
    yield client.execUpdate(sql);
  });

  it('exec should ok', function* () {
    var sql = 'SELECT * FROM ' + spaceName + '.' + collectionName;
    var cursor = yield client.exec(sql);
    var item = yield cursor.current();
    expect(item).to.be.ok();
  });

  it('createProcedure should ok', function* () {
    var code = function sum(x,y){return x+y;};
    yield client.createProcedure(code);
  });

  it('evalJS should ok', function* () {
    var result = yield client.evalJS('sum(1,2)');
    expect(result).to.be.ok();
    var cursor = result.cursor;
    var item = yield cursor.current();
    expect(item).to.be.ok();
  });

  it('getProcedures should ok', function* () {
    var cursor = yield client.getProcedures({'name':'sum'});
    var item = yield cursor.current();
    expect(item).to.be.ok();
  });

  it('removeProcedure should ok', function* () {
    yield client.removeProcedure('sum');
  });

  it('getProcedures should ok', function* () {
    var cursor = yield client.getProcedures({'name':'sum'});
    var item = yield cursor.current();
    expect(item).to.be(null);
  });
});
