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

describe('Domain Actions', function () {
  var client = common.createClient();
  var domainName = 'domain_name';
  var domain;

  before(function* () {
    this.timeout(8000);
    client.ready(function () {
      client.createDomain(domainName, {Groups:['data']}, function (err, _domain) {

        expect(_domain).to.be.ok();
        expect(_domain.name).to.be(domainName);
        domain = _domain;

      });
    });
  });

  after(function* () {
    client.dropDomain(domainName, function (err, domain) {

      client.disconnect(done);
    });
  });

  it('getCollectionSpaces should ok', function* () {
    domain.getCollectionSpaces(function (err, cursor) {

      expect(cursor).to.be.ok();
      var item = yield cursor.current();

        expect(item).to.be(null);

      });
    });
  });

  describe('CollectionSpace with domain', function () {
    var _space;
    it('alter to data group should ok', function* () {
      var options = {
        "Groups": [ "data" ],
        "AutoSplit": true
      };
      domain.alter(options, function (err) {


      });
    });

    it('alter should fail without options', function () {
      expect(function () {
        domain.alter();
      }).to.throwError(/Invalid Argument/);
    });

    it('createCollectionSpace', function* () {
      var options = {'Domain': domainName};
      client.createCollectionSpace('space', options, function (err, space) {

        expect(space).to.be.ok();
        _space = space;

      });
    });

    it('getCollectionSpaces should ok', function* () {
      domain.getCollectionSpaces(function (err, cursor) {

        expect(cursor).to.be.ok();
        var item = yield cursor.current();

          expect(item.Name).to.be('space');

        });
      });
    });

    it('createCollection should ok', function* () {
      var opts = {
        "ShardingKey": {a: 1},
        "ShardingType": "hash",
        "AutoSplit": true
      };
      _space.createCollection('cl', opts, function (err, cl) {

        expect(cl).to.be.ok();
        //expect(cl.Name).to.be('space.cl');

      });
    });

    it('getCollections should ok', function* () {
      domain.getCollections(function (err, cursor) {

        expect(cursor).to.be.ok();
        var item = yield cursor.current();

          expect(item.Name).to.be('space.cl');

        });
      });
    });

    it('dropCollectionSpace should ok', function* () {
      client.dropCollectionSpace('space', function (err) {


      });
    });
  });

  it('getCollections should ok', function* () {
    domain.getCollections(function (err, cursor) {

      expect(cursor).to.be.ok();
      var item = yield cursor.current();

        expect(item).to.be(null);

      });
    });
  });

  it('alter should ok', function* () {
    var options = {
      "Groups": [ "group1", "group2", "group3" ],
      "AutoSplit": true
    };
    domain.alter(options, function (err) {


    });
  });

  it('alter reset should ok', function* (){
   var options = {
      "Groups": [],
      "AutoSplit": false
    };
    domain.alter(options, function (err) {


    });
  });
});
