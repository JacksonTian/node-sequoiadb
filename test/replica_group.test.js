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

describe('Replica Group', function () {
  var client = common.createClient();

  before(function* () {
    this.timeout(8000);
    client.ready(done);
  });

  after(function* () {
    client.disconnect(done);
  });

  it('getReplicaGroups should ok', function* () {
    client.getReplicaGroups(function (err, cursor) {

      expect(cursor).to.be.ok();
      var item = yield cursor.current();

        expect(item.Group.length).to.above(0);
        expect(item.GroupID).to.be(1);
        expect(item.GroupName).to.be('SYSCatalogGroup');

      });
    });
  });

  it('getReplicaGroupById should ok', function* () {
    client.getReplicaGroupById(1, function (err, group) {

      expect(group).to.be.ok();
      expect(group.isCatalog).to.be(true);
      expect(group.groupId).to.be(1);
      expect(group.name).to.be('SYSCatalogGroup');

    });
  });

  it('getReplicaGroupByName should ok', function* () {
    client.getReplicaGroupByName('SYSCatalogGroup', function (err, group) {

      expect(group).to.be.ok();
      expect(group.isCatalog).to.be(true);
      expect(group.groupId).to.be(1);
      expect(group.name).to.be('SYSCatalogGroup');

    });
  });

  it('createReplicaGroup should ok', function* () {
    client.createReplicaGroup('group5', function (err, group) {

      expect(group).to.be.ok();
      expect(group.isCatalog).to.be(false);
      expect(group.name).to.be('group5');

    });
  });

  it('createReplicaCataGroup should ok', function* () {
    this.timeout(8000);
    var host = common.ip;
    var port = 11810;
    var dbpath = common.dbpath + 'data/11890';
    client.createReplicaCataGroup(host, port, dbpath, null, function (err) {
      expect(err).to.be.ok();
      expect(err.message).to.be("Unable to create new catalog when there's already one exists");

    });
  });

  it('activateReplicaGroup should ok', function* () {
    client.activateReplicaGroup('group5', function (err, group) {

      // expect(group).to.be.ok();

    });
  });

  it('removeReplicaGroup should ok', function* () {
    client.removeReplicaGroup('group5', function (err, group) {


    });
  });
});
