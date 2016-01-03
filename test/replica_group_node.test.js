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
var constants = require('../lib/const');

describe('Replica Group Node', function () {
  var client = common.createClient();

  var groupname = 'for_node';
  var group;
  var node;
  before(function* () {
    this.timeout(8000);
    client.ready(function () {
      client.createReplicaGroup(groupname, function (err, _group) {

        group = _group;

      });
    });
  });

  after(function* () {
    this.timeout(8000);
    client.removeReplicaGroup(groupname, function (err) {

      client.disconnect(done);
    });
  });

  it('getDetail should ok', function* () {
    group.getDetail(function (err, detail) {

      expect(detail.GroupName).to.be(groupname);
      expect(detail.Group.length).to.be(0);

    });
  });

  it('getNodeCount should ok', function* () {
    group.getNodeCount(function (err, count) {

      expect(count).to.be(0);

    });
  });

  it('createNode should ok', function* () {
    var host = common.ip;
    var port = 11880;
    var dbpath = common.dbpath + 'data/11880';
    group.createNode(host, port, dbpath, {}, function (err, _node) {

      node = _node;
      expect(_node.nodename).to.be(common.ip + ':11880');

    });
  });

  it('getNodeByName should ok', function* () {
    var name = common.ip + ':11880';
    group.getNodeByName(name, function (err, node) {

      expect(node.nodename).to.be(common.ip + ':11880');

    });
  });

  it('node.getStatus should ok', function* () {
    node.getStatus(function (err, status) {

      expect(status).to.be(constants.NodeStatus.SDB_NODE_ACTIVE);

    });
  });

  it('node.start should ok', function* () {
    this.timeout(20000);
    node.start(function (err, status) {

      expect(status).to.be(true);

    });
  });

  it('node.connect should ok', function* () {
    this.timeout(8000);
    var conn = node.connect("", "");
    conn.on('error', done);
    conn.ready(function () {
      conn.disconnect(done);
    });
  });

  it('node.stop should ok', function* () {
    this.timeout(8000);
    node.stop(function (err, status) {

      expect(status).to.be(true);

    });
  });
  it('removeNode should ok', function* () {
    var host = common.ip;
    group.removeNode(host, 11880, {}, function (err) {
      expect(err).to.be.ok();

    });
  });

  it('getNodeCount should be 1', function* () {
    group.getNodeCount(function (err, count) {

      expect(count).to.be(1);

    });
  });

  it('start Group should ok', function* () {
    this.timeout(20000);
    group.start(function (err, ok) {

      expect(ok).to.be(true);

    });
  });

  it('stop Group should ok', function* () {
    this.timeout(20000);
    group.stop(function (err, ok) {

      expect(ok).to.be(true);

    });
  });

  it('getMaster should ok', function* () {
    group.getMaster(function (err, node) {

      expect(node).to.be.ok();
      expect(node.nodename).to.be(common.ip + ':11880');
      expect(node.group.name).to.be('for_node');

    });
  });

  it('getSlave should ok', function* () {
    group.getSlave(function (err, node) {

      expect(node).to.be.ok();
      expect(node.nodename).to.be(common.ip + ':11880');
      expect(node.group.name).to.be('for_node');

    });
  });
});
