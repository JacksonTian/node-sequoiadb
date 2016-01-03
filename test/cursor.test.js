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
var Cursor = require('../lib/cursor');

describe('/lib/cursor', function () {
  var client = common.createClient();
  var cursor;

  before(function* () {
    this.timeout(8000);
    client.ready(function () {
      client.getCollectionSpaces(function (err, _cursor) {

        expect(_cursor).to.be.a(Cursor);
        cursor = _cursor;

      });
    });
  });

  after(function* () {
    client.disconnect(done);
  });

  it('next should ok', function* () {
    expect(cursor.isClosed).to.be(false);
    cursor.next(function (err, item) {

      if (item) {
        expect(item.Name).to.be.ok();
      }

    });
  });

  it('current should ok', function* () {
    expect(cursor.isClosed).to.be(false);
    var item = yield cursor.current();

      if (item) {
        expect(item.Name).to.be.ok();
      }

    });
  });

  it('close should ok', function* () {
    expect(cursor.isClosed).to.be(false);
    cursor.close(function (err) {

      expect(cursor.conn).not.to.be.ok();

    });
  });

  it('current after closed should not ok', function* () {
    expect(cursor.isClosed).to.be(true);
    cursor.current(function (err) {
      expect(err).to.be.ok();
      expect(err.message).to.be('Context is closed');

    });
  });

  it('next after closed should not ok', function* () {
    expect(cursor.isClosed).to.be(true);
    cursor.next(function (err) {
      expect(err).to.be.ok();
      expect(err.message).to.be('Context is closed');

    });
  });
});
