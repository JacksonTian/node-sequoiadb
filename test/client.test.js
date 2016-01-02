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

describe('Client', function () {
  var client = common.createClient();

  before(function * () {
    this.timeout(8000);
    yield client.ready();
  });

  after(function * () {
    yield client.disconnect();
  });

  it('isValid should ok', function * () {
    var valid = yield client.isValid();
    expect(valid).to.be(true);
  });

  it('setSessionAttr should ok', function * () {
    var conf = {'PreferedInstance': 'm'};
    yield client.setSessionAttr(conf);
  });
});
