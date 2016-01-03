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

describe('Domain', function () {
  var client = common.createClient();

  before(function* () {
    this.timeout(8000);
    client.ready(done);
  });

  after(function* () {
    client.disconnect(done);
  });

  it('getDomains should ok', function* () {
    client.getDomains(null, null, null, null, function (err, cursor) {

      expect(cursor).to.be.ok();

    });
  });

  it('isDomainExist should ok', function* () {
    client.isDomainExist('inexist', function (err, exist) {

      expect(exist).to.be(false);

    });
  });

  it('getDomain should ok', function* () {
    client.getDomain('inexist', function (err, domain) {

      expect(domain).to.be(null);

    });
  });

  it('createDomain should ok', function* () {
    client.createDomain('mydomain', function (err, domain) {

      expect(domain).to.be.ok();
      expect(domain.name).to.be('mydomain');

    });
  });

  it('getDomain should ok with exist', function* () {
    client.getDomain('mydomain', function (err, domain) {

      expect(domain.name).to.be('mydomain');

    });
  });

  it('dropDomain should ok', function* () {
    client.dropDomain('mydomain', function (err, domain) {


    });
  });
});
