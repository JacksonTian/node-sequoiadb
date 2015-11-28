TESTS = test/*.js
REPORTER = spec
TIMEOUT = 4000
ISTANBUL = ./node_modules/.bin/istanbul
MOCHA = ./node_modules/mocha/bin/_mocha
COVERALLS = ./node_modules/coveralls/bin/coveralls.js

test:
	@NODE_ENV=test $(MOCHA) -R $(REPORTER) -t $(TIMEOUT) \
		--require co-mocha \
		$(MOCHA_OPTS) \
		$(TESTS)

test-cov:
	@$(ISTANBUL) cover --report html \
		$(MOCHA) -- -t $(TIMEOUT) \
		--require co-mocha \
		-R spec $(TESTS)

test-coveralls:
	@$(ISTANBUL) cover --report lcovonly $(MOCHA) -- -t $(TIMEOUT) -R spec $(TESTS)
	@echo TRAVIS_JOB_ID $(TRAVIS_JOB_ID)
	@cat ./coverage/lcov.info | $(COVERALLS) && rm -rf ./coverage

debug:
	@NODE_ENV=test \
		node-debug \
		$(MOCHA) \
		--reporter $(REPORTER) \
		--timeout $(TIMEOUT) \
		$(MOCHA_OPTS) \
		$(TESTS)

test-all: test test-coveralls

.PHONY: test
