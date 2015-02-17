ALL_TESTS = $(shell find test -name 'test.*.js')
KAFKA_TEST_HOST?=127.0.0.1

kafka-instance:
	# Always start tests with a clean slate by removing old instances
	fig rm --force kafka zookeeper
	KAFKA_ADVERTISED_HOST_NAME=$(KAFKA_TEST_HOST) fig up

test:
	@NODE_ENV=test ./node_modules/.bin/mocha $(ALL_TESTS)

coverage:
	@jscoverage lib lib-cov
	@KAFKA_COV=1 mocha -r should -R html-cov > ~/work/coverage.html
	rm -rf lib-cov
docs:
	rm -rf docs
	jsdoc -d docs lib/ DOCS.md

.PHONY: test docs
