ALL_TESTS = $(shell find test -name 'test.*.js')
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
