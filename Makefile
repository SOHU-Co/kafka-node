ifeq ($(OS),Windows_NT)
	ALL_TESTS = $(shell dir test.*.js /s/b)
else
	ALL_TESTS = $(shell find test -name 'test.*.js')
endif

test:
ifeq ($(OS),Windows_NT)
	@.\node_modules\.bin\mocha $(ALL_TESTS)
else
	@NODE_ENV=test ./node_modules/.bin/mocha $(ALL_TESTS)
endif

coverage:
	@jscoverage lib lib-cov
	@KAFKA_COV=1 mocha -r should -R html-cov > ~/work/coverage.html
	rm -rf lib-cov
.PHONY: test
