ALL_TESTS = $(shell find test -name 'test.*.js')
test:
	@NODE_ENV=test ./node_modules/.bin/mocha $(ALL_TESTS)

.PHONY: test
