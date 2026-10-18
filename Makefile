.PHONY: test fs-check build

build:
	npm run build

test:
	npm test

fs-check:
	docker compose run --rm worker node dist/fscheck.js
