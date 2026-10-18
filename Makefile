.PHONY: test fs-check build up down stage trickle

CORPUS := test-automation/sample-images

build:
	npm run build

test:
	npm test

up:
	docker compose up -d --build --wait

down:
	docker compose down

fs-check:
	docker compose run --rm worker node dist/fscheck.js

# Rebuild staging now (FORCE=1 rebuilds even when nothing changed).
stage:
	docker compose exec -T worker node dist/cli.js stage $(if $(FORCE),--force,)

# Live end-to-end check (G6): a HEIC/JPG copy pair lands one in unique/, the other in copies/g-…/.
# The corpus is only ever cloned from (cp -c), never written.
trickle:
	mkdir -p data/dropzone && cp -c "$(CORPUS)/IMG_6941 1.HEIC" $(CORPUS)/IMG_E6941.JPG data/dropzone/ && sleep 60 && $(MAKE) stage FORCE=1 && find data/staging/current/ -type f && test -n "$$(find data/staging/current/unique -name '*6941*')" && test -n "$$(find data/staging/current/copies -path '*/g-*/*6941*')"
