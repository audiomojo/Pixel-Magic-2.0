.PHONY: test fs-check build up down stage retry-failed verify-vault doctor trickle corpus-manifest corpus-verify \
	test-smoke test-standard update-golden golden-smoke

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

# Reset FAILED contents and start a ContentWorkflow for each.
retry-failed:
	docker compose exec -T worker node dist/cli.js retry-failed

# Re-hash vault files (SAMPLE=1000 checks a random 1000); lists mismatches, never repairs.
verify-vault:
	docker compose exec -T worker node dist/cli.js verify-vault $(if $(SAMPLE),--sample $(SAMPLE),)

# Orphaned work/claimed files and stuck retries.
doctor:
	docker compose exec -T worker node dist/cli.js doctor

# Live end-to-end check (G6): a HEIC/JPG copy pair lands one in unique/, the other in copies/g-…/.
# The corpus is only ever cloned from (cp -c), never written.
trickle:
	mkdir -p data/dropzone && cp -c "$(CORPUS)/IMG_6941 1.HEIC" $(CORPUS)/IMG_E6941.JPG data/dropzone/ && sleep 60 && $(MAKE) stage FORCE=1 && find data/staging/current/ -type f && test -n "$$(find data/staging/current/unique -name '*6941*')" && test -n "$$(find data/staging/current/copies -path '*/g-*/*6941*')"

# Ground-truth manifest of the corpus (git-ignored; reruns reuse tests/corpus/.hash-cache.json).
corpus-manifest:
	npx tsx tools/corpus/buildManifest.ts

# G7: the corpus still matches its manifest (size+mtime; DEEP=1 re-hashes every file).
corpus-verify:
	npx tsx tools/corpus/verify.ts $(if $(DEEP),--deep,)

# G8: an e2e tier on its own compose project (pmtest_<runId>) and data-test/<runId>, with
# corpus-verify before and after (the after-check runs even when the tier fails).
define e2e
	$(MAKE) corpus-verify
	status=0; TIER=$(1) $(2) npm run test:e2e || status=$$?; $(MAKE) corpus-verify && exit $$status
endef

test-smoke:
	$(call e2e,smoke,)

test-standard:
	$(call e2e,standard,)

# Writes tests/e2e/golden/smoke_tree.txt locally. It is git-ignored: staged names come from
# personal photos (dates + original names), so every machine generates its own golden.
update-golden:
	$(call e2e,smoke,UPDATE_GOLDEN=1)

golden-smoke: update-golden
