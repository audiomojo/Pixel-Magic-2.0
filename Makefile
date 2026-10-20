.PHONY: test fs-check build up down stage retry-failed verify-vault doctor copies sweep-copies trickle \
	corpus-manifest corpus-verify \
	harvest-install harvest-uninstall harvester harvest-status \
	test-smoke test-standard update-golden golden-smoke chaos

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

# Start a ContentWorkflow for each FAILED content (the workflow reopens it); safe to re-run.
retry-failed:
	docker compose exec -T worker node dist/cli.js retry-failed

# Re-hash vault files (SAMPLE=1000 checks a random 1000); lists mismatches, never repairs.
verify-vault:
	docker compose exec -T worker node dist/cli.js verify-vault $(if $(SAMPLE),--sample $(SAMPLE),)

# Orphaned work/claimed files, stuck retries and copy mismatches (duplicates kept, see §6.5).
doctor:
	docker compose exec -T worker node dist/cli.js doctor

# Every duplicate drop (TSV: rel_path, mtime, dropped_at, vault sha256, removed|present|mismatch).
copies:
	docker compose exec -T worker node dist/cli.js copies

# Run one bounded duplicate sweep now (pm-copies runs it every 15 min); prints its counts.
sweep-copies:
	docker compose exec -T worker node dist/cli.js sweep-copies

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

# G8: an e2e command on its own compose project (pmtest_<runId>) and data-test/<runId>, with
# corpus-verify before and after (the after-check runs even when the command fails).
define e2e
	$(MAKE) corpus-verify
	status=0; $(1) || status=$$?; $(MAKE) corpus-verify && exit $$status
endef

test-smoke:
	$(call e2e,TIER=smoke npm run test:e2e)

test-standard:
	$(call e2e,TIER=standard npm run test:e2e)

# Task 13: a calm and a chaos standard run (kills, restarts, a pause) must end the same.
chaos:
	$(call e2e,npm run test:chaos)

# Writes tests/e2e/golden/smoke_tree.txt locally. It is git-ignored: staged names come from
# personal photos (dates + original names), so every machine generates its own golden.
update-golden:
	$(call e2e,TIER=smoke UPDATE_GOLDEN=1 npm run test:e2e)

golden-smoke: update-golden

# Harvest (spec docs/superpowers/specs/2026-10-19-harvest-design.md): the native harvester on the Mac.
# Install the launchd agent (starts at login; run again after a Node upgrade or a repo move).
harvest-install: build
	node dist/harvest/cli.js install

harvest-uninstall:
	node dist/harvest/cli.js uninstall

# Run the harvester in the foreground (stop the launchd agent first: both would use the port).
harvester: build
	node --env-file-if-exists=.env dist/harvest/host.js

# Active run, item counts, gate and the host heartbeat.
harvest-status:
	node --env-file-if-exists=.env dist/harvest/cli.js status
