/** `tsx tools/corpus/verify.ts [--deep] [--corpus DIR] [--manifest FILE]`: exits 1 on any drift. */
import { parseArgs } from 'node:util';
import { PATHS, loadManifest, verify } from './lib.js';

const { values } = parseArgs({
  options: {
    deep: { type: 'boolean', default: false },
    corpus: { type: 'string', default: PATHS.corpus },
    manifest: { type: 'string', default: PATHS.manifest },
  },
});

const m = await loadManifest(values.manifest).catch((err: unknown) => {
  console.error(`cannot read ${values.manifest} (run \`make corpus-manifest\`): ${String(err)}`);
  process.exit(2);
});
const t0 = performance.now();
const drift = await verify(values.corpus, m, values.deep);
for (const d of drift) console.error(d);
const secs = ((performance.now() - t0) / 1000).toFixed(1);
console.log(
  `corpus-verify (${values.deep ? 'deep' : 'size+mtime'}): ${m.records.length} files, ${drift.length} drifted, ${secs} s`,
);
process.exit(drift.length === 0 ? 0 : 1);
