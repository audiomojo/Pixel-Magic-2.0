/**
 * `tsx tools/corpus/synth.ts --dest DIR [--subset smoke|standard] [--seed N] [--prefix P]`
 * Writes synthetic variants of real seeds into DIR and appends their ground truth to
 * DIR/../seed-manifest.json (created if absent).
 */
import { readFile, writeFile } from 'node:fs/promises';
import { dirname, join, resolve } from 'node:path';
import { parseArgs } from 'node:util';
import { PATHS, loadManifest, selectSubsets, synthesize, type SeedManifest } from './lib.js';

const { values } = parseArgs({
  options: {
    dest: { type: 'string' },
    subset: { type: 'string', default: 'smoke' },
    seed: { type: 'string', default: '1' },
    prefix: { type: 'string', default: '' },
    corpus: { type: 'string', default: PATHS.corpus },
    manifest: { type: 'string', default: PATHS.manifest },
  },
});
if (!values.dest || (values.subset !== 'smoke' && values.subset !== 'standard')) {
  console.error('usage: synth.ts --dest DIR [--subset smoke|standard] [--seed N] [--prefix P]');
  process.exit(2);
}

const dest = resolve(values.dest);
const names = selectSubsets(await loadManifest(values.manifest), Number(values.seed))[
  values.subset
];
const files = await synthesize(values.corpus, names, dest, values.prefix);
const out = join(dirname(dest), 'seed-manifest.json');
const sm = await readFile(out, 'utf8')
  .then((s) => JSON.parse(s) as SeedManifest)
  .catch((): SeedManifest => ({ dest, files: [] }));
sm.files.push(...files);
await writeFile(out, JSON.stringify(sm, null, 1));
console.log(`wrote ${files.length} synthetic variants into ${dest}; updated ${out}`);
