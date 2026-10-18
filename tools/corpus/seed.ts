/**
 * `tsx tools/corpus/seed.ts --subset smoke|standard|full --dest DIR [--layout flat|nested]
 *  [--shuffle N] [--extras junk,corrupt] [--synthetic] [--seed N] [--prefix P]`
 * Clones (cp -c) the subset into DIR and writes DIR/../seed-manifest.json.
 */
import { writeFile } from 'node:fs/promises';
import { dirname, join, resolve } from 'node:path';
import { parseArgs } from 'node:util';
import { PATHS, loadManifest, seed, selectSubsets, type SeedOptions } from './lib.js';

const { values } = parseArgs({
  options: {
    subset: { type: 'string', default: 'smoke' },
    dest: { type: 'string' },
    layout: { type: 'string', default: 'flat' },
    shuffle: { type: 'string' },
    extras: { type: 'string', default: '' },
    synthetic: { type: 'boolean', default: false },
    seed: { type: 'string', default: '1' },
    prefix: { type: 'string', default: '' },
    corpus: { type: 'string', default: PATHS.corpus },
    manifest: { type: 'string', default: PATHS.manifest },
  },
});

const fail = (msg: string): never => {
  console.error(msg);
  process.exit(2);
};
if (!values.dest) fail('--dest DIR is required');
if (!['smoke', 'standard', 'full'].includes(values.subset)) fail(`bad --subset ${values.subset}`);
if (values.layout !== 'flat' && values.layout !== 'nested') fail(`bad --layout ${values.layout}`);
const extras = values.extras.split(',').filter((e) => e !== '');
if (extras.some((e) => e !== 'junk' && e !== 'corrupt')) fail(`bad --extras ${values.extras}`);

const m = await loadManifest(values.manifest);
const names =
  values.subset === 'full'
    ? m.records.map((r) => r.name)
    : selectSubsets(m, Number(values.seed))[values.subset as 'smoke' | 'standard'];
const dest = resolve(values.dest!);
const opts: SeedOptions = {
  corpusDir: values.corpus,
  names,
  dest,
  layout: values.layout as 'flat' | 'nested',
  extras: extras as Array<'junk' | 'corrupt'>,
  synthetic: values.synthetic,
  prefix: values.prefix,
};
if (values.shuffle !== undefined) opts.shuffle = Number(values.shuffle);
const sm = await seed(opts);
const out = join(dirname(dest), 'seed-manifest.json');
await writeFile(out, JSON.stringify(sm, null, 1));
console.log(
  `seeded ${sm.files.length} files into ${dest} (${sm.fallbackCopies} byte copies); wrote ${out}`,
);
