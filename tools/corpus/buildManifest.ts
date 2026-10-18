/** `tsx tools/corpus/buildManifest.ts [--corpus DIR] [--out FILE] [--cache FILE]` */
import { mkdir, writeFile } from 'node:fs/promises';
import { dirname } from 'node:path';
import { parseArgs } from 'node:util';
import { PATHS, buildManifest, components } from './lib.js';

const { values } = parseArgs({
  options: {
    corpus: { type: 'string', default: PATHS.corpus },
    out: { type: 'string', default: PATHS.manifest },
    cache: { type: 'string', default: PATHS.cache },
  },
});

const t0 = performance.now();
const m = await buildManifest(values.corpus, { cachePath: values.cache });
await mkdir(dirname(values.out), { recursive: true });
await writeFile(values.out, JSON.stringify(m, null, 1));

const kinds: Record<string, number> = {};
for (const r of m.records) kinds[r.kind] = (kinds[r.kind] ?? 0) + 1;
const r = m.relations;
console.log(
  `files: ${m.records.length} (${Object.entries(kinds)
    .map(([k, n]) => `${k} ${n}`)
    .join(', ')})`,
);
console.log(`extMismatch: ${m.records.filter((x) => x.extMismatch).length}`);
console.log(`exactGroups: ${r.exactGroups.length}`);
console.log(
  `heicJpgTwin: ${r.heicJpgTwin.length}  editOf: ${r.editOf.length}  spaceSuffixCopy: ${r.spaceSuffixCopy.length}`,
);
console.log(`burst: ${r.burst.length} pairs in ${components(r.burst).length} sets`);
console.log(`livePhoto: ${r.livePhoto.length}`);
console.log(`wrote ${values.out} in ${((performance.now() - t0) / 1000).toFixed(1)} s`);
