// Chaos proof (spec §10.4): kills, restarts and pauses during a standard run never change the
// outcome. Compared with a calm run modulo `copy_ambiguous` contents (spec §12 R3, as amended).
import { expect, test } from 'vitest';
import { diffExpectations, moduloAmbiguity } from '../lib/expectations.js';
import { checkInvariant } from '../lib/invariant.js';
import { runTier, type TierRun } from '../lib/stack.js';

/** Printed even when the test passes (vitest shows `console` output of failing tests only). */
const say = (msg: string) => process.stdout.write(`${msg}\n`);

test(
  'chaos equals calm',
  async () => {
    const runs: TierRun[] = [];
    let ok = false;
    try {
      const t0 = Date.now();
      const calm = await runTier('standard', { shuffle: 1, runId: `calm-${t0.toString(36)}` });
      runs.push(calm);
      await calm.runToIdle();
      // Only its database and staged tree are compared from here on: free the VM's memory.
      await calm.stack.compose('stop', 'worker', 'temporal', 'temporal-ui', 'grafana');
      const t1 = Date.now();

      const storm = await runTier('standard', { shuffle: 1, runId: `storm-${t0.toString(36)}` });
      runs.push(storm);
      const log = await storm.runWithChaos({
        seed: 42,
        everyS: [15, 45],
        untilProgress: 0.9,
        actions: [
          ['kill', 'worker'],
          ['kill', 'worker-extra'],
          ['restart', 'temporal'],
          ['restart', 'postgres'],
          ['killAll'],
          ['pause', 'worker', 120_000], // pause ≈ Mac sleep
        ],
      });
      await storm.runToIdle();
      const t2 = Date.now();
      say(
        `chaos action log:\n${log.join('\n')}\n` +
          `calm ${((t1 - t0) / 1000).toFixed(1)} s, storm ${((t2 - t1) / 1000).toFixed(1)} s`,
      );

      expect(await checkInvariant(...storm.ctx)).toEqual([]);
      const m = await moduloAmbiguity(storm, calm);
      say(`left out ${String(m.ambiguous)} copy_ambiguous contents (either run)`);
      expect(m.partitions[0]).toEqual(m.partitions[1]);
      expect(m.tree[0]).toEqual(m.tree[1]);
      expect(await diffExpectations(...storm.exp)).toEqual([]);
      ok = true;
    } finally {
      for (const r of runs) await r.stack.down(!ok);
    }
  },
  3 * 60 * 60 * 1000,
);
