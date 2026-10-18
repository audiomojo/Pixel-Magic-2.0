import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    projects: [
      {
        test: {
          name: 'unit',
          include: ['tests/unit/**/*.test.ts'],
          exclude: ['tests/**/*.corpus.test.ts'],
        },
      },
      { test: { name: 'workflow', include: ['tests/workflow/**/*.test.ts'] } },
      { test: { name: 'corpus', include: ['tests/**/*.corpus.test.ts'] } },
      {
        // The real compose stack against a seeded corpus subset; TIER picks the suite.
        test: {
          name: 'e2e',
          include: [`tests/e2e/${process.env['TIER'] ?? 'smoke'}.test.ts`],
          fileParallelism: false,
          testTimeout: 30 * 60_000,
          hookTimeout: 60 * 60_000,
        },
      },
      {
        // Task 13: a calm and a chaos standard run on two compose projects.
        test: {
          name: 'chaos',
          include: ['tests/chaos/**/*.test.ts'],
          fileParallelism: false,
          testTimeout: 3 * 60 * 60_000,
        },
      },
    ],
    coverage: {
      provider: 'v8',
      include: ['src/**', 'tools/corpus/lib.ts'],
      exclude: [
        'src/fscheck.ts',
        'src/worker.ts',
        'src/decodeWorker.ts',
        'tools/corpus/{buildManifest,seed,verify,synth}.ts',
      ],
      thresholds: { lines: 85, branches: 85 },
    },
  },
});
