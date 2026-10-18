import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    passWithNoTests: true,
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
