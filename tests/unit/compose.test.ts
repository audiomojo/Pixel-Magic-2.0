// docker-compose.yml as text: the settings `make up` is supposed to honour must reach the
// workers (I4), memory limits must be settable (I8) and logs must rotate (M20).
import { readFileSync } from 'node:fs';
import { expect, test } from 'vitest';
import { TUNABLE_ENV } from '../../src/config.js';

const compose = readFileSync('docker-compose.yml', 'utf8');
const block = (name: string): string => {
  const start = compose.indexOf(`\n${name}:`);
  if (start < 0) throw new Error(`no top-level ${name}`);
  const end = compose.slice(start + 1).search(/\n\S/);
  return compose.slice(start, end < 0 ? undefined : start + 1 + end);
};

/** One service's block inside `services:`. */
const service = (name: string): string => {
  const services = block('services');
  const at = services.indexOf(`\n  ${name}:\n`);
  if (at < 0) throw new Error(`no service ${name}`);
  const next = services.slice(at + 1).search(/\n {2}\S/);
  return services.slice(at, next < 0 ? undefined : at + 1 + next);
};

test.each(TUNABLE_ENV)('x-worker-env forwards %s as ${NAME:-} (unset → default)', (name) => {
  expect(block('x-worker-env')).toContain(`\n  ${name}: \${${name}:-}\n`);
});

test('every tunable in config is one of the forwarded ones', () =>
  expect(TUNABLE_ENV).toEqual(
    expect.arrayContaining(['CPU_CONCURRENCY', 'POISON_ATTEMPTS', 'COPY_PHASH']),
  ));

test.each([
  ['worker', 'WORKER_MEM_LIMIT'],
  ['worker-extra', 'WORKER_EXTRA_MEM_LIMIT'],
])('%s takes a memory limit from %s (0, no limit, when unset)', (svc, v) =>
  expect(service(svc)).toContain(`mem_limit: \${${v}:-0}`),
);

test('logs rotate for the app, temporal, postgres and grafana', () => {
  expect(block('x-logging')).toMatch(/driver: local[\s\S]*max-size: 20m[\s\S]*max-file: '5'/);
  expect(block('x-app')).toContain('logging: *logging');
  for (const svc of ['postgres', 'temporal', 'grafana'])
    expect(service(svc), svc).toContain('logging: *logging');
});
