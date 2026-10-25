import { describe, expect, it } from 'vitest';
import { withTimeout } from '../../src/harvest/timeout.js';

const never = new Promise<string>(() => {});

describe('withTimeout', () => {
  it('returns the work when it finishes first', async () => {
    await expect(withTimeout(Promise.resolve('done'), 1000, () => 'late')).resolves.toBe('done');
  });
  it('returns the fallback after the limit', async () => {
    await expect(withTimeout(never, 5, () => null)).resolves.toBeNull();
  });
  it('rejects with what onTimeout throws', async () => {
    await expect(
      withTimeout(never, 5, () => {
        throw new Error('timed out');
      }),
    ).rejects.toThrow('timed out');
  });
  it('passes the work’s own rejection through', async () => {
    await expect(withTimeout(Promise.reject(new Error('EIO')), 1000, () => 0)).rejects.toThrow(
      'EIO',
    );
  });
});
