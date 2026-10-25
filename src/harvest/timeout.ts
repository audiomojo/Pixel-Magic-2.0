/**
 * `work`'s result, or `onTimeout()`'s after `ms` (return a fallback value, or throw).
 * The work itself is not cancelled: callers bound hung calls (stale SMB mounts), not abort them.
 */
export function withTimeout<T, U = never>(
  work: Promise<T>,
  ms: number,
  onTimeout: () => U,
): Promise<T | U> {
  let timer: NodeJS.Timeout | undefined;
  const limit = new Promise<void>((resolve) => {
    timer = setTimeout(resolve, ms);
  }).then(onTimeout);
  return Promise.race([work, limit]).finally(() => clearTimeout(timer));
}
