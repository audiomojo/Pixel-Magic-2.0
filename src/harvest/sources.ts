import * as fsp from 'node:fs/promises';
import { basename, dirname, resolve, sep } from 'node:path';
import type pg from 'pg';
import { withTx } from '../db.js';
import { logger } from '../logger.js';
import { withTimeout } from './timeout.js';

export interface MountEntry {
  device: string;
  mountPoint: string;
  fsType: string;
  options: string[];
}

/** Parses `mount` output. Splits on the last ` (` and the first ` on `, so mount points may hold spaces. */
export function parseMount(output: string): MountEntry[] {
  const out: MountEntry[] = [];
  for (const line of output.split('\n')) {
    const open = line.lastIndexOf(' (');
    if (open < 0 || !line.endsWith(')')) continue;
    const head = line.slice(0, open);
    const on = head.indexOf(' on ');
    if (on < 0) continue;
    const opts = line
      .slice(open + 2, -1)
      .split(',')
      .map((o) => o.trim());
    out.push({
      device: head.slice(0, on),
      mountPoint: head.slice(on + 4),
      fsType: opts[0] ?? '',
      options: opts.slice(1),
    });
  }
  return out;
}

export interface SystemProbe {
  mountOutput(): Promise<string>;
  diskutilInfo(device: string): Promise<Record<string, unknown> | null>;
  statfs(path: string): Promise<{ blocks: bigint; bavail: bigint; bsize: bigint }>;
}

export interface DetectedSource {
  identity: string;
  labelBase: string;
  kind: 'network' | 'local';
  mountPath: string;
  /** diskutil failed for this local volume: `identity` is '' and registerSources resolves it by mount path. */
  probeFailed?: boolean;
}

const NETWORK_FS = new Set(['smbfs', 'afpfs', 'nfs', 'webdav']);
const RESERVED_LABELS = new Set(['harvest', '.copies', '_logs']);

function isReservedLabel(label: string): boolean {
  return RESERVED_LABELS.has(label) || label.startsWith('.');
}

function safeDecode(s: string): string {
  try {
    return decodeURIComponent(s);
  } catch {
    return s;
  }
}

/** `//GUEST:@NAS._smb._tcp.local/Plex` → server `nas`, share `Plex`; null when the share is unusable. */
function parseShare(device: string): { server: string; share: string } | null {
  const rest = device.replace(/^\/\//, '');
  const slash = rest.indexOf('/');
  const hostPart = slash < 0 ? rest : rest.slice(0, slash);
  const sharePart = slash < 0 ? '' : rest.slice(slash + 1).replace(/\/+$/, '');
  const host = hostPart.slice(hostPart.lastIndexOf('@') + 1);
  const last = sharePart.slice(sharePart.lastIndexOf('/') + 1);
  const share = safeDecode(last).replaceAll('/', '_');
  if (share === '' || share === '.' || share === '..') return null;
  const server = safeDecode(host)
    .toLowerCase()
    .replace(/\._smb\._tcp\.local$/, '');
  return { server, share };
}

export interface FsProbe {
  realpath(path: string): Promise<string>;
  stat(path: string, opts: { bigint: true }): Promise<{ dev: bigint; ino: bigint }>;
}

export interface DetectOptions {
  /** Mount prefix; tests point it at tmp dirs. */
  volumes?: string;
  /** Bound on the fs calls made against one mount root (a stale SMB mount can hang them). */
  fsTimeoutMs?: number;
  fs?: FsProbe;
}

class FsTimeout extends Error {}

function bounded<T>(work: Promise<T>, ms: number): Promise<T> {
  return withTimeout(work, ms, () => {
    throw new FsTimeout();
  });
}

/** True when `dataDir` lies inside the volume mounted at `root` (device+inode, then casefolded path). */
async function containsDataDir(root: string, dataDir: string, fs: FsProbe): Promise<boolean> {
  const rootReal = await fs.realpath(root).catch(() => root);
  const dataReal = await fs.realpath(dataDir).catch(() => resolve(dataDir));
  const rk = rootReal.toLowerCase();
  const dk = dataReal.toLowerCase();
  if (dk === rk || dk.startsWith(rk.endsWith(sep) ? rk : rk + sep)) return true;
  const rs = await fs.stat(root, { bigint: true }).catch(() => null);
  if (!rs) return false;
  let cur = dataReal;
  for (;;) {
    const s = await fs.stat(cur, { bigint: true }).catch(() => null);
    if (s && s.dev === rs.dev && s.ino === rs.ino) return true;
    const parent = dirname(cur);
    if (parent === cur) return false;
    cur = parent;
  }
}

/**
 * Containment probes still running, per fs seam and (mount point, data dir). A probe on a hung
 * SMB share never returns and holds a libuv thread: later detections (the page, every
 * sendBatch, every scan) wait on the same one instead of starting another, so a hung share
 * cannot exhaust the thread pool and freeze all file I/O.
 */
const pendingProbes = new WeakMap<FsProbe, Map<string, Promise<boolean>>>();

function probeContainment(root: string, dataDir: string, fs: FsProbe): Promise<boolean> {
  let byMount = pendingProbes.get(fs);
  if (!byMount) pendingProbes.set(fs, (byMount = new Map<string, Promise<boolean>>()));
  const key = `${root}\0${dataDir}`;
  let probe = byMount.get(key);
  if (!probe) {
    probe = containsDataDir(root, dataDir, fs);
    byMount.set(key, probe);
    const done = () => byMount.delete(key);
    probe.then(done, done);
  }
  return probe;
}

/** Applies the spec §4.1 exclusions and the §4.2 identities to the current mounts. */
export async function detectSources(
  sys: SystemProbe,
  dataDir: string,
  opts: DetectOptions = {},
): Promise<DetectedSource[]> {
  const volumes = opts.volumes ?? '/Volumes/';
  const fsTimeoutMs = opts.fsTimeoutMs ?? 5000;
  const fs: FsProbe = opts.fs ?? fsp;
  const found: DetectedSource[] = [];
  for (const m of parseMount(await sys.mountOutput())) {
    if (!m.mountPoint.startsWith(volumes)) continue;
    const top = m.mountPoint.slice(volumes.length).split('/', 1)[0] ?? '';
    let src: DetectedSource;
    if (NETWORK_FS.has(m.fsType)) {
      const p = parseShare(m.device);
      if (!p) continue;
      src = {
        identity: `net:${p.server}/${p.share.toLowerCase()}`,
        labelBase: `NAS-${p.share}`,
        kind: 'network',
        mountPath: m.mountPoint,
      };
    } else {
      if (
        m.mountPoint === `${volumes}Recovery` ||
        m.options.includes('nobrowse') ||
        m.device.includes('@') ||
        top.startsWith('.') ||
        top.startsWith('com.apple.')
      )
        continue;
      const info = await sys.diskutilInfo(m.device).catch(() => null);
      if (info?.['BusProtocol'] === 'Disk Image') continue;
      if (!info) {
        // Never mint a name-based identity from a failed probe: it would fork a UUID drive's history.
        src = {
          identity: '',
          labelBase: basename(m.mountPoint),
          kind: 'local',
          mountPath: m.mountPoint,
          probeFailed: true,
        };
      } else {
        const name = basename(m.mountPoint);
        const uuid = info['VolumeUUID'];
        const vname =
          typeof info['VolumeName'] === 'string' && info['VolumeName'] ? info['VolumeName'] : name;
        src = {
          identity: typeof uuid === 'string' && uuid ? `local:${uuid}` : `local-name:${vname}`,
          labelBase: vname,
          kind: 'local',
          mountPath: m.mountPoint,
        };
      }
    }
    if (isReservedLabel(src.labelBase)) continue;
    try {
      if (await bounded(probeContainment(m.mountPoint, dataDir, fs), fsTimeoutMs)) continue;
    } catch (e) {
      if (!(e instanceof FsTimeout)) throw e;
      logger.warn(
        { mountPoint: m.mountPoint, fsTimeoutMs },
        'harvest: mount did not answer, skipped',
      );
      continue;
    }
    found.push(src);
  }
  return found;
}

export interface SourceRow {
  id: string;
  identity: string;
  label: string;
  kind: string;
  lastMountPath: string | null;
  lastSeenAt: Date | null;
  mounted: boolean;
}

interface DbRow {
  id: string;
  identity: string;
  label: string;
  kind: string;
  last_mount_path: string | null;
  last_seen_at: Date | null;
}

/** A mounted local volume whose probe failed and that matches no known source: shown, not selectable. */
export interface UnidentifiedVolume {
  mountPath: string;
  labelBase: string;
}

/** Upserts by identity (an existing label is kept), suffixes a colliding new label, returns every source. */
export async function registerSources(
  pool: pg.Pool,
  detected: DetectedSource[],
): Promise<SourceRow[]> {
  return (await registerDetected(pool, detected)).rows;
}

/** registerSources plus the volumes that could not be identified (no row is created for those). */
export async function registerDetected(
  pool: pg.Pool,
  detected: DetectedSource[],
): Promise<{ rows: SourceRow[]; unidentified: UnidentifiedVolume[] }> {
  const unidentified: UnidentifiedVolume[] = [];
  for (const d of detected) {
    if (!d.probeFailed && isReservedLabel(d.labelBase))
      throw new Error(`reserved source label: ${d.labelBase}`);
  }
  const resolved: DetectedSource[] = [];
  const rows = await withTx(pool, async (client) => {
    await client.query("SELECT pg_advisory_xact_lock(hashtext('harvest_sources'))");
    for (const d0 of detected) {
      let d = d0;
      if (d0.probeFailed) {
        // The probe failed: keep the identity of the local row last seen at this mount point.
        const known = await client.query<{ identity: string }>(
          "SELECT identity FROM harvest_sources WHERE kind = 'local' AND last_mount_path = $1",
          [d0.mountPath],
        );
        if (known.rows.length !== 1) {
          logger.warn(
            { mountPath: d0.mountPath, matches: known.rows.length },
            known.rows.length === 0
              ? 'harvest: diskutil failed for unknown volume, skipped'
              : 'harvest: diskutil failed and the mount path is ambiguous, skipped',
          );
          unidentified.push({ mountPath: d0.mountPath, labelBase: d0.labelBase });
          continue;
        }
        d = { ...d0, identity: known.rows[0]!.identity };
      }
      resolved.push(d);
      const upd = await client.query(
        `UPDATE harvest_sources SET last_mount_path = $2, last_seen_at = clock_timestamp()
         WHERE identity = $1`,
        [d.identity, d.mountPath],
      );
      if (upd.rowCount) continue;
      const taken = new Set(
        (await client.query<{ label: string }>('SELECT label FROM harvest_sources')).rows.map(
          (r) => r.label,
        ),
      );
      let label = d.labelBase;
      for (let k = 2; taken.has(label); k++) label = `${d.labelBase} (${k})`;
      await client.query(
        `INSERT INTO harvest_sources (identity, label, kind, last_mount_path, last_seen_at)
         VALUES ($1, $2, $3, $4, clock_timestamp())`,
        [d.identity, label, d.kind, d.mountPath],
      );
    }
    const all = await client.query<DbRow>(
      `SELECT id::text AS id, identity, label, kind, last_mount_path, last_seen_at
       FROM harvest_sources ORDER BY id`,
    );
    return all.rows;
  });
  const now = new Set(resolved.map((d) => d.identity));
  return {
    rows: rows.map((r) => ({
      id: r.id,
      identity: r.identity,
      label: r.label,
      kind: r.kind,
      lastMountPath: r.last_mount_path,
      lastSeenAt: r.last_seen_at,
      mounted: now.has(r.identity),
    })),
    unidentified,
  };
}

/** The current mount path of the source with this id, or null when it is not mounted. */
export async function resolveMount(
  pool: pg.Pool,
  sys: SystemProbe,
  sourceId: string,
  dataDir: string,
  detectOpts: DetectOptions = {},
): Promise<string | null> {
  const { rows } = await pool.query<{ identity: string; last_mount_path: string | null }>(
    'SELECT identity, last_mount_path FROM harvest_sources WHERE id = $1',
    [sourceId],
  );
  const row = rows[0];
  if (!row) return null;
  const hit = (await detectSources(sys, dataDir, detectOpts)).find((d) =>
    d.probeFailed ? d.mountPath === row.last_mount_path : d.identity === row.identity,
  );
  return hit?.mountPath ?? null;
}

/** A mounted source's path, and whether it is still the drive that was mounted at the start. */
export interface LiveMount {
  path: string;
  alive(): Promise<boolean>;
}

/**
 * The source's mount with its root's device id, or null when it is not mounted (or its root
 * cannot be read). An unplugged drive can leave a plain directory at its mount point, so
 * "exists" is not "still mounted": `alive()` compares the device id again.
 */
export async function liveMount(
  deps: {
    pool: pg.Pool;
    sys: SystemProbe;
    settings: { dataDir: string };
    detectOpts?: DetectOptions;
  },
  sourceId: string,
  stat: (p: string) => Promise<{ dev: bigint }> = (p) => fsp.lstat(p, { bigint: true }),
): Promise<LiveMount | null> {
  const path = await resolveMount(
    deps.pool,
    deps.sys,
    sourceId,
    deps.settings.dataDir,
    deps.detectOpts,
  );
  if (path === null) return null;
  const devOf = (): Promise<bigint | null> =>
    stat(path).then(
      (s) => s.dev,
      () => null,
    );
  const dev = await devOf();
  if (dev === null) return null;
  return { path, alive: async () => (await devOf()) === dev };
}
