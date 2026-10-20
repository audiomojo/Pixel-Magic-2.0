import { realpath, stat } from 'node:fs/promises';
import { basename, dirname, resolve, sep } from 'node:path';
import type pg from 'pg';

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
}

const NETWORK_FS = new Set(['smbfs', 'afpfs', 'nfs', 'webdav']);
const RESERVED_LABELS = new Set(['harvest', '.copies', '_logs']);

export function isReservedLabel(label: string): boolean {
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

/** True when `dataDir` lies inside the volume mounted at `root` (device+inode, then casefolded path). */
async function containsDataDir(root: string, dataDir: string): Promise<boolean> {
  const rootReal = await realpath(root).catch(() => root);
  const dataReal = await realpath(dataDir).catch(() => resolve(dataDir));
  const rk = rootReal.toLowerCase();
  const dk = dataReal.toLowerCase();
  if (dk === rk || dk.startsWith(rk.endsWith(sep) ? rk : rk + sep)) return true;
  const rs = await stat(root, { bigint: true }).catch(() => null);
  if (!rs) return false;
  let cur = dataReal;
  for (;;) {
    const s = await stat(cur, { bigint: true }).catch(() => null);
    if (s && s.dev === rs.dev && s.ino === rs.ino) return true;
    const parent = dirname(cur);
    if (parent === cur) return false;
    cur = parent;
  }
}

/** Applies the spec §4.1 exclusions and the §4.2 identities to the current mounts. */
export async function detectSources(
  sys: SystemProbe,
  dataDir: string,
  volumes = '/Volumes/',
): Promise<DetectedSource[]> {
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
      const name = basename(m.mountPoint);
      const uuid = info?.['VolumeUUID'];
      const vname =
        typeof info?.['VolumeName'] === 'string' && info['VolumeName'] ? info['VolumeName'] : name;
      src = {
        identity: typeof uuid === 'string' && uuid ? `local:${uuid}` : `local-name:${vname}`,
        labelBase: vname,
        kind: 'local',
        mountPath: m.mountPoint,
      };
    }
    if (isReservedLabel(src.labelBase)) continue;
    if (await containsDataDir(m.mountPoint, dataDir)) continue;
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

/** Upserts by identity (an existing label is kept), suffixes a colliding new label, returns every source. */
export async function registerSources(
  pool: pg.Pool,
  detected: DetectedSource[],
): Promise<SourceRow[]> {
  for (const d of detected) {
    if (isReservedLabel(d.labelBase)) throw new Error(`reserved source label: ${d.labelBase}`);
  }
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query("SELECT pg_advisory_xact_lock(hashtext('harvest_sources'))");
    for (const d of detected) {
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
    const { rows } = await client.query<DbRow>(
      `SELECT id::text AS id, identity, label, kind, last_mount_path, last_seen_at
       FROM harvest_sources ORDER BY id`,
    );
    await client.query('COMMIT');
    const now = new Set(detected.map((d) => d.identity));
    return rows.map((r) => ({
      id: r.id,
      identity: r.identity,
      label: r.label,
      kind: r.kind,
      lastMountPath: r.last_mount_path,
      lastSeenAt: r.last_seen_at,
      mounted: now.has(r.identity),
    }));
  } catch (e) {
    await client.query('ROLLBACK').catch(() => {});
    throw e;
  } finally {
    client.release();
  }
}

/** The current mount path of the source with this id, or null when it is not mounted. */
export async function resolveMount(
  pool: pg.Pool,
  sys: SystemProbe,
  sourceId: string,
  dataDir: string,
): Promise<string | null> {
  const { rows } = await pool.query<{ identity: string }>(
    'SELECT identity FROM harvest_sources WHERE id = $1',
    [sourceId],
  );
  const identity = rows[0]?.identity;
  if (!identity) return null;
  const hit = (await detectSources(sys, dataDir)).find((d) => d.identity === identity);
  return hit?.mountPath ?? null;
}
