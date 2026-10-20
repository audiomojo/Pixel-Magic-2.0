import { execFileSync } from 'node:child_process';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { expect, test } from 'vitest';
import { LABEL, renderPlist } from '../../src/harvest/launchd.js';

const opts = { node: '/opt/node/bin/node', repo: '/Users/me/pixel-magic', logDir: '/data/logs' };

test('renders the agent definition', () => {
  const p = renderPlist(opts);
  expect(LABEL).toBe('com.pixelmagic.harvester');
  expect(p).toContain('<key>Label</key>\n  <string>com.pixelmagic.harvester</string>');
  expect(p).toContain(
    [
      '<string>/opt/node/bin/node</string>',
      '<string>--env-file-if-exists=.env</string>',
      '<string>dist/harvest/host.js</string>',
    ].join('\n    '),
  );
  expect(p).toContain('<key>WorkingDirectory</key>\n  <string>/Users/me/pixel-magic</string>');
  expect(p).toMatch(/<key>RunAtLoad<\/key>\s*<true\/>/);
  expect(p).toMatch(/<key>KeepAlive<\/key>\s*<true\/>/);
  expect(p).toContain('<string>/data/logs/harvester.launchd.log</string>');
  expect(p).toContain('/usr/bin:/bin:/usr/sbin:/sbin');
  expect(p.match(/harvester\.launchd\.log/g)).toHaveLength(2);
});

test('escapes XML special characters in paths', () => {
  const p = renderPlist({ ...opts, repo: '/Users/a&b/<x>' });
  expect(p).toContain('/Users/a&amp;b/&lt;x&gt;');
  expect(p).not.toContain('a&b');
});

test.skipIf(process.platform !== 'darwin')('plutil accepts the rendered plist', () => {
  const dir = mkdtempSync(join(tmpdir(), 'plist-'));
  try {
    const f = join(dir, 'x.plist');
    writeFileSync(f, renderPlist({ ...opts, repo: '/Users/a&b' }));
    expect(execFileSync('/usr/bin/plutil', ['-lint', f]).toString()).toContain('OK');
  } finally {
    rmSync(dir, { recursive: true });
  }
});
