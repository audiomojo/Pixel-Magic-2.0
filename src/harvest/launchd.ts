// The launchd agent that starts the harvester at login (spec §13). Pure rendering: the CLI
// (cli.ts) writes the file and calls launchctl.
import { join } from 'node:path';

export const LABEL = 'com.pixelmagic.harvester';
/** Crash output only: the host rotates its own log (DATA_DIR/logs/harvester.log). */
export const LAUNCHD_LOG = 'harvester.launchd.log';

/**
 * libuv's thread pool (default 4) runs every fs call: a hung SMB share can hold threads until
 * the share answers, so a larger pool keeps the page and the other drives responsive (I4).
 * Read by libuv at startup, so it is set in the environment, not in code.
 */
export const UV_THREADPOOL_SIZE = 32;

const esc = (s: string): string =>
  s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

/**
 * The agent definition. `node` is an absolute path (launchd's PATH is minimal), `repo` the working
 * directory (the host reads `.env` and `dist/` from there), `logDir` receives stdout and stderr.
 */
export function renderPlist(o: { node: string; repo: string; logDir: string }): string {
  const log = esc(join(o.logDir, LAUNCHD_LOG));
  return `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>${LABEL}</string>
  <key>ProgramArguments</key>
  <array>
    <string>${esc(o.node)}</string>
    <string>--env-file-if-exists=.env</string>
    <string>dist/harvest/host.js</string>
  </array>
  <key>WorkingDirectory</key>
  <string>${esc(o.repo)}</string>
  <key>EnvironmentVariables</key>
  <dict>
    <key>PATH</key>
    <string>/usr/bin:/bin:/usr/sbin:/sbin</string>
    <key>UV_THREADPOOL_SIZE</key>
    <string>${UV_THREADPOOL_SIZE}</string>
  </dict>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>StandardOutPath</key>
  <string>${log}</string>
  <key>StandardErrorPath</key>
  <string>${log}</string>
</dict>
</plist>
`;
}
