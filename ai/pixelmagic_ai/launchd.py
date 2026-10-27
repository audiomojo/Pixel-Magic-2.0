"""The launchd agent that keeps the AI worker running (modelled on src/harvest/launchd.ts).

    python -m pixelmagic_ai.launchd install | uninstall
"""

import os
import re
import shutil
import subprocess
import sys
import time
from collections.abc import Callable
from pathlib import Path
from xml.sax.saxutils import escape

from .config import load_settings

LABEL = "com.pixelmagic.ai"
LAUNCHD_LOG = "ai.launchd.log"
LAUNCHCTL = "/bin/launchctl"
REPO_ROOT = Path(__file__).resolve().parents[2]

Launchctl = Callable[[list[str]], tuple[int, str]]


def render_plist(uv: str, repo: Path, log_dir: Path) -> str:
    """`uv` is an absolute path (launchd's PATH is minimal); the worker runs in `<repo>/ai`."""
    args = [uv, "run", "--extra", "ml", "--extra", "mac", "python", "-m", "pixelmagic_ai.worker"]
    program = "\n".join(f"    <string>{escape(a)}</string>" for a in args)
    log = escape(str(log_dir / LAUNCHD_LOG))
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>{LABEL}</string>
  <key>ProgramArguments</key>
  <array>
{program}
  </array>
  <key>WorkingDirectory</key>
  <string>{escape(str(repo / "ai"))}</string>
  <key>EnvironmentVariables</key>
  <dict>
    <key>PATH</key>
    <string>/usr/bin:/bin:/usr/sbin:/sbin</string>
  </dict>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>StandardOutPath</key>
  <string>{log}</string>
  <key>StandardErrorPath</key>
  <string>{log}</string>
</dict>
</plist>
"""


def real_launchctl(args: list[str]) -> tuple[int, str]:
    r = subprocess.run([LAUNCHCTL, *args], capture_output=True, text=True)
    return r.returncode, r.stderr


def _not_loaded(code: int, stderr: str) -> bool:
    """"No such process" (3) or "Could not find service" (113)."""
    return code in (3, 113) or bool(re.search(r"no such process|could not find|not loaded", stderr, re.I))


def plist_path(agents_dir: Path) -> Path:
    return agents_dir / f"{LABEL}.plist"


BOOTSTRAP_TRIES = 10


def install(
    launchctl: Launchctl, agents_dir: Path, uid: int, uv: str, repo: Path, log_dir: Path,
    sleep: Callable[[float], None] = time.sleep,
) -> int:
    target = plist_path(agents_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    launchctl(["bootout", f"gui/{uid}/{LABEL}"])  # an old agent; failure = none was loaded
    agents_dir.mkdir(parents=True, exist_ok=True)
    target.write_text(render_plist(uv, repo, log_dir))
    code, err = 0, ""
    for i in range(BOOTSTRAP_TRIES):
        if i:
            sleep(1.0)
        code, err = launchctl(["bootstrap", f"gui/{uid}", str(target)])
        if code == 0 or re.search(r"already (loaded|bootstrapped)", err, re.I):
            print(f"installed {target}; the AI worker starts now and at every login")
            return 0
        # 5 / Input/output error: a bootout may still be tearing down.
        if code != 5 and not re.search(r"input/output error", err, re.I):
            break
    print(f"launchctl bootstrap failed: {err.strip()}", file=sys.stderr)
    return 1


def uninstall(launchctl: Launchctl, agents_dir: Path, uid: int) -> int:
    code, err = launchctl(["bootout", f"gui/{uid}/{LABEL}"])
    if code != 0 and not _not_loaded(code, err):
        print(f"launchctl bootout failed: {err.strip()}", file=sys.stderr)
        return 1
    plist_path(agents_dir).unlink(missing_ok=True)
    print("uninstalled the AI worker agent")
    return 0


def main(argv: list[str]) -> int:
    agents = Path.home() / "Library" / "LaunchAgents"
    uid = os.getuid()
    if argv == ["install"]:
        uv = shutil.which("uv")
        if uv is None:
            print("uv not found on PATH", file=sys.stderr)
            return 1
        log_dir = load_settings(os.environ, REPO_ROOT).data_dir / "logs"
        return install(real_launchctl, agents, uid, os.path.abspath(uv), REPO_ROOT, log_dir)
    if argv == ["uninstall"]:
        return uninstall(real_launchctl, agents, uid)
    print("usage: python -m pixelmagic_ai.launchd install | uninstall", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
