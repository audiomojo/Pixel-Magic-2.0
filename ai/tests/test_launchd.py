import plistlib
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from pixelmagic_ai import launchd

UV = "/Users/me/.local/bin/uv"
REPO = Path("/Users/me/src/pixel & <magic>")
LOGS = Path("/Users/me/data/logs")


def test_render_plist_contents():
    p = plistlib.loads(launchd.render_plist(UV, REPO, LOGS).encode())
    assert p["Label"] == "com.pixelmagic.ai"
    assert p["ProgramArguments"] == [
        UV, "run", "--extra", "ml", "--extra", "mac", "python", "-m", "pixelmagic_ai.worker",
    ]
    assert p["WorkingDirectory"] == "/Users/me/src/pixel & <magic>/ai"
    assert p["EnvironmentVariables"] == {"PATH": "/usr/bin:/bin:/usr/sbin:/sbin"}
    assert p["RunAtLoad"] is True
    assert p["KeepAlive"] is True
    assert p["StandardOutPath"] == "/Users/me/data/logs/ai.launchd.log"
    assert p["StandardErrorPath"] == "/Users/me/data/logs/ai.launchd.log"


@pytest.mark.skipif(sys.platform != "darwin" or not shutil.which("plutil"), reason="needs plutil")
def test_rendered_plist_passes_plutil(tmp_path):
    f = tmp_path / "com.pixelmagic.ai.plist"
    f.write_text(launchd.render_plist(UV, REPO, LOGS))
    out = subprocess.run(["plutil", "-lint", str(f)], capture_output=True, text=True)
    assert out.returncode == 0, out.stdout + out.stderr


class FakeLaunchctl:
    def __init__(self, results: dict[str, tuple[int, str]] | None = None) -> None:
        self.calls: list[list[str]] = []
        self.results = results or {}

    def __call__(self, args: list[str]) -> tuple[int, str]:
        self.calls.append(args)
        return self.results.get(args[0], (0, ""))


def test_install_boots_out_writes_and_bootstraps(tmp_path):
    agents, logs = tmp_path / "agents", tmp_path / "logs"
    lc = FakeLaunchctl({"bootout": (3, "No such process")})
    code = launchd.install(lc, agents, 501, UV, REPO, logs)
    target = agents / "com.pixelmagic.ai.plist"
    assert code == 0
    assert lc.calls == [
        ["bootout", "gui/501/com.pixelmagic.ai"],
        ["bootstrap", "gui/501", str(target)],
    ]
    assert target.read_text() == launchd.render_plist(UV, REPO, logs)
    assert logs.is_dir()


def test_install_replaces_an_old_plist(tmp_path):
    agents = tmp_path / "agents"
    agents.mkdir()
    (agents / "com.pixelmagic.ai.plist").write_text("old")
    assert launchd.install(FakeLaunchctl(), agents, 501, UV, REPO, tmp_path / "logs") == 0
    assert (agents / "com.pixelmagic.ai.plist").read_text() != "old"


def test_install_fails_when_bootstrap_fails(tmp_path):
    lc = FakeLaunchctl({"bootstrap": (1, "bad plist")})
    assert launchd.install(lc, tmp_path / "a", 501, UV, REPO, tmp_path / "l") == 1


def test_uninstall_boots_out_then_removes(tmp_path):
    agents = tmp_path / "agents"
    agents.mkdir()
    target = agents / "com.pixelmagic.ai.plist"
    target.write_text("x")
    lc = FakeLaunchctl()
    assert launchd.uninstall(lc, agents, 501) == 0
    assert lc.calls == [["bootout", "gui/501/com.pixelmagic.ai"]]
    assert not target.exists()


def test_uninstall_when_not_installed_succeeds(tmp_path):
    lc = FakeLaunchctl({"bootout": (113, "Could not find service")})
    assert launchd.uninstall(lc, tmp_path, 501) == 0


def test_uninstall_keeps_plist_when_bootout_fails(tmp_path):
    target = tmp_path / "com.pixelmagic.ai.plist"
    target.write_text("x")
    assert launchd.uninstall(FakeLaunchctl({"bootout": (1, "Operation not permitted")}), tmp_path, 501) == 1
    assert target.exists()
