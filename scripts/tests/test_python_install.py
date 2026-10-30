"""Check CPython installer choices with recording commands, without a build."""

import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
FIXTURES = Path(__file__).parent / "fixtures"


class PythonInstallTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="gufi-python-install-")
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.source = self.directory / "source"
        self.source.mkdir()
        self.bin = self.directory / "bin"
        self.bin.mkdir()
        for name in ["cc", "uname", "make", "gmake", "llvm-profdata"]:
            self.command(self.bin / name)
        self.command(self.source / "configure")
        self.log = self.directory / "commands"
        self.env = {key: value for key, value in os.environ.items()
                    if key not in ["CFLAGS", "CPPFLAGS", "LDFLAGS", "CC", "LLVM_PROFDATA", "STATIC", "PGO_EXCLUDE"]
                    and not key.startswith("PYTHON")}
        self.env.update(
            PATH=str(self.bin) + os.pathsep + os.defpath,
            INSTALLER=str(ROOT / "scripts/python-install.sh"),
            PYTHON_SRC_DIR=str(self.source), PYTHON_INSTALL_DIR=str(self.directory / "prefix"),
            CHECK_LOG=str(self.log), MAKE_JOBS="1", CC=str(self.bin / "cc"),
            FIXTURE_PROFDATA=str(self.bin / "llvm-profdata"),
        )

    def command(self, path):
        shutil.copyfile(FIXTURES / "python-build-command.sh", path)
        path.chmod(0o755)

    def run_harness(self, action, **environment):
        return subprocess.run(
            ["/bin/bash", str(FIXTURES / "python-build-harness.sh"), action],
            env=dict(self.env, **environment), cwd=self.directory,
            capture_output=True, text=True,
        )

    def calls(self, command):
        return [line.decode().split("\0")[:-1]
                for line in self.log.read_bytes().splitlines()
                if line.startswith(command.encode() + b"\0")]

    def test_default_is_requested_release_without_installing(self):
        result = self.run_harness("dry-run")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("version: 3.15.2", result.stdout)
        self.assertIn("debug: disabled", result.stdout)
        result = subprocess.run(["bash", self.env["INSTALLER"]], env=self.env,
                                capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("Usage:", result.stdout)

    def test_release_keeps_pgo_lto_and_overrides_inherited_o0(self):
        result = self.run_harness("build", CFLAGS="-I/example/include -O0 -g3")
        self.assertEqual(result.returncode, 0, result.stderr)
        call = self.calls("configure")[0]
        self.assertIn("-O0 -g3 -fno-semantic-interposition -O3 -g0", call[1])
        self.assertIn("-ftls-model=initial-exec", call[1])
        for option in ["--enable-optimizations", "--with-lto", "--without-pydebug", "--with-frame-pointers"]:
            self.assertIn(option, call)
        self.assertIn("PROFILE_TASK=-m test --pgo -x test_generators", self.calls("make")[0])

    def test_lto_debug_flags_are_overridden_during_build_and_install(self):
        result = self.run_harness("build", CFLAGS_NODIST="-g3", LDFLAGS_NODIST="-g")
        self.assertEqual(result.returncode, 0, result.stderr)
        for call in self.calls("make"):
            self.assertIn("CFLAGS_NODIST=-g3 -O3 -g0", call)
            self.assertIn("LDFLAGS_NODIST=-g -g0", call)

    def test_freebsd_uses_gmake_and_matching_clang_profile_tool(self):
        (self.source / "Makefile").touch()
        result = self.run_harness("build", FIXTURE_OS="FreeBSD", FIXTURE_COMPILER="clang version 19 fixture")
        self.assertEqual(result.returncode, 0, result.stderr)
        calls = self.calls("gmake")
        self.assertEqual(calls[0][-1], "distclean")
        self.assertIn("-j1", calls[1])
        self.assertEqual(calls[2][-1], "install")
        self.assertEqual(self.calls("make"), [])
        configure = self.calls("configure")[0]
        self.assertEqual(configure[4], self.env["FIXTURE_PROFDATA"])
        self.assertIn("-I/usr/local/include", configure[2])
        self.assertIn("-L/usr/local/lib", configure[3])

    def test_missing_matching_clang_tool_stops_before_configuration(self):
        result = self.run_harness("build", FIXTURE_COMPILER="clang fixture", FIXTURE_PROFDATA="llvm-profdata")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("matching tool", result.stderr)
        self.assertEqual(self.calls("configure"), [])

    def test_static_build_omits_shared_tls_and_allows_complete_training(self):
        result = self.run_harness("build", STATIC="1", PGO_EXCLUDE="")
        self.assertEqual(result.returncode, 0, result.stderr)
        configure = self.calls("configure")[0]
        self.assertNotIn("--enable-shared", configure)
        self.assertNotIn("-ftls-model=initial-exec", configure[1])
        self.assertIn("-fPIC", configure[1])
        self.assertIn("PROFILE_TASK=-m test --pgo", self.calls("make")[0])


if __name__ == "__main__":
    unittest.main()
