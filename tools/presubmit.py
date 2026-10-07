#!/usr/bin/python3
# SPDX-License-Identifier: GPL-3.0-or-later
"""Run every required presubmit; missing tools/checkouts are failures."""
import argparse
import hashlib
import os
from pathlib import Path
import shutil
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
SCHEMA_SHA256 = "ffac19f4521a3960e9ad75dab1f19259f3d8fe7c4585d686e2438430f04a6376"


def run(command, *, environment=None, timeout=300):
    print("+", " ".join(str(value) for value in command), flush=True)
    subprocess.run([str(value) for value in command], cwd=ROOT, check=True,
                   env=environment, timeout=timeout)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--build", type=Path, default=ROOT / "build")
    parser.add_argument("--rustradio", type=Path, default=ROOT.parent / "rustradio")
    parser.add_argument("--jobs", type=int, default=3)
    args = parser.parse_args()
    for tool in ("cmake", "ninja", "clang-format", "rustfmt", "cargo",
                 "grcc", "gnuradio-config-info", "protoc", "openssl"):
        if not shutil.which(tool):
            raise RuntimeError(f"Required presubmit tool missing: {tool}")
    version = subprocess.check_output(["gnuradio-config-info", "--version"],
                                      text=True).strip()
    if version not in ("3.10.12", "3.10.12.0"):
        raise RuntimeError(f"Presubmits target GNU Radio 3.10.12; found {version}")
    schema = (ROOT / "proto/iq_stream.proto").read_bytes()
    if hashlib.sha256(schema).hexdigest() != SCHEMA_SHA256:
        raise RuntimeError("Local schema changed; update upstream and recopy it")
    upstream = args.rustradio.resolve()
    if not (upstream / "proto/iq_stream.proto").is_file():
        raise RuntimeError("RustRadio checkout required: pass --rustradio PATH")
    if schema != (upstream / "proto/iq_stream.proto").read_bytes():
        raise RuntimeError("Schema differs from upstream; request/review upstream updates")
    files = []
    for directory in ("include", "lib", "tests", "python"):
        files.extend(sorted(str(path) for path in (ROOT / directory).rglob("*")
                            if path.suffix in (".h", ".cc")))
    run(["clang-format", "--dry-run", "--Werror", *files])
    run(["rustfmt", "--edition", "2024", "--check",
         ROOT / "tests/rustradio_fixture.rs"])
    for directory in ("include", "lib", "tests", "python", "grc", "examples",
                      "tools", "cmake"):
        for path in (ROOT / directory).rglob("*"):
            if not path.is_file() or "__pycache__" in path.parts:
                continue
            text = path.read_text()
            if not text.endswith("\n") or any(line != line.rstrip()
                                              for line in text.splitlines()):
                raise RuntimeError(f"Whitespace presubmit failed: {path}")
            if path.suffix == ".py":
                compile(text, str(path), "exec")
    build = args.build.resolve()
    run(["cmake", "-S", ROOT, "-B", build, "-G", "Ninja",
         "-DCMAKE_BUILD_TYPE=RelWithDebInfo", "-DENABLE_PYTHON=ON",
         "-DENABLE_TESTING=ON", f"-DPython3_EXECUTABLE={sys.executable}",
         f"-DPYTHON_EXECUTABLE={sys.executable}"])
    run(["cmake", "--build", build, "-j", args.jobs])
    run(["ctest", "--test-dir", build, "--output-on-failure", "-j", args.jobs])
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(build / "python")
    environment["GRC_BLOCKS_PATH"] = str(ROOT / "grc")
    run([sys.executable, ROOT / "tests/qa_grc.py", "--build", build],
        environment=environment, timeout=90)
    run([sys.executable, ROOT / "tests/qa_interop.py", "--build", build,
         "--rustradio", upstream], environment=environment, timeout=600)
    run([sys.executable, ROOT / "tests/qa_install.py", "--build", build],
        timeout=180)
    sanitized = build.parent / (build.name + "-sanitized")
    run(["cmake", "-S", ROOT, "-B", sanitized, "-G", "Ninja",
         "-DCMAKE_BUILD_TYPE=Debug", "-DENABLE_PYTHON=OFF",
         "-DENABLE_TESTING=ON", "-DENABLE_SANITIZERS=ON"])
    run(["cmake", "--build", sanitized, "-j", args.jobs])
    run(["ctest", "--test-dir", sanitized, "--output-on-failure",
         "-j", args.jobs])
    run(["git", "diff", "--check"])
    print("All presubmits passed", flush=True)


if __name__ == "__main__":
    main()
