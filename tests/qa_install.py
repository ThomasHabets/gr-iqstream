#!/usr/bin/python3
"""Validate a staged installation, its exported CMake target and Python package."""
import argparse
import os
from pathlib import Path
import subprocess
import shutil
import sys


def run(command, **kwargs):
    subprocess.run([str(value) for value in command], check=True, timeout=90,
                   **kwargs)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--build", type=Path, required=True)
    args = parser.parse_args()
    build = args.build.resolve()
    prefix = build / "stage"
    if prefix.exists():
        shutil.rmtree(prefix)
    run(["cmake", "--install", build, "--prefix", prefix])
    libraries = list(prefix.glob("lib*/libgnuradio-iqstream.so"))
    assert len(libraries) == 1, libraries
    library_dir = libraries[0].parent
    packages = list(prefix.glob("**/gnuradio/iqstream/__init__.py"))
    assert len(packages) == 1, packages
    python_root = packages[0].parent.parent.parent
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(python_root)
    environment["LD_LIBRARY_PATH"] = str(library_dir)
    root = Path(__file__).resolve().parents[1]
    consumer = build / "install-consumer"
    consumer.mkdir(exist_ok=True)
    (consumer / "CMakeLists.txt").write_text('''cmake_minimum_required(VERSION 3.16.3)
project(iqstream_consumer LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)
find_package(gnuradio-iqstream CONFIG REQUIRED)
add_executable(consumer main.cc)
target_link_libraries(consumer PRIVATE gnuradio::iqstream)
''')
    (consumer / "main.cc").write_text('''#include <gnuradio/iqstream/source.h>
#include <gnuradio/iqstream/iq_stream.pb.h>
int main() {
    auto block = gr::iqstream::source::make("127.0.0.1:1", "iq");
    rustradio::iq::v1::StreamDescription description;
    return block->status().state == gr::iqstream::session_state::IDLE ? 0 : 1;
}
''')
    run(["cmake", "-S", consumer, "-B", consumer / "build", "-G", "Ninja",
         f"-DCMAKE_PREFIX_PATH={prefix}", f"-DPYTHON_EXECUTABLE={sys.executable}"])
    run(["cmake", "--build", consumer / "build"])
    run([consumer / "build/consumer"], env=environment)
    assert (prefix / "share/gnuradio/grc/blocks/iqstream_source.block.yml").is_file()
    assert (prefix / "share/gr-iqstream/proto/iq_stream.proto").read_bytes() == (
        root / "proto/iq_stream.proto").read_bytes()
    run([sys.executable, root / "examples/roundtrip.py"], env=environment)
    print("Staged C++/Python installation passed")


if __name__ == "__main__":
    main()
