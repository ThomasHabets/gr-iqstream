#!/usr/bin/python3
"""Compile and execute all four GRC blocks using the installed GNU Radio compiler."""
import argparse
import importlib.util
from pathlib import Path
import subprocess
from gnuradio import iqstream
from qa_interop import run_graph


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--build", type=Path, required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    output = args.build.resolve() / "grc-generated"
    output.mkdir(parents=True, exist_ok=True)
    subprocess.run(["grcc", "-o", str(output),
                    str(root / "examples/iqstream_roundtrip.grc")], check=True,
                   timeout=60)
    spec = importlib.util.spec_from_file_location(
        "iqstream_roundtrip", output / "iqstream_roundtrip.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    graph = module.iqstream_roundtrip()
    try:
        run_graph(graph)
        for name in ("served_download", "client_download",
                     "client_upload", "served_upload"):
            status = getattr(graph, name).status()
            assert status.state == iqstream.session_state.COMPLETE, status.message
            assert status.next_sample == 1000
    finally:
        graph.iq_server.shutdown()
    print("GRC compilation and execution passed")


if __name__ == "__main__":
    main()
