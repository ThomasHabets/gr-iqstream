#!/usr/bin/python3
"""Compile and execute all four GRC blocks using the installed GNU Radio compiler."""
import argparse
import importlib.util
import os
from pathlib import Path
import subprocess
import yaml
from gnuradio import gr, iqstream
from qa_interop import run_graph


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--build", type=Path, required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    output = args.build.resolve() / "grc-generated"
    output.mkdir(parents=True, exist_ok=True)
    # GNU Radio 3.10 loads later block definitions last. An installed copy must
    # not shadow the source definitions being tested.
    environment = os.environ.copy()
    environment["GRC_BLOCKS_PATH"] = ""
    environment["GR_CONF_GRC_GLOBAL_BLOCKS_PATH"] = os.pathsep.join((
        gr.prefs().get_string("grc", "global_blocks_path", ""), str(root / "grc")))
    environment["XDG_CACHE_HOME"] = str(args.build.resolve() / "cache")
    subprocess.run(["grcc", "-o", str(output),
                    str(root / "examples/iqstream_roundtrip.grc")], check=True,
                   timeout=60, env=environment)
    spec = importlib.util.spec_from_file_location(
        "iqstream_roundtrip", output / "iqstream_roundtrip.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    graph = module.iqstream_roundtrip()
    try:
        run_graph(graph)
        assert all(entry.origin is None for entry in graph.iq_server.streams())
        for name in ("served_download", "client_download",
                     "client_upload", "served_upload"):
            status = getattr(graph, name).status()
            assert status.state == iqstream.session_state.COMPLETE, status.message
            assert status.next_sample == 1000
    finally:
        graph.iq_server.shutdown()
    document = yaml.safe_load((root / "examples/iqstream_roundtrip.grc").read_text())
    document["options"]["parameters"]["id"] = "iqstream_observation_grc"
    for block in document["blocks"]:
        if block["name"] == "iq_server":
            block["parameters"].update(
                observe_all="True",
                observation_profile="iqstream.metadata_profile.RUSTRADIO",
                default_sample_rate="96000")
        if block["name"] == "real_samples":
            block["parameters"]["alias"] = r'Receiver "RF" \ tap'
    enabled = output / "observation.grc"
    enabled.write_text(yaml.safe_dump(document))
    subprocess.run(["grcc", "-o", str(output), str(enabled)], check=True,
                   timeout=60, env=environment)
    spec = importlib.util.spec_from_file_location(
        "iqstream_observation_grc", output / "iqstream_observation_grc.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    graph = module.iqstream_observation_grc()
    try:
        run_graph(graph)
        origins = {entry.origin.block_id for entry in graph.iq_server.streams()
                   if entry.origin}
        assert all(entry.sample_rate_hz == 96000 for entry in graph.iq_server.streams()
                   if entry.origin)
        named = next(entry for entry in graph.iq_server.streams()
                     if entry.origin and entry.origin.block_id == graph.real_samples.unique_id())
        assert named.name.startswith(r'gr/real_samples [Receiver "RF" \ tap]/'), named.name
        assert graph.real_samples.unique_id() in origins
        assert graph.complex_samples.unique_id() in origins
    finally:
        graph.iq_server.shutdown()
    print("GRC compilation and execution passed, observation disabled and enabled")


if __name__ == "__main__":
    main()
