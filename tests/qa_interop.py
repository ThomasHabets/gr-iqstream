#!/usr/bin/python3
"""Test both native RustRadio/GNU Radio download directions without upstream edits."""
import argparse
import json
from pathlib import Path
import shutil
import subprocess
import threading
from gnuradio import blocks, gr, iqstream
import pmt


def expected_tags():
    values = ((123, "u64", pmt.from_uint64(2**64 - 1)),
              (123, "false", pmt.PMT_F),
              (123, "empty", pmt.intern("")),
              (124, "i64", pmt.from_long(-(2**63))),
              (124, "float", pmt.from_double(0.25)))
    result = []
    for offset, key, value in values:
        tag = gr.tag_t()
        tag.offset, tag.key, tag.value = offset, pmt.intern(key), value
        result.append(tag)
    return result


def run_graph(graph):
    errors = []

    def run():
        try:
            graph.run()
        except Exception as error:
            errors.append(error)

    thread = threading.Thread(target=run)
    thread.start()
    thread.join(30)
    if thread.is_alive():
        graph.stop()
        thread.join(10)
        raise RuntimeError("interop graph timed out")
    if errors:
        raise errors[0]


def check_complete(block):
    state = block.status()
    assert state.state == iqstream.session_state.COMPLETE, state.message
    assert state.next_sample == 1000


def check_process(process):
    _, error = process.communicate(timeout=30)
    assert process.returncode == 0, error


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rustradio", type=Path, required=True)
    parser.add_argument("--build", type=Path, required=True)
    args = parser.parse_args()
    project = Path(__file__).resolve().parents[1]
    fixture = args.build.resolve() / "rustradio-interop"
    fixture.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(project / "tests/rustradio_fixture.rs", fixture / "main.rs")
    manifest = f'''[package]
name = "iqstream-interop"
version = "0.0.0"
edition = "2024"
[[bin]]
name = "iqstream-interop"
path = "main.rs"
[dependencies]
rustradio = {{ path = {json.dumps(str(args.rustradio.resolve()))}, default-features = false, features = ["unstable"] }}
tokio = {{ version = "1.44", features = ["full"] }}
'''
    (fixture / "Cargo.toml").write_text(manifest)
    subprocess.run(["cargo", "build", "--manifest-path", str(fixture / "Cargo.toml")], check=True)
    executable = fixture / "target/debug/iqstream-interop"
    for complex_samples in (False, True):
        kind = "complex" if complex_samples else "real"
        layout = (iqstream.sample_layout.COMPLEX if complex_samples
                  else iqstream.sample_layout.REAL)
        data = ([complex(i / 32, -i / 64) for i in range(1000)]
                if complex_samples else [i / 32 for i in range(1000)])
        options = iqstream.stream_options()
        options.sample_rate_hz = 48000
        options.profile = iqstream.metadata_profile.RUSTRADIO
        options.max_in_flight_frames = 1
        # RustRadio -> GNU Radio.
        remote = subprocess.Popen([str(executable), "serve", kind],
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                  text=True)
        try:
            address = remote.stdout.readline().strip()
            assert address, remote.stderr.read()
            source = iqstream.source(address, "iq", layout, options)
            collected = (blocks.vector_sink_c() if complex_samples
                         else blocks.vector_sink_f())
            graph = gr.top_block()
            graph.connect(source, collected)
            run_graph(graph)
            check_complete(source)
            assert list(collected.data()) == data
            for expected in expected_tags():
                found = [tag for tag in collected.tags()
                         if pmt.equal(tag.key, expected.key)]
                assert len(found) == 1
                assert found[0].offset == expected.offset
                assert pmt.equal(found[0].value, expected.value)
            check_process(remote)
        finally:
            if remote.poll() is None:
                remote.kill()
                remote.wait()
        # GNU Radio -> RustRadio.
        listener = iqstream.server("127.0.0.1:0")
        outgoing = iqstream.server_sink(listener, "iq", layout, options)
        vector = (blocks.vector_source_c(data, False, 1, expected_tags())
                  if complex_samples else
                  blocks.vector_source_f(data, False, 1, expected_tags()))
        graph = gr.top_block()
        graph.connect(vector, outgoing)
        remote = subprocess.Popen([str(executable), "receive", kind,
                                   listener.address()], stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE, text=True)
        try:
            run_graph(graph)
            check_complete(outgoing)
            check_process(remote)
        finally:
            if remote.poll() is None:
                remote.kill()
                remote.wait()
            listener.shutdown()
    print("RustRadio interoperability passed: both layouts, both directions")


if __name__ == "__main__":
    main()
