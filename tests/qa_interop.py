#!/usr/bin/python3
"""Test both native RustRadio/GNU Radio download directions without upstream edits."""
import argparse
import json
from pathlib import Path
import shutil
import subprocess
import threading
import numpy
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


def run_graph(graph, already_started=False):
    errors = []

    def run():
        try:
            if already_started:
                graph.wait()
            else:
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


class observed_source(gr.sync_block):
    def __init__(self, data, complex_samples):
        dtype = numpy.complex64 if complex_samples else numpy.float32
        super().__init__(name="interop_observed", in_sig=[], out_sig=[dtype])
        self.data = numpy.asarray(data, dtype=dtype)
        self.release = threading.Event()
        self.position = 0
        # Leave the rate unknown to exercise the server fallback on both transports.
        self.set_output_stream_name(0, "iq")

    def work(self, inputs, outputs):
        if not self.release.wait(10):
            raise RuntimeError("interop observer did not connect")
        if self.position == len(self.data):
            return -1
        count = min(len(outputs[0]), len(self.data) - self.position)
        outputs[0][:count] = self.data[self.position:self.position + count]
        if not self.position:
            for tag in expected_tags():
                self.add_item_tag(0, tag)
        self.position += count
        return count


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
tokio-tungstenite = "0.29"
futures-util = {{ version = "0.3", features = ["sink"] }}
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
            subprocess.run([str(args.build.resolve() / "qa_transport"),
                            "--catalogue", address], check=True, timeout=10)
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
        for mode in ("observe", "observe-ws"):
            # An ordinary GNU Radio output is downloaded directly by RustRadio.
            listener = iqstream.server("127.0.0.1:0")
            listener.register_all_streams(iqstream.metadata_profile.RUSTRADIO, default_sample_rate_hz=48000)
            vector = observed_source(data, complex_samples)
            primary = blocks.vector_sink_c() if complex_samples else blocks.vector_sink_f()
            graph = gr.top_block()
            graph.connect(vector, primary)
            graph.start()
            entries = [entry for entry in listener.streams() if entry.origin and
                       entry.origin.block_id == vector.to_basic_block().unique_id()]
            assert len(entries) == 1
            assert entries[0].origin.sample_rate_hz is None
            assert entries[0].sample_rate_hz == 48000
            subprocess.run([str(args.build.resolve() / "qa_transport"),
                            "--catalogue", listener.address()], check=True, timeout=10)
            remote = subprocess.Popen([str(executable), mode, kind,
                                       listener.address(), entries[0].name],
                                      stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            try:
                assert remote.stdout.readline().strip() == "connected", remote.stderr.read()
                vector.release.set()
                run_graph(graph, already_started=True)
                check_process(remote)
                assert list(primary.data()) == data
            finally:
                vector.release.set()
                graph.stop()
                graph.wait()
                if remote.poll() is None:
                    remote.kill()
                    remote.wait()
                listener.shutdown()
    print("RustRadio interoperability passed: both layouts, both directions, catalogues and automatic gRPC/WebSocket downloads")


if __name__ == "__main__":
    main()
