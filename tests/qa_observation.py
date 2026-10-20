#!/usr/bin/python3
# SPDX-License-Identifier: MIT
"""Automatic download discovery without additional graph sinks."""
import threading
import time
import unittest

import numpy
import pmt
from gnuradio import blocks, gr, iqstream


class gated_source(gr.sync_block):
    def __init__(self, complex_samples, sample_rate_hz=48000):
        self.dtype = numpy.complex64 if complex_samples else numpy.float32
        super().__init__(name="iq_observation_source", in_sig=[], out_sig=[self.dtype])
        self.release = threading.Event()
        self.sent = False
        if sample_rate_hz is not None:
            self.set_output_stream_sample_rate(0, sample_rate_hz)

    def work(self, inputs, outputs):
        if not self.release.wait(10):
            raise RuntimeError("test source was not released")
        if self.sent:
            return -1
        count = min(64, len(outputs[0]))
        outputs[0][:count] = numpy.arange(count, dtype=self.dtype)
        for duplicate in range(2):
            self.add_item_tag(0, 7, pmt.intern("duplicate"), pmt.from_long(duplicate))
        self.sent = True
        return count


class observation_tests(unittest.TestCase):
    def test_real_and_complex_downloads(self):
        for complex_samples, declared_rate in ((False, 48000), (True, None)):
            with self.subTest(complex_samples=complex_samples, declared_rate=declared_rate):
                server = iqstream.server("127.0.0.1:0")
                producer_graph, download_graph = gr.top_block(), gr.top_block()
                source = gated_source(complex_samples, sample_rate_hz=declared_rate)
                sink_type = blocks.vector_sink_c if complex_samples else blocks.vector_sink_f
                primary, downloaded = sink_type(), sink_type()
                producer_graph.connect(source, primary)
                producer_graph.start()
                try:
                    self.assertEqual(server.streams(), [])
                    server.register_all_streams(default_sample_rate_hz=96000)
                    entries = [entry for entry in server.streams() if entry.origin and
                               entry.origin.block_id == source.to_basic_block().unique_id()]
                    self.assertEqual(len(entries), 1)
                    entry = entries[0]
                    self.assertEqual(entry.sample_rate_hz, declared_rate or 96000)
                    self.assertEqual(entry.origin.sample_rate_hz, declared_rate)
                    options = iqstream.stream_options()
                    options.loss = iqstream.loss_policy.ALLOW_GAPS
                    layout = (iqstream.sample_layout.COMPLEX if complex_samples
                              else iqstream.sample_layout.REAL)
                    remote = iqstream.source(server.address(), entry.name, layout, options)
                    download_graph.connect(remote, downloaded)
                    download_graph.start()
                    deadline = time.monotonic() + 5
                    while remote.status().state == iqstream.session_state.OPENING:
                        self.assertLess(time.monotonic(), deadline)
                        time.sleep(0.005)
                    self.assertEqual(remote.status().state, iqstream.session_state.STREAMING,
                                     remote.status().message)
                    if declared_rate is None:
                        server.register_all_streams(default_sample_rate_hz=192000)
                        updated = next(item for item in server.streams() if item.name == entry.name)
                        self.assertEqual(updated.sample_rate_hz, 192000)
                        self.assertEqual(remote.status().sample_rate_hz, 96000)
                    source.release.set()
                    producer_graph.wait()
                    download_graph.wait()
                    self.assertEqual(primary.data(), downloaded.data())
                    self.assertEqual(len(downloaded.data()), 64)
                    self.assertEqual([pmt.to_long(tag.value) for tag in downloaded.tags()
                                      if pmt.symbol_to_string(tag.key) == "duplicate"], [0, 1])
                    status = remote.status()
                    self.assertEqual(status.state, iqstream.session_state.COMPLETE,
                                     status.message)
                    self.assertEqual(status.sample_rate_hz, declared_rate or 96000)
                    self.assertEqual(status.lost_samples, 0)
                    self.assertEqual(status.source_sample_offset, 0)
                finally:
                    source.release.set()
                    producer_graph.stop()
                    download_graph.stop()
                    producer_graph.wait()
                    download_graph.wait()
                    server.shutdown()

    def test_invalid_default_rates(self):
        server = iqstream.server("127.0.0.1:0")
        try:
            for rate in (-1, float("nan"), float("inf"), -float("inf")):
                with self.subTest(rate=rate), self.assertRaises(RuntimeError):
                    server.register_all_streams(default_sample_rate_hz=rate)
            self.assertEqual(server.streams(), [])
        finally:
            server.shutdown()

    def test_enabled_server_does_not_need_clients(self):
        server = iqstream.server("127.0.0.1:0")
        server.register_all_streams()
        graph = gr.top_block()
        source = blocks.vector_source_f(list(range(1000)))
        sink = blocks.vector_sink_f()
        graph.connect(source, sink)
        try:
            graph.run()
            self.assertEqual(list(sink.data()), list(range(1000)))
        finally:
            server.shutdown()


if __name__ == "__main__":
    unittest.main()
