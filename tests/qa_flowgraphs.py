#!/usr/bin/python3
"""Exercise actual GNU Radio scheduling, tags, shutdown and bindings."""
import unittest
from pathlib import Path
import subprocess
import tempfile
import time
from gnuradio import blocks, gr, iqstream
import pmt


class Flowgraphs(unittest.TestCase):
    def roundtrip(self, upload, complex_samples, count, tls=None):
        listener = iqstream.server("127.0.0.1:0", tls or iqstream.tls_options())
        options = iqstream.stream_options()
        options.sample_rate_hz = 48000
        if tls is not None:
            options.tls = tls
        options.max_frame_bytes = 512
        options.max_in_flight_frames = 1
        layout = (iqstream.sample_layout.COMPLEX if complex_samples
                  else iqstream.sample_layout.REAL)
        sample_data = ([complex(i / 32, -i / 64) for i in range(count)]
                       if complex_samples else [i / 32 for i in range(count)])
        tags = []
        if count > 3:
            for value in (pmt.from_uint64(2**64 - 1), pmt.PMT_F):
                tag = gr.tag_t()
                tag.offset = 3
                tag.key = pmt.intern("marker")
                tag.value = value
                tag.srcid = pmt.intern("origin")
                tags.append(tag)
        vector_source = (blocks.vector_source_c(sample_data, False, 1, tags)
                         if complex_samples else
                         blocks.vector_source_f(sample_data, False, 1, tags))
        vector_sink = (blocks.vector_sink_c() if complex_samples
                       else blocks.vector_sink_f())
        if upload:
            source = iqstream.server_source(listener, "iq", layout, options)
            sink = iqstream.sink(listener.address(), "iq", layout, options)
        else:
            sink = iqstream.server_sink(listener, "iq", layout, options)
            source = iqstream.source(listener.address(), "iq", layout, options)
        sender, receiver = gr.top_block(), gr.top_block()
        sender.connect(vector_source, sink)
        receiver.connect(source, vector_sink)
        # Both schedulers run independently; sink.stop() waits for acceptance.
        receiver.start(max_noutput_items=3)
        sender.start(max_noutput_items=17)
        sender.wait()
        receiver.wait()
        self.assertEqual(list(vector_sink.data()), sample_data)
        self.assertEqual(len(vector_sink.tags()), len(tags))
        for original, received in zip(tags, vector_sink.tags()):
            self.assertEqual(original.offset, received.offset)
            self.assertTrue(pmt.equal(original.value, received.value))
            self.assertTrue(pmt.equal(original.srcid, received.srcid))
        for block in (source, sink):
            self.assertEqual(block.status().state, iqstream.session_state.COMPLETE,
                             block.status().message)
            self.assertEqual(block.status().next_sample, count)
        listener.shutdown()

    def test_roundtrips(self):
        for upload in (False, True):
            for complex_samples in (False, True):
                for count in (0, 1000):
                    with self.subTest(upload=upload, complex=complex_samples,
                                      count=count):
                        self.roundtrip(upload, complex_samples, count)

    def test_cancel_stalled_source(self):
        listener = iqstream.server("127.0.0.1:0")
        options = iqstream.stream_options()
        remote = iqstream.server_sink(listener, "iq",
                                      iqstream.sample_layout.REAL, options)
        source = iqstream.source(listener.address(), "iq",
                                 iqstream.sample_layout.REAL, options)
        sink = blocks.vector_sink_f()
        graph = gr.top_block()
        graph.connect(source, sink)
        graph.start()
        source.cancel()
        graph.wait()
        self.assertEqual(source.status().state, iqstream.session_state.CANCELLED)
        listener.shutdown()
        del remote

    def test_tls_and_mutual_tls(self):
        with tempfile.TemporaryDirectory() as temporary:
            folder = Path(temporary)
            key, certificate = folder / "key.pem", folder / "cert.pem"
            subprocess.run([
                "openssl", "req", "-x509", "-newkey", "ec", "-pkeyopt",
                "ec_paramgen_curve:P-256", "-nodes", "-days", "1",
                "-subj", "/CN=localhost", "-addext",
                "subjectAltName=DNS:localhost,IP:127.0.0.1",
                "-keyout", str(key), "-out", str(certificate),
            ], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            tls = iqstream.tls_options()
            tls.enabled = True
            tls.root_certificates = certificate.read_text()
            tls.certificate_chain = certificate.read_text()
            tls.private_key = key.read_text()
            for mutual in (False, True):
                tls.require_client_certificate = mutual
                for upload in (False, True):
                    self.roundtrip(upload, False, 1000, tls)

    def test_manual_stop_finishes_prefix(self):
        listener = iqstream.server("127.0.0.1:0")
        options = iqstream.stream_options()
        options.max_frame_bytes = 512
        options.max_in_flight_frames = 1
        source = iqstream.source(listener.address(), "iq",
                                 iqstream.sample_layout.REAL, options)
        sender = iqstream.server_sink(listener, "iq",
                                      iqstream.sample_layout.REAL, options)
        destination = blocks.vector_sink_f()
        receiving, sending = gr.top_block(), gr.top_block()
        receiving.connect(source, destination)
        sending.connect(blocks.vector_source_f([0.25] * 100, True), sender)
        receiving.start()
        sending.start()
        deadline = time.monotonic() + 5
        while sender.status().next_sample < 1000:
            self.assertLess(time.monotonic(), deadline)
            time.sleep(0.001)
        sending.stop()
        sending.wait()
        receiving.wait()
        self.assertEqual(source.status().state, iqstream.session_state.COMPLETE,
                         source.status().message)
        self.assertEqual(sender.status().state, iqstream.session_state.COMPLETE,
                         sender.status().message)
        self.assertEqual(len(destination.data()), sender.status().next_sample)
        listener.shutdown()

    def test_options_and_properties(self):
        options = iqstream.stream_options()
        options.properties = [("u64", pmt.from_uint64(2**64 - 1))]
        self.assertEqual(pmt.to_uint64(options.properties[0][1]), 2**64 - 1)
        options.max_in_flight_frames = 0
        with self.assertRaises(RuntimeError):
            iqstream.source("127.0.0.1:1", "iq", options=options)

    def test_grc_option_overrides(self):
        original = iqstream.stream_options()
        original.max_in_flight_frames = 3
        configured = iqstream.configured_options(
            original, profile=iqstream.metadata_profile.RUSTRADIO,
            loss=iqstream.loss_policy.ALLOW_GAPS)
        self.assertEqual(configured.profile, iqstream.metadata_profile.RUSTRADIO)
        self.assertEqual(configured.loss, iqstream.loss_policy.ALLOW_GAPS)
        self.assertEqual(configured.max_in_flight_frames, 3)
        self.assertEqual(original.profile, iqstream.metadata_profile.NATIVE)
        self.assertEqual(original.loss, iqstream.loss_policy.LOSSLESS)
        copied = iqstream.configured_options(configured)
        self.assertEqual(copied.profile, configured.profile)
        self.assertEqual(copied.loss, configured.loss)


if __name__ == "__main__":
    unittest.main()
