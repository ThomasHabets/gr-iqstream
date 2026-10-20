#!/usr/bin/python3
# SPDX-License-Identifier: MIT
"""Exercise browser-compatible protobuf WebSocket streams on the IQ listener."""
import importlib.util
from pathlib import Path
import struct
import subprocess
import tempfile
import unittest

import websocket
from gnuradio import blocks, gr, iqstream
from qa_observation import gated_source


class WebSocket(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temporary = tempfile.TemporaryDirectory()
        root = Path(__file__).resolve().parents[1]
        subprocess.run(["protoc", "-I", str(root / "proto"),
                        "--python_out=" + cls.temporary.name,
                        str(root / "proto/iq_stream.proto")], check=True)
        spec = importlib.util.spec_from_file_location(
            "iq_stream_pb2", Path(cls.temporary.name) / "iq_stream_pb2.py")
        cls.proto = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.proto)

    @classmethod
    def tearDownClass(cls):
        cls.temporary.cleanup()

    def connect(self, server, secure=False, **kwargs):
        ws = websocket.create_connection(
            ("wss://" if secure else "ws://") + server.address() + "/iq/v1/stream",
            subprotocols=["rustradio.iq.v1"], timeout=5, **kwargs)
        self.assertEqual(ws.getsubprotocol(), "rustradio.iq.v1")
        self.assertEqual(ws.getheaders()["access-control-allow-origin"], "*")
        return ws

    def open(self, ws, name, complex_samples=False, observe=False, fragmented=False):
        message = self.proto.ClientMessage()
        request = message.open
        request.protocol_version = 1
        request.limits.max_frame_bytes = 1024
        request.limits.max_in_flight_frames = 1
        request.loss_policy = 2 if observe else 1
        request.completion_mode = 1
        request.download.source = name
        request.download.accepted_encodings.add(component_type=1, byte_order=1,
                                                layout=2 if complex_samples else 1)
        request.download.accepted_tag_kinds.extend(value for value in self.proto.TagKind.values() if value)
        request.download.accept_tag_source_ids = True
        request.download.accept_terminal_tags = True
        payload = message.SerializeToString()
        if fragmented:
            middle = len(payload) // 2
            ws.send_frame(websocket.ABNF.create_frame(payload[:middle], websocket.ABNF.OPCODE_BINARY, fin=0))
            ws.send_frame(websocket.ABNF.create_frame(payload[middle:], websocket.ABNF.OPCODE_CONT))
        else:
            ws.send_binary(payload)
        response = self.receive(ws)
        self.assertEqual(response.WhichOneof("body"), "started", str(response))
        credit = self.proto.ClientMessage()
        credit.flow_control.send_limit = 1
        ws.send_binary(credit.SerializeToString())
        return response.started

    def receive(self, ws):
        payload = ws.recv()
        self.assertIsInstance(payload, bytes)
        result = self.proto.ServerMessage()
        result.ParseFromString(payload)
        return result

    def download(self, ws):
        data, tags, frames = bytearray(), [], 0
        while True:
            message = self.receive(ws)
            kind = message.WhichOneof("body")
            if kind == "frame":
                self.assertEqual(message.frame.sequence, frames)
                data.extend(message.frame.chunk.samples)
                tags.extend(message.frame.chunk.tags)
                frames += 1
                credit = self.proto.ClientMessage()
                credit.flow_control.send_limit = frames + 1
                ws.send_binary(credit.SerializeToString())
            elif kind == "end":
                complete = self.proto.ClientMessage()
                complete.complete.next_sequence = message.end.next_sequence
                complete.complete.next_sample = message.end.next_sample
                complete.complete.completion_mode = 1
                ws.send_binary(complete.SerializeToString())
                self.assertEqual(self.receive(ws).WhichOneof("body"), "complete")
                opcode, payload = ws.recv_data(control_frame=True)
                self.assertEqual(opcode, websocket.ABNF.OPCODE_CLOSE)
                self.assertEqual(struct.unpack("!H", payload[:2])[0], 1000)
                ws.shutdown()
                return bytes(data), tags
            else:
                self.fail(str(message))

    def roundtrip(self, complex_samples, tls=None, sslopt=None):
        server = iqstream.server("127.0.0.1:0", tls or iqstream.tls_options())
        options = iqstream.stream_options()
        options.sample_rate_hz = 48000
        options.max_frame_bytes = 1024
        layout = iqstream.sample_layout.COMPLEX if complex_samples else iqstream.sample_layout.REAL
        data = [complex(i / 32, -i / 64) for i in range(1000)] if complex_samples else [i / 32 for i in range(1000)]
        source = blocks.vector_source_c(data) if complex_samples else blocks.vector_source_f(data)
        sink = iqstream.server_sink(server, "iq", layout, options)
        graph = gr.top_block()
        graph.connect(source, sink)
        graph.start()
        ws = None
        try:
            ws = self.connect(server, secure=tls is not None, sslopt=sslopt or {})
            ws.ping(b"alive")
            self.assertEqual(ws.recv_data(control_frame=True), (websocket.ABNF.OPCODE_PONG, b"alive"))
            started = self.open(ws, "iq", complex_samples, fragmented=True)
            self.assertEqual(started.description.sample_rate_hz, 48000)
            received, tags = self.download(ws)
            expected = b"".join(struct.pack("<ff", value.real, value.imag) if complex_samples else struct.pack("<f", value) for value in data)
            self.assertEqual(received, expected)
            graph.wait()
            self.assertEqual(sink.status().state, iqstream.session_state.COMPLETE, sink.status().message)
        finally:
            if ws:
                ws.close()
            server.shutdown()
            graph.stop()
            graph.wait()

    def test_real_and_complex_streams(self):
        for complex_samples in (False, True):
            with self.subTest(complex_samples=complex_samples):
                self.roundtrip(complex_samples)

    def test_observation_cancel_and_reconnect(self):
        server = iqstream.server("127.0.0.1:0")
        server.register_all_streams(iqstream.metadata_profile.RUSTRADIO, default_sample_rate_hz=96000)
        source = gated_source(True, sample_rate_hz=None)
        primary = blocks.vector_sink_c()
        graph = gr.top_block()
        graph.connect(source, primary)
        graph.start()
        try:
            name = next(entry.name for entry in server.streams() if entry.origin.block_id == source.unique_id())
            ws = self.connect(server)
            started = self.open(ws, name, True, observe=True)
            self.assertEqual(started.description.sample_rate_hz, 96000)
            ws.close()
            ws = self.connect(server)
            started = self.open(ws, name, True, observe=True)
            self.assertEqual(started.description.sample_rate_hz, 96000)
            source.release.set()
            received, tags = self.download(ws)
            self.assertEqual(received, b"".join(struct.pack("<ff", float(i), 0) for i in range(64)))
            self.assertEqual([tag.sample_index for tag in tags], [7, 7])
            ws.close()
        finally:
            source.release.set()
            server.shutdown()
            graph.stop()
            graph.wait()

    def test_bad_protocol_and_text_messages(self):
        server = iqstream.server("127.0.0.1:0")
        try:
            invalid = websocket.WebSocket()
            try:
                with self.assertRaises(websocket.WebSocketBadStatusException) as error:
                    invalid.connect("ws://" + server.address() + "/iq/v1/stream", timeout=5)
                self.assertEqual(error.exception.status_code, 400)
            finally:
                invalid.shutdown()
            ws = self.connect(server)
            ws.send("invalid text")
            self.assertEqual(self.receive(ws).WhichOneof("body"), "failure")
            opcode, payload = ws.recv_data(control_frame=True)
            self.assertEqual(opcode, websocket.ABNF.OPCODE_CLOSE)
            self.assertEqual(struct.unpack("!H", payload[:2])[0], 1002)
            ws.shutdown()
        finally:
            server.shutdown()

    def test_wss_and_mutual_tls(self):
        key, certificate = Path(self.temporary.name) / "key.pem", Path(self.temporary.name) / "cert.pem"
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
            self.roundtrip(True, tls, {"ca_certs": str(certificate),
                                      "certfile": str(certificate), "keyfile": str(key)})

    def test_server_shutdown_closes_idle_websocket(self):
        server = iqstream.server("127.0.0.1:0")
        ws = self.connect(server)
        server.shutdown()
        with self.assertRaises(websocket.WebSocketConnectionClosedException):
            ws.recv()
        ws.close()


if __name__ == "__main__":
    unittest.main()
