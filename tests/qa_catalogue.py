#!/usr/bin/python3
# SPDX-License-Identifier: MIT
"""HTTP catalogue compatibility, browser CORS and shared-listener security."""
import http.client
import json
from pathlib import Path
import socket
import ssl
import subprocess
import tempfile
import time
import unittest

from gnuradio import blocks, gr, iqstream


class Catalogue(unittest.TestCase):
    def request(self, server, method="GET", path="/iq/v1/streams", context=None):
        connection_type = http.client.HTTPSConnection if context else http.client.HTTPConnection
        kwargs = {"context": context} if context else {}
        connection = connection_type(server.address(), timeout=5, **kwargs)
        try:
            connection.request(method, path, headers={
                "Origin": "http://localhost:8080",
                "Access-Control-Request-Method": "GET",
                "Access-Control-Request-Headers": "content-type",
            })
            response = connection.getresponse()
            self.assertEqual(response.getheader("Access-Control-Allow-Origin"), "*")
            return response.status, dict(response.getheaders()), response.read()
        finally:
            connection.close()

    def test_manual_catalogue_and_cors(self):
        server = iqstream.server("127.0.0.1:0")
        other = iqstream.server("127.0.0.1:0")
        options = iqstream.stream_options()
        options.sample_rate_hz = 48000
        real = iqstream.server_sink(server, 'a-"real\\λ', iqstream.sample_layout.REAL, options)
        options.sample_rate_hz = 0
        complex_sink = iqstream.server_sink(server, "z-complex", iqstream.sample_layout.COMPLEX, options)
        upload = iqstream.server_source(server, "upload", iqstream.sample_layout.REAL, options)
        try:
            status, headers, body = self.request(server)
            self.assertEqual(status, 200)
            self.assertEqual(headers["Content-Type"], "application/json")
            self.assertEqual(json.loads(body), [
                {"name": 'a-"real\\λ', "messages": False, "sample_rate_hz": 48000,
                 "ended": False, "encoding": {"component_type": 1, "byte_order": 1, "layout": 1}},
                {"name": "z-complex", "messages": False, "sample_rate_hz": None,
                 "ended": False, "encoding": {"component_type": 1, "byte_order": 1, "layout": 2}},
            ])
            self.assertEqual(json.loads(self.request(other)[2]), [])
            self.assertEqual(self.request(server, path="/iq/v1/streams?unused=1")[2], body)
            status, headers, head_body = self.request(server, "HEAD")
            self.assertEqual(status, 200)
            self.assertEqual(int(headers["Content-Length"]), len(body))
            self.assertEqual(head_body, b"")
            status, headers, preflight_body = self.request(server, "OPTIONS")
            self.assertEqual(status, 204)
            self.assertIn("GET", headers["Access-Control-Allow-Methods"])
            self.assertIn("content-type", headers["Access-Control-Allow-Headers"].lower())
            self.assertEqual(preflight_body, b"")
            self.assertEqual(self.request(server, "POST")[0], 405)
            self.assertEqual(self.request(server, path="/missing")[0], 404)
        finally:
            server.shutdown()
            other.shutdown()

    def test_automatic_catalogue(self):
        server = iqstream.server("127.0.0.1:0")
        server.register_all_streams(default_sample_rate_hz=96000)
        graph = gr.top_block()
        source = blocks.vector_source_f([1, 2, 3], repeat=True)
        source.set_output_stream_sample_rate(0, 48000)
        throttle = blocks.throttle(gr.sizeof_float, 48000)
        sink = blocks.null_sink(gr.sizeof_float)
        graph.connect(source, throttle, sink)
        graph.start()
        try:
            entries = json.loads(self.request(server)[2])
            local = server.streams()
            self.assertEqual([entry["name"] for entry in entries], [entry.name for entry in local])
            name = next(entry.name for entry in local if entry.origin.block_id == source.unique_id())
            entry = next(entry for entry in entries if entry["name"] == name)
            self.assertEqual(entry["sample_rate_hz"], 48000)
            self.assertEqual(entry["encoding"]["layout"], 1)
            fallback = next(entry.name for entry in local if entry.origin.block_id == throttle.unique_id())
            self.assertEqual(next(entry for entry in entries if entry["name"] == fallback)["sample_rate_hz"], 96000)
            server.register_all_streams()  # Zero resets the fallback to unknown.
            entries = json.loads(self.request(server)[2])
            self.assertIsNone(next(entry for entry in entries if entry["name"] == fallback)["sample_rate_hz"])
            self.assertEqual(next(entry for entry in entries if entry["name"] == name)["sample_rate_hz"], 48000)
        finally:
            graph.stop()
            graph.wait()
            server.shutdown()

    def test_tls_and_mutual_tls(self):
        with tempfile.TemporaryDirectory() as temporary:
            key, certificate = Path(temporary) / "key.pem", Path(temporary) / "cert.pem"
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
                server = iqstream.server("127.0.0.1:0", tls)
                context = ssl.create_default_context(cafile=str(certificate))
                # Browsers offer both; gRPC offers h2 alone.
                context.set_alpn_protocols(["h2", "http/1.1"])
                try:
                    if mutual:
                        with self.assertRaises((ssl.SSLError, OSError)):
                            self.request(server, context=context)
                        context.load_cert_chain(str(certificate), str(key))
                    self.assertEqual(json.loads(self.request(server, context=context)[2]), [])
                finally:
                    server.shutdown()

    def test_shutdown_while_accepting_requests(self):
        for _ in range(20):
            server = iqstream.server("127.0.0.1:0")
            host, port = server.address().rsplit(":", 1)
            with socket.create_connection((host, int(port)), timeout=5) as connection:
                connection.sendall(b"GET /iq/v1/streams HTTP/1.1\r\nHost: localhost\r\n\r\n")
                start = time.monotonic()
                server.shutdown()
                self.assertLess(time.monotonic() - start, 2)

    def test_partial_handshake_does_not_block_catalogue_or_shutdown(self):
        server = iqstream.server("127.0.0.1:0")
        host, port = server.address().rsplit(":", 1)
        with socket.create_connection((host, int(port)), timeout=5) as stalled:
            stalled.sendall(b"GET /iq/v1/streams HTTP/1.1\r\n")
            self.assertEqual(self.request(server)[0], 200)
            start = time.monotonic()
            server.shutdown()
            self.assertLess(time.monotonic() - start, 2)
            self.assertEqual(stalled.recv(1), b"")


if __name__ == "__main__":
    unittest.main()
