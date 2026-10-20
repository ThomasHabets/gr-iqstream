#!/usr/bin/python3
# SPDX-License-Identifier: MIT
"""Expose ordinary flowgraph outputs without adding IQ sinks."""
from gnuradio import blocks, gr, iqstream

server = iqstream.server("127.0.0.1:50051")
server.register_all_streams()
graph = gr.top_block()
source = blocks.vector_source_c([1 + 1j, -1 - 1j], repeat=True)
throttle = blocks.throttle(gr.sizeof_gr_complex, 48000)
sink = blocks.null_sink(gr.sizeof_gr_complex)
graph.connect(source, throttle, sink)
throttle.set_output_stream_sample_rate(0, 48000)
graph.start()
try:
    for stream in server.streams():
        print(stream.name, stream.origin.block_id, stream.origin.output_port)
    input("Serving on 127.0.0.1:50051; press Enter to stop\n")
finally:
    server.shutdown()
    graph.stop()
    graph.wait()
