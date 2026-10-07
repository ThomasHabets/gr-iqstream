#!/usr/bin/python3
"""Download a finite GNU Radio stream through a shared gRPC listener."""
from gnuradio import blocks, gr, iqstream


def main():
    listener = iqstream.server("127.0.0.1:0")
    options = iqstream.stream_options()
    options.sample_rate_hz = 48000
    data = [i / 64 for i in range(1000)]
    sending = gr.top_block()
    outgoing = iqstream.server_sink(listener, "audio",
                                    iqstream.sample_layout.REAL, options)
    sending.connect(blocks.vector_source_f(data), outgoing)
    receiving = gr.top_block()
    incoming = iqstream.source(listener.address(), "audio",
                                iqstream.sample_layout.REAL, options)
    collected = blocks.vector_sink_f()
    receiving.connect(incoming, collected)
    receiving.start()
    sending.start()
    sending.wait()
    receiving.wait()
    assert list(collected.data()) == data
    assert incoming.status().state == iqstream.session_state.COMPLETE
    assert outgoing.status().state == iqstream.session_state.COMPLETE
    print(f"Received {len(collected.data())} samples at "
          f"{incoming.status().sample_rate_hz:g} Hz")
    listener.shutdown()


if __name__ == "__main__":
    main()
