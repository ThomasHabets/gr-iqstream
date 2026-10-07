#!/usr/bin/python3
"""Receive a RustRadio resource using its explicitly selected scalar profile."""
import argparse
from gnuradio import blocks, gr, iqstream


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("address")
    parser.add_argument("resource")
    parser.add_argument("--real", action="store_true")
    args = parser.parse_args()
    options = iqstream.stream_options()
    options.profile = iqstream.metadata_profile.RUSTRADIO
    layout = (iqstream.sample_layout.REAL if args.real
              else iqstream.sample_layout.COMPLEX)
    source = iqstream.source(args.address, args.resource, layout, options)
    destination = (blocks.vector_sink_f() if args.real else blocks.vector_sink_c())
    graph = gr.top_block()
    graph.connect(source, destination)
    graph.run()
    status = source.status()
    if status.state != iqstream.session_state.COMPLETE:
        raise RuntimeError(status.message)
    print(f"Received {len(destination.data())} samples at "
          f"{status.sample_rate_hz:g} Hz")


if __name__ == "__main__":
    main()
