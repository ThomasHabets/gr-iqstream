# gr-iqstream

A C++ GNU Radio OOT module targeting **GNU Radio 3.10.12 with the stream
observation API** (installed release 3.10.12.0). Implements the native gRPC `rustradio.iq.v1.IqStreaming/Stream`
protocol, with Python bindings and GNU Radio Companion blocks.

Each session carries one real `float` or complex `gr_complex` stream in
FLOAT32/little-endian encoding. Samples are bit-preserving: there is no scaling,
resampling, or implicit layout conversion. Both upload and download are supported.
WebSocket, SigMF adapters, integer/FLOAT64 sample ports, DURABLE completion,
terminal tags are outside this release's graph profile. Receivers advertise all
defined tag kinds, convert supported values to PMT and discard unsupported tags.

| Block | GNU Radio port | Network role |
| --- | --- | --- |
| `source` | Output | Client downloads a server source |
| `sink` | Input | Client uploads to a server destination |
| `server_source` | Output | Registered destination receives an upload |
| `server_sink` | Input | Registered source supplies a download |

## Build and install

Dependencies: GNU Radio 3.10.12 built with stream observation support, a C++17 compiler,
CMake >= 3.16.3, Protobuf >= 3.15 and `protoc`, gRPC C++ >= 1.51, Boost headers, OpenSSL, and threads.
Python/GRC builds also require the Python development package and pybind11 C++
headers/CMake package >= 2.12. The Python `pybind11` module is not needed.
The distribution packages supply build dependencies; build the modified GNU Radio
runtime separately and supply its prefix to CMake.

On Debian 13 these packages provide the build dependencies:

```sh
sudo apt-get install build-essential cmake ninja-build gnuradio gnuradio-dev \
  libprotobuf-dev protobuf-compiler libgrpc++-dev libssl-dev libboost-dev \
  pybind11-dev python3-dev python3-websocket python3-protobuf
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release \
  -DPython3_EXECUTABLE=/usr/bin/python3 -DPYTHON_EXECUTABLE=/usr/bin/python3 \
  -DCMAKE_PREFIX_PATH=/path/to/gnuradio-install
cmake --build build -j3
ctest --test-dir build --output-on-failure
sudo cmake --install build
sudo ldconfig
```

Use the same Python installation as GNU Radio. Override `CMAKE_INSTALL_PREFIX`
and, when necessary, `IQSTREAM_PYTHON_DIR` for a custom prefix. A custom prefix
must be on your Python, library, CMake and GRC search paths. Installation is not
performed by the presubmit runner: it tests a private staging prefix.

`-DENABLE_PYTHON=OFF` builds a C++-only library. Installed CMake consumers use
`find_package(gnuradio-iqstream CONFIG REQUIRED)` and link `gnuradio::iqstream`.
Public headers live under `gnuradio/iqstream`.

Run a local finite example from the build tree:

```sh
LD_LIBRARY_PATH="$PWD/build" PYTHONPATH="$PWD/build/python" \
  /usr/bin/python3 examples/roundtrip.py
```

`examples/iqstream_roundtrip.grc` exercises all four block types. For an
uninstalled build, set `GRC_BLOCKS_PATH="$PWD/grc"` and
`PYTHONPATH="$PWD/build/python"` and `LD_LIBRARY_PATH="$PWD/build"` before opening
it in Companion. The shared server
variable is passed to server blocks; client blocks accept either that variable
or a quoted address in their Server or Address field.
If a copy is already installed, reinstall after editing the GRC definitions and
restart Companion; GNU Radio 3.10.12 can load installed definitions after
`GRC_BLOCKS_PATH`, overriding them.

## Configuration and lifetime

```python
from gnuradio import iqstream

options = iqstream.stream_options()
options.sample_rate_hz = 48000
listener = iqstream.server("127.0.0.1:50051")
sending = iqstream.server_sink(listener, "audio",
                              iqstream.sample_layout.REAL, options)
receiving = iqstream.source(listener.address(), "audio",
                           iqstream.sample_layout.REAL, options)
```

Factories copy options. Sending resources and upload destinations require a
finite positive configured sample rate. Download clients learn the rate from
`status()` after negotiation; it does not automatically retune downstream blocks.
Resource identifiers are opaque strings, not filesystem paths.

Client blocks connect in `start()`. Server construction starts its listener;
server blocks register resources during construction, permitting negotiation
before their graph starts. One resource admits one session per block run. A
concurrent client gets ALREADY_EXISTS; a spent resource requires another run or
a newly constructed block. Start server graphs before reconnecting clients on
subsequent runs. There is no automatic reconnect, replay, retry or resume.

**Sending blocks finish on every `stop()`, including manual graph stop.** They
drain their bounded queue of already accepted samples and send End. Unconsumed
upstream GNU Radio items are outside this accepted prefix. Empty streams also
negotiate and complete. A server sender stopping before connection waits up to
its shutdown timeout for a client; without one, the resource closes without a
completed session. Use `cancel()` for an explicit abort. Stopping a receiving
block cancels an unfinished receive session. `listener.shutdown()` cancels all
remaining sessions and closes the listener.

GNU Radio's `stop()` return value is not a transfer-success report; inspect both
endpoints' status. COMPLETE requires final acknowledgement and gRPC OK.
ACCEPTED acknowledges entry into graph buffers, not completion of downstream DSP
or durable file storage. A missing acknowledgement after End leaves completion
uncertain. Already accepted samples are not rolled back on failure/cancellation.

`status()` exposes state, gRPC error code/message, submitted/received exclusive
cursors, negotiated rate/origin, cumulative loss, trailing loss and completion
uncertainty. `description_bytes` contains the complete original protobuf
StreamDescription (Python `bytes`), preserving optional presence and typed
properties. C++ can parse it using the installed `iq_stream.pb.h`; Python users
can generate bindings from the installed unmodified schema with `protoc`.

Default limits are 256 KiB per encoded frame and eight frames. Protocol ceilings
and decoder budgets are enforced on received wire bytes before protobuf object
allocation. Network callbacks and watchdogs run outside GNU Radio work functions.
There is one ordered writer per direction, bounded local acquisition/receive
queues, coalesced credits, and separate capacity for terminal controls. Receiver
credits return only after whole frames enter graph buffers and their frame
storage is released. Sample chunks are submitted promptly; credits flush at
half-window progress or within approximately 10 ms, with prompt one-frame credit.

The local acquisition queue adds at most a window of frames beyond the wire
window; transport and decoding copies add bounded overhead. PMT symbols use GNU
Radio's process-wide intern table, so applications should avoid indefinitely
introducing new symbol strings when long-term process memory matters.

Handshake and graceful-shutdown timeouts default to 5000 ms. Set
`handshake_timeout_ms`, `shutdown_timeout_ms`, or `transfer_timeout_ms` explicitly
as needed; zero transfer timeout leaves continuous streams without an overall
deadline. Sample compression defaults off.

## Tags and metadata

Wire indices are rebased to local absolute item offsets; duplicate tags and their
order at equal indices are retained. Tag keys are UTF-8 PMT symbols. Optional
wire source IDs become PMT symbols, including present empty strings. Default PMT
F/NIL source IDs mean absence; other source-ID types fail explicitly. A native
sender conservatively declares provenance support.

| Wire value | Native PMT mapping / export behavior |
| --- | --- |
| BOOL, INT64, UINT64 | Typed booleans and exact signed/unsigned integers |
| FLOAT32, FLOAT64 | PMT double; export as FLOAT64 |
| SYMBOL | PMT symbol; export as SYMBOL |
| STRING | PMT symbol; re-export as SYMBOL |
| BYTES | PMT blob/u8vector; export as BYTES |
| COMPLEX | PMT complex double |
| LIST | Proper PMT list; export proper lists as LIST |
| TUPLE | PMT tuple, preserving element order |
| PAIR | PMT pair; a resulting proper list exports as LIST |
| DICTIONARY | Ordered native dictionary; supported noncolliding keys only |
| NIL | PMT NIL |

Empty wire lists/dictionaries become PMT NIL and re-export as NIL. Native mapping
preserves values according to these documented conversions, not every original
wire kind. Dictionary BYTES keys are rejected because PMT dictionary lookup uses
identity rather than byte-content equality. STRING/SYMBOL keys that collide after
conversion cannot be represented. Unsupported received values, including JSON,
OPAQUE, unsupported dictionary keys,
and structures containing such values, cause the entire tag to be dropped. Sample
bytes, retained tag offsets and frame credits are unaffected. Malformed frames,
undeclared values and decoder resource-limit violations still fail the stream.
Immutable properties remain available in their original protobuf representation
through `status().description_bytes`, including unsupported values.

Download clients always advertise all 16 defined tag kinds. No metadata profile
selection or STRING conversion opt-in is required. In Companion, match Loss
Policy to the remote RustRadio sink: `blocking(true)` requires **Lossless**, while
`blocking(false)` requires **Allow gaps**. A TCP connection can exist even when
negotiation fails; failures are logged and retained in `status().message`.

The existing `stream_options.profile` applies only to outgoing PMT encoding.
`metadata_profile.RUSTRADIO` limits outgoing metadata to RustRadio's five scalar
kinds, exports symbols as STRING and narrows real metadata to FLOAT32. It rejects
structured values and provenance instead of stripping them. Incoming metadata
conversion is independent of this option. Other uniform vectors and custom PMTs
remain unsupported for export.

OPAQUE has separate codec negotiation: the current schema requires exact accepted
codec identifiers and forbids wildcards. Advertising the OPAQUE kind alone cannot
accept arbitrary codecs; that feature requires an upstream protocol/API change.
The schema is not modified here. Terminal tags remain unadvertised because GNU
Radio streaming ports have no item at EOF to attach them to.

## Synchronizing two streams

Use **IQ Stream Align Streams** in Companion, or
`iqstream.align_streams(itemsize0, itemsize1, tag_key0, tag_key1)` in Python/C++, between the two
sources and their downstream blocks. Each output retains its input's type;
Companion offers independent byte, short, integer, float and complex selections.

```python
from gnuradio import gr, iqstream

aligned = iqstream.align_streams(gr.sizeof_float, gr.sizeof_gr_complex)
graph.connect(real_source, (aligned, 0))
graph.connect(complex_source, (aligned, 1))
graph.connect((aligned, 0), real_destination)
graph.connect((aligned, 1), complex_destination)
```

Set **Stream 0 Sync Tag** and **Stream 1 Sync Tag** independently in Companion,
otherwise both default to `rustradio.iq.absolute_sample_index`. In Python, pass
`tag_key0="first.position", tag_key1="second.position"` as optional keyword
arguments. Each output uses its own input's configured tag name for retained and
newly inserted index anchors. Names must be nonempty and cannot be the reserved
`rustradio.iq.gap_samples` gap-count key.

The configured UINT64 sync tag anchors the absolute index of its associated sample. Indices advance by one per item. Samples before each
input's first anchor are discarded. For example, if inputs begin at indices 100
and 107, discard the first seven samples from the input at 100, then emit matching
pairs beginning at 107. The block does not add latency to the input at 107 or
invent missing samples.

Later absolute-index anchors and `rustradio.iq.gap_samples` tags trigger
realignment. An explicit index at a gap already includes that gap's missing
samples. Both inputs must use the same item timeline and sample rate; there is
no resampling. Timing values must be PMT UINT64; backwards/conflicting anchors
and index overflow fail explicitly. The final valid UINT64_MAX sample is allowed.

Retained tags keep their values, source IDs and duplicate order on their own
output, with offsets rebased to the output items. Tags on discarded samples are
discarded too, including persistent-state updates. New absolute-index tags are
inserted when needed so both outputs retain an accurate timeline after dropping
samples. The block uses scheduler buffers and ends when either input ends; it
does not emit an unmatched tail. Without an initial anchor it discards that
input while waiting for one and cannot produce aligned output.
`examples/align_streams.grc` demonstrates real and complex inputs starting at
indices 100 and 102, producing eight aligned pairs from ten input items each.

## Gaps

LOSSLESS is the default: acquisition waits for bounded queue capacity. To drop
newly acquired samples under pressure, set both
`options.loss = iqstream.loss_policy.ALLOW_GAPS` and `options.blocking = False`
on the sender, and request ALLOW_GAPS on the receiver. A nonblocking server sink
discards input before connection; the session begins at the current graph
position, reported by `source_sample_offset`.

Losses affect only unsent samples/tags. Exact counts advance the wire timeline;
there is no zero filling. Receivers attach these U64 markers, in order, to the
first retained sample after a gap:

1. `rustradio.iq.gap_samples`: the missing sample count.
2. `rustradio.iq.sample_index`: the retained sample's absolute session index.

Both keys are reserved and rejected as ordinary input/wire tags. Markers mean
persistent tag state is unknown; downstream applications must handle the
discontinuity. Retained samples occupy consecutive local item offsets. Adjacent
losses coalesce, gaps consume credit, and final losses precede End. Status retains
cumulative and trailing losses, including gaps before EOF or with no retained
samples. Unknown loss counts and gaps in LOSSLESS sessions fail.

## Presubmits and upstream ownership

Run every check before submitting:

```sh
/usr/bin/python3 tools/presubmit.py --rustradio ../rustradio
```

Additional check dependencies are `clang-format`, OpenSSL CLI, Rust >= 1.98 with
Cargo/rustfmt, and the GNU Radio Python/GRC installation. The runner verifies
formatting, schema identity, protocol/state-machine checks, native transport and
scheduler flowgraphs, TLS/mutual TLS, gaps, cancellation/timeouts, GRC generation
and execution, RustRadio interoperability, staged C++/Python installation, and
address/undefined-behavior sanitizers. Missing dependencies or the upstream
checkout fail presubmits; no checks are silently skipped. CI uses Debian 13's
GNU Radio 3.10.12 and the matching pinned RustRadio revision.

The checked-in schema is copied unchanged from `../rustradio/proto/iq_stream.proto`.
`proto/UPSTREAM` records its checksum and origin. Schema or dependency changes
belong upstream: request the change, then copy the approved upstream schema
verbatim. The module does not vendor modified dependencies.

Code is MIT licensed; the copied schema retains RustRadio's MIT license,
included as `proto/LICENSE.rustradio`.

## Automatic discovery and downloads

Build against GNU Radio with the stream observation API described in
`gnuradio/stream_observer.h`. Configure with `CMAKE_PREFIX_PATH` pointing at that
GNU Radio installation. For presubmits, use:

```sh
/usr/bin/python3 tools/presubmit.py --rustradio ../rustradio \
    --gnuradio-prefix /path/to/gnuradio-install
```

Enable **Observe All Streams** on the IQ Stream Server variable in Companion,
or enable it on an existing server in Python:

```python
server = iqstream.server("127.0.0.1:50051")
server.register_all_streams()
# Start the application's ordinary flowgraph without adding IQ sinks.
for entry in server.streams():
    print(entry.name, entry.layout, entry.sample_rate_hz, entry.origin)
```

Automatic discovery is process-wide and includes subsequently started graphs.
It offers scalar float32 and complex64 outputs backed by supported host buffers.
`gr.streams()` also lists other item types, vectors, unknown types and unsupported
buffers, with the originating block ID and output port. Typed built-in and
Python blocks supply format information; legacy size-only ports can remain
unknown. There is no format guess based on byte size.

For RustRadio clients, call
`server.register_all_streams(iqstream.metadata_profile.RUSTRADIO)` or select
**RustRadio** as the server's Observation Metadata Profile. The default native
profile declares GNU Radio's complete supported metadata and provenance.
The RustRadio profile retains its existing restrictions: structured tag values
(such as GNU Radio `rx_time` tuples) and nondefault provenance are rejected.

Clients use the advertised name as their download resource and must request
`ALLOW_GAPS`. Downloads start at the current producer position, without replay.
Slow or disconnected clients do not apply backpressure to the original graph.
Bounded observer queues drop batches and export exact gap counts, including
trailing gaps. Network work runs outside scheduler threads. One active download
is allowed per automatic resource; subsequent sessions start at their own current
position. Server shutdown and client cancellation detach observations without
stopping the application graph.

Default automatic resource names include the GRC block ID (or runtime block name)
and an explicit alias when available, followed by the numeric block ID, output
port and graph-run stream ID. For example: `gr/blocks_throttle_0 [Receiver]/42/0/7`.
GRC supplies textual IDs automatically; hand-written flowgraphs can call
`block.set_stream_block_id("receiver_0")` before starting. Stream-description properties also carry the producer
name/alias, ID, port and type information. Manually registered resources retain
their names; colliding automatic names receive a deterministic suffix. Restarting
or reconfiguring the scheduler creates new stream IDs, so clients should list
streams again. Producer completion ends its download; producer failures fail the
observation session.

The `rustradio.iq.v1.IqStreaming/ListStreams` unary RPC lists available downloads,
including manually registered sinks. It reports encoding, optional rate and
completion state. Unknown sample rates are omitted in the catalogue and encoded
as zero in stream descriptions. Catalogue responses enforce the upstream entry
and byte limits. No additional numeric or string wire formats are advertised.

The same listener serves `GET /iq/v1/streams` over HTTP/1.1 (HTTPS when TLS is
configured), so browsers can discover downloads without a gRPC client:

```sh
curl http://127.0.0.1:50051/iq/v1/streams
```

The response is a JSON array matching RustRadio's HTTP catalogue. Each object
contains `name`, `messages` (false for these numeric streams), `encoding` with
numeric `component_type`, `byte_order` and `layout`, `sample_rate_hz` (null when
unknown), and `ended`. Names and entries match the gRPC `ListStreams` catalogue;
upload-only resources are excluded. Automatic outputs appear when **Observe All
Streams** is enabled. Responses include `Access-Control-Allow-Origin: *`;
`OPTIONS` preflight permits `GET`, `HEAD` and `Content-Type`. TLS and mutual TLS
settings apply to both HTTP and gRPC on this port. No extra listener configuration
is required. HTTP requests have bounded headers, connections and response sizes,
and a five-second request deadline; gRPC streams keep their existing deadlines.

Browser downloads use a WebSocket upgrade at `/iq/v1/stream` on the same port
(`ws://` normally, `wss://` with TLS). Clients must negotiate the
`rustradio.iq.v1` subprotocol. Each binary WebSocket message is exactly one
protobuf `ClientMessage` or `ServerMessage`, without a length prefix. The existing
Open/Started, cumulative credit, frame, End/Complete and cancellation rules apply.
Text messages are rejected. Fragmented binary messages and ping/pong are supported;
message sizes and session counts remain bounded. Both manually registered
resources and automatic observations are available, including real float32 and
complex64 streams for the RustRadio iq-waterfall viewer. Set a positive sample
rate on the sink or producer output before starting the graph: the waterfall
requires a known rate even though the transport permits unknown rates.

For automatic observations whose producer has no declared sample rate, set
**Default Sample Rate (Hz)** on the IQ Stream Server variable, or use:

```python
server.register_all_streams(
    iqstream.metadata_profile.RUSTRADIO, default_sample_rate_hz=48000)
```

The default is advertised in `server.streams()`, gRPC `ListStreams`, HTTP
`/iq/v1/streams`, and the description negotiated for gRPC and WebSocket downloads.
An explicit producer output rate takes precedence. Manually registered sinks keep
their configured rates. Zero (the default) leaves missing rates unknown; other
values must be finite and positive. Updating the default affects listings and new
sessions; an active stream retains its negotiated rate. This setting describes
the samples and does not resample or throttle the producer.
