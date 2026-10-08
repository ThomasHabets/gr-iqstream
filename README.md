# gr-iqstream

A C++ GNU Radio OOT module targeting **GNU Radio 3.10.12** (installed release
3.10.12.0). Implements the native gRPC `rustradio.iq.v1.IqStreaming/Stream`
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

Dependencies: GNU Radio 3.10.12 development/runtime packages, a C++17 compiler,
CMake >= 3.16.3, Protobuf >= 3.15 and `protoc`, gRPC C++ >= 1.51, and threads.
Python/GRC builds also require the Python development package and pybind11 C++
headers/CMake package >= 2.12. The Python `pybind11` module is not needed.
All required dependencies were available in the target workspace.

On Debian 13 these packages provide the build dependencies:

```sh
sudo apt-get install build-essential cmake ninja-build gnuradio gnuradio-dev \
  libprotobuf-dev protobuf-compiler libgrpc++-dev pybind11-dev python3-dev
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release \
  -DPython3_EXECUTABLE=/usr/bin/python3 -DPYTHON_EXECUTABLE=/usr/bin/python3
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
