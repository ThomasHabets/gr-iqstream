/* SPDX-License-Identifier: MIT */
#ifndef INCLUDED_GR_IQSTREAM_OPTIONS_H
#define INCLUDED_GR_IQSTREAM_OPTIONS_H
#include <gnuradio/iqstream/api.h>
#include <pmt/pmt.h>
#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace gr {
namespace iqstream {
enum class sample_layout { REAL, COMPLEX };
enum class loss_policy { LOSSLESS, ALLOW_GAPS };
enum class metadata_profile { NATIVE, RUSTRADIO };
enum class session_state {
    IDLE,
    OPENING,
    STREAMING,
    FINISHING,
    COMPLETE,
    FAILED,
    CANCELLED
};

/*! Connection security. Certificates and keys contain PEM text, not filenames.
 * An empty TLS root certificate field uses the gRPC system trust configuration.
 */
struct IQSTREAM_API tls_options {
    bool enabled = false;
    bool require_client_certificate = false;
    std::string root_certificates;
    std::string certificate_chain;
    std::string private_key;
};

/*! Fixed session options. The profile controls outgoing metadata only.
 * RUSTRADIO exports symbols as STRING and narrows doubles to FLOAT32.
 * Receivers accept all tag kinds and discard tags without a PMT mapping.
 * Properties retain their order here; wire property keys must be unique.
 */
struct IQSTREAM_API stream_options {
    double sample_rate_hz = 1.0;
    loss_policy loss = loss_policy::LOSSLESS;
    metadata_profile profile = metadata_profile::NATIVE;
    bool blocking = true;
    uint32_t max_frame_bytes = 256 * 1024;
    uint32_t max_in_flight_frames = 8;
    uint32_t handshake_timeout_ms = 5000;
    uint32_t shutdown_timeout_ms = 5000;
    uint32_t transfer_timeout_ms = 0;
    std::string source_id;
    std::optional<uint64_t> source_sample_offset;
    std::optional<int64_t> sample_zero_seconds;
    int32_t sample_zero_nanos = 0;
    std::vector<std::pair<std::string, pmt::pmt_t>> properties;
    tls_options tls;
    std::vector<std::pair<std::string, std::string>> grpc_metadata;
};

/*! A bounded snapshot, including the serialized original StreamDescription.
 * Complete means the destination accepted the stream and transport finished OK;
 * it does not promise downstream DSP completion or durable storage.
 */
struct IQSTREAM_API session_status {
    session_state state = session_state::IDLE;
    uint64_t next_sequence = 0;
    uint64_t next_sample = 0;
    uint64_t lost_samples = 0;
    uint64_t trailing_gap = 0;
    bool completion_uncertain = false;
    int grpc_status_code = 0;
    std::string message;
    double sample_rate_hz = 0;
    std::optional<uint64_t> source_sample_offset;
    std::string description_bytes;
};
} // namespace iqstream
} // namespace gr

#endif /* INCLUDED_GR_IQSTREAM_OPTIONS_H */
