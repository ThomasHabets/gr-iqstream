/* SPDX-License-Identifier: GPL-3.0-or-later */
#ifndef INCLUDED_GR_IQSTREAM_PROTOCOL_H
#define INCLUDED_GR_IQSTREAM_PROTOCOL_H
#include "iq_stream.pb.h"
#include <gnuradio/iqstream/options.h>
#include <gnuradio/tags.h>
#include <google/protobuf/io/coded_stream.h>
#include <grpcpp/support/status.h>
#include <stdexcept>

namespace gr {
namespace iqstream {
namespace wire = rustradio::iq::v1;
constexpr size_t MAX_ENVELOPE_BYTES = 2 * 1024 * 1024;
constexpr size_t MAX_CONTROL_BYTES = 64 * 1024;
constexpr size_t MAX_TAGS = 4096;
constexpr size_t MAX_NODES = 16384;
constexpr size_t MAX_DEPTH = 32;
constexpr const char* METHOD = "/rustradio.iq.v1.IqStreaming/Stream";
constexpr const char* GAP_SAMPLES = "rustradio.iq.gap_samples";
constexpr const char* SAMPLE_INDEX = "rustradio.iq.sample_index";

class protocol_error : public std::runtime_error
{
public:
    protocol_error(grpc::StatusCode code, const std::string& text)
        : std::runtime_error(text), d_code(code)
    {
    }
    grpc::StatusCode code() const { return d_code; }

private:
    grpc::StatusCode d_code;
};
void require(bool condition,
             const std::string& message,
             grpc::StatusCode code = grpc::StatusCode::INVALID_ARGUMENT);
uint64_t advance(uint64_t value, uint64_t count);
bool valid_utf8(const std::string& value);
bool reserved_key(const std::string& key);
void validate_options(const stream_options& options, bool sending);
void validate_limits(const wire::Limits& limits);
wire::Encoding encoding(sample_layout layout);
wire::StreamDescription description(sample_layout layout, const stream_options& options);
std::vector<wire::TagKind> capabilities(const stream_options& options);
std::vector<wire::TagKind> download_capabilities();
void validate_description(const wire::StreamDescription& desc,
                          sample_layout layout,
                          const stream_options& options);
bool equal_description(const wire::StreamDescription& a,
                       const wire::StreamDescription& b);
wire::TagValue to_wire_value(const pmt::pmt_t& value, const stream_options& options);
pmt::pmt_t from_wire_value(const wire::TagValue& value, const stream_options& options);
wire::Tag to_wire_tag(const gr::tag_t& tag,
                      uint64_t local_origin,
                      uint64_t wire_origin,
                      const stream_options& options);
gr::tag_t
from_wire_tag(const wire::Tag& tag, uint64_t local_offset, const stream_options& options);
void validate_tags(const google::protobuf::RepeatedPtrField<wire::Tag>& tags,
                   const wire::StreamDescription& desc,
                   uint64_t first,
                   uint64_t count,
                   bool terminal = false);
void validate_frame(const wire::Frame& frame,
                    const wire::StreamDescription& desc,
                    uint64_t sequence,
                    uint64_t cursor,
                    uint64_t grant,
                    wire::LossPolicy policy);
void swap_components(std::string& bytes);
size_t inspect_wire(const std::string& bytes,
                    const google::protobuf::Descriptor* descriptor);
template <class T>
size_t decode(const std::string& bytes, T& message)
{
    const auto frame_bytes = inspect_wire(bytes, T::descriptor());
    google::protobuf::io::CodedInputStream input(
        reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size());
    input.SetRecursionLimit(256);
    input.SetTotalBytesLimit(MAX_ENVELOPE_BYTES);
    require(message.ParseFromCodedStream(&input) && input.ConsumedEntireMessage(),
            "invalid protobuf envelope");
    return frame_bytes;
}
} // namespace iqstream
} // namespace gr

#endif /* INCLUDED_GR_IQSTREAM_PROTOCOL_H */
