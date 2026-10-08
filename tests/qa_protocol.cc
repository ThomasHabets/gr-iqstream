/* SPDX-License-Identifier: GPL-3.0-or-later */
#include "session.h"
#include "test.h"
#include <cmath>
#include <limits>

using namespace gr::iqstream;
void test_protocol()
{
    stream_options options;
    auto desc = description(sample_layout::REAL, options);
    CHECK(desc.encoding().layout() == wire::SAMPLE_LAYOUT_REAL);
    auto other = desc;
    other.mutable_encoding()->clear_layout();
    rejects([&] { validate_description(other, sample_layout::REAL, options); });
    other = desc;
    other.set_sample_rate_hz(std::numeric_limits<double>::infinity());
    rejects([&] { validate_description(other, sample_layout::REAL, options); });
    other = desc;
    other.set_uses_terminal_tags(true);
    rejects([&] { validate_description(other, sample_layout::REAL, options); },
            grpc::StatusCode::UNIMPLEMENTED);
    wire::Limits limits;
    rejects([&] { validate_limits(limits); });
    limits.set_max_frame_bytes(1);
    limits.set_max_in_flight_frames(1);
    validate_limits(limits);
    rejects([&] { advance(UINT64_MAX, 1); });
    CHECK(valid_utf8("hello\xc3\xa5"));
    CHECK(!valid_utf8("\xc0\x80"));
    CHECK(!valid_utf8("\xed\xa0\x80"));
    CHECK(!valid_utf8("\xf4\x90\x80\x80"));
    wire::Frame frame;
    frame.set_sequence(0);
    frame.mutable_chunk()->set_sample_count(1);
    frame.mutable_chunk()->set_samples(std::string("\x00\x00\x80\x3f", 4));
    validate_frame(frame, desc, 0, 0, 1, wire::LOSS_POLICY_LOSSLESS);
    rejects([&] { validate_frame(frame, desc, 1, 0, 2, wire::LOSS_POLICY_LOSSLESS); });
    rejects([&] { validate_frame(frame, desc, 0, 0, 0, wire::LOSS_POLICY_LOSSLESS); });
    frame.mutable_chunk()->set_samples("x");
    rejects([&] { validate_frame(frame, desc, 0, 0, 1, wire::LOSS_POLICY_LOSSLESS); });
    frame.mutable_gap()->set_sample_count(5);
    rejects([&] { validate_frame(frame, desc, 0, 0, 1, wire::LOSS_POLICY_LOSSLESS); });
    validate_frame(frame, desc, 0, 0, 1, wire::LOSS_POLICY_ALLOW_GAPS);
    wire::TagValue v;
    rejects([&] { from_wire_value(v, options); });
    v.set_uint64_value(UINT64_MAX);
    CHECK(pmt::to_uint64(from_wire_value(v, options)) == UINT64_MAX);
    v.set_int64_value(INT64_MIN);
    CHECK(pmt::to_long(from_wire_value(v, options)) == INT64_MIN);
    v.set_bool_value(false);
    CHECK(from_wire_value(v, options) == pmt::PMT_F);
    v.set_string_value("");
    CHECK(pmt::symbol_to_string(from_wire_value(v, options)).empty());
    v.mutable_list_value();
    CHECK(from_wire_value(v, options) == pmt::PMT_NIL);
    CHECK(to_wire_value(pmt::PMT_NIL, options).has_nil_value());
    auto tuple = pmt::make_tuple(pmt::from_uint64(UINT64_MAX), pmt::from_double(0.125));
    auto t = from_wire_value(to_wire_value(tuple, options), options);
    CHECK(pmt::is_tuple(t) && pmt::to_uint64(pmt::tuple_ref(t, 0)) == UINT64_MAX);
    CHECK(pmt::to_double(pmt::tuple_ref(t, 1)) == 0.125);
    auto pair = pmt::cons(pmt::intern("x"), pmt::from_long(42));
    CHECK(to_wire_value(pair, options).has_pair_value());
    auto list = pmt::list2(pmt::from_long(1), pmt::from_long(2));
    CHECK(to_wire_value(list, options).has_list_value());
    v.Clear();
    auto* e = v.mutable_dictionary_value()->add_entries();
    e->mutable_key()->set_string_value("same");
    e->mutable_value()->set_bool_value(false);
    e = v.mutable_dictionary_value()->add_entries();
    e->mutable_key()->set_symbol_value("same");
    e->mutable_value()->set_bool_value(true);
    rejects([&] { from_wire_value(v, options); }, grpc::StatusCode::UNIMPLEMENTED);
    options.properties = {
        { "nan", pmt::from_double(std::numeric_limits<double>::quiet_NaN()) }
    };
    desc = description(sample_layout::REAL, options);
    other = desc;
    std::reverse(other.mutable_tag_kinds()->begin(), other.mutable_tag_kinds()->end());
    CHECK(equal_description(desc, other));
    other.set_source_sample_offset(0);
    CHECK(!equal_description(desc, other));
    desc.mutable_sample_zero_time()->set_seconds(253402300800LL);
    rejects([&] { validate_description(desc, sample_layout::REAL, options); });
    wire::ClientMessage m, parsed;
    m.mutable_open()->set_protocol_version(1);
    auto bytes = m.SerializeAsString();
    // Repeated message fields merge rather than requiring canonical encoding.
    decode(bytes + bytes, parsed);
    CHECK(parsed.open().protocol_version() == 1);
    decode(std::string("\x0a\x02\x08\x01\x0a\x02\x08\x02", 8), parsed);
    CHECK(parsed.open().protocol_version() == 2);
    rejects([&] { decode(std::string(MAX_CONTROL_BYTES + 1, '\0'), parsed); });
    wire::TagValue nested;
    nested.mutable_nil_value();
    for (size_t i = 0; i < MAX_DEPTH; ++i) {
        wire::TagValue parent;
        *parent.mutable_tuple_value()->add_values() = nested;
        nested = parent;
    }
    rejects([&] { from_wire_value(nested, options); },
            grpc::StatusCode::RESOURCE_EXHAUSTED);
    wire::ServerMessage envelope;
    auto* chunk = envelope.mutable_frame()->mutable_chunk();
    chunk->set_sample_count(1);
    chunk->set_samples("1234");
    auto* tag = chunk->add_tags();
    tag->set_key("depth");
    *tag->mutable_value() = nested;
    wire::ServerMessage out;
    rejects([&] { decode(envelope.SerializeAsString(), out); },
            grpc::StatusCode::RESOURCE_EXHAUSTED);
    // Unknown fields count towards the original encoded frame size.
    auto raw_frame = frame.SerializeAsString();
    raw_frame += std::string("\xa0\x06\x01", 3);
    std::string wrapped = "\x12";
    CHECK(raw_frame.size() < 128);
    wrapped.push_back(static_cast<char>(raw_frame.size()));
    wrapped += raw_frame;
    CHECK(decode(wrapped, parsed) == raw_frame.size());
}
int main()
{
    try {
        test_protocol();
        std::cout << "protocol validation passed\n";
    } catch (const std::exception& e) {
        std::cerr << e.what() << '\n';
        return 1;
    }
}
