/* SPDX-License-Identifier: GPL-3.0-or-later */
#include "protocol.h"
#include <google/protobuf/descriptor.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <google/protobuf/wire_format_lite.h>
#include <algorithm>
#include <climits>
#include <cmath>
#include <cstring>
#include <limits>
#include <set>

namespace gr {
namespace iqstream {
void require(bool condition, const std::string& message, grpc::StatusCode code)
{
    if (!condition)
        throw protocol_error(code, message);
}
uint64_t advance(uint64_t value, uint64_t count)
{
    require(count <= UINT64_MAX - value, "sample/frame counter overflow");
    return value + count;
}
bool reserved_key(const std::string& key)
{
    return key == GAP_SAMPLES || key == SAMPLE_INDEX;
}
bool valid_utf8(const std::string& s)
{
    size_t i = 0;
    while (i < s.size()) {
        uint32_t c = static_cast<unsigned char>(s[i++]);
        if (c < 128)
            continue;
        unsigned n;
        uint32_t minimum;
        if (c >= 0xc2 && c <= 0xdf) {
            n = 1;
            minimum = 0x80;
            c &= 0x1f;
        } else if (c >= 0xe0 && c <= 0xef) {
            n = 2;
            minimum = 0x800;
            c &= 0x0f;
        } else if (c >= 0xf0 && c <= 0xf4) {
            n = 3;
            minimum = 0x10000;
            c &= 7;
        } else
            return false;
        if (n > s.size() - i)
            return false;
        for (unsigned j = 0; j < n; ++j) {
            auto b = static_cast<unsigned char>(s[i++]);
            if ((b & 0xc0) != 0x80)
                return false;
            c = (c << 6) | (b & 0x3f);
        }
        if (c < minimum || c > 0x10ffff || (c >= 0xd800 && c <= 0xdfff))
            return false;
    }
    return true;
}
void validate_limits(const wire::Limits& l)
{
    require(l.max_frame_bytes() > 0 && l.max_frame_bytes() <= 1024 * 1024 &&
                l.max_in_flight_frames() > 0 && l.max_in_flight_frames() <= 64,
            "invalid frame/window limits");
}
void validate_options(const stream_options& o, bool sending)
{
    wire::Limits l;
    l.set_max_frame_bytes(o.max_frame_bytes);
    l.set_max_in_flight_frames(o.max_in_flight_frames);
    validate_limits(l);
    require(o.handshake_timeout_ms && o.shutdown_timeout_ms, "timeouts must be positive");
    require(o.loss == loss_policy::LOSSLESS || o.loss == loss_policy::ALLOW_GAPS,
            "invalid loss policy");
    if (sending)
        require(o.profile == metadata_profile::NATIVE ||
                    o.profile == metadata_profile::RUSTRADIO,
                "invalid outgoing metadata profile");
    if (sending) {
        require(std::isfinite(o.sample_rate_hz) && o.sample_rate_hz > 0,
                "invalid sample rate");
        require(o.blocking || o.loss == loss_policy::ALLOW_GAPS,
                "nonblocking acquisition requires ALLOW_GAPS");
    }
}
wire::Encoding encoding(sample_layout l)
{
    require(l == sample_layout::REAL || l == sample_layout::COMPLEX,
            "invalid sample layout");
    wire::Encoding e;
    e.set_component_type(wire::COMPONENT_TYPE_FLOAT32);
    e.set_byte_order(wire::BYTE_ORDER_LITTLE_ENDIAN);
    e.set_layout(l == sample_layout::REAL ? wire::SAMPLE_LAYOUT_REAL
                                          : wire::SAMPLE_LAYOUT_COMPLEX);
    return e;
}
std::vector<wire::TagKind> capabilities(const stream_options& o)
{
    if (o.profile == metadata_profile::RUSTRADIO)
        return { wire::TAG_KIND_FLOAT32,
                 wire::TAG_KIND_BOOL,
                 wire::TAG_KIND_INT64,
                 wire::TAG_KIND_UINT64,
                 wire::TAG_KIND_STRING };
    std::vector<wire::TagKind> kinds = {
        wire::TAG_KIND_FLOAT32, wire::TAG_KIND_FLOAT64, wire::TAG_KIND_BOOL,
        wire::TAG_KIND_INT64,   wire::TAG_KIND_UINT64,  wire::TAG_KIND_BYTES,
        wire::TAG_KIND_SYMBOL,  wire::TAG_KIND_COMPLEX, wire::TAG_KIND_LIST,
        wire::TAG_KIND_TUPLE,   wire::TAG_KIND_PAIR,    wire::TAG_KIND_DICTIONARY,
        wire::TAG_KIND_NIL
    };
    return kinds;
}
std::vector<wire::TagKind> download_capabilities()
{
    std::vector<wire::TagKind> kinds;
    for (int kind = 1; kind <= wire::TagKind_MAX; ++kind)
        if (wire::TagKind_IsValid(kind))
            kinds.push_back(static_cast<wire::TagKind>(kind));
    return kinds;
}
namespace {
struct value_budget {
    size_t nodes = 0;
    void enter(size_t depth)
    {
        require(depth <= MAX_DEPTH && ++nodes <= MAX_NODES,
                "tag value depth/node limit",
                grpc::StatusCode::RESOURCE_EXHAUSTED);
    }
};
wire::TagValue
encode_value(const pmt::pmt_t& v, const stream_options& o, value_budget& b, size_t depth)
{
    b.enter(depth);
    require(static_cast<bool>(v), "null PMT pointer");
    wire::TagValue result;
    const bool rust = o.profile == metadata_profile::RUSTRADIO;
    if (pmt::is_bool(v))
        result.set_bool_value(pmt::to_bool(v));
    else if (pmt::is_uint64(v))
        result.set_uint64_value(pmt::to_uint64(v));
    else if (pmt::is_integer(v))
        result.set_int64_value(pmt::to_long(v));
    else if (pmt::is_real(v)) {
        if (rust)
            result.set_float32_value(static_cast<float>(pmt::to_double(v)));
        else
            result.set_float64_value(pmt::to_double(v));
    } else if (pmt::is_symbol(v)) {
        const auto s = pmt::symbol_to_string(v);
        require(valid_utf8(s), "PMT symbol is not UTF-8");
        if (rust)
            result.set_string_value(s);
        else
            result.set_symbol_value(s);
    } else {
        require(!rust,
                "PMT kind unsupported by RustRadio profile",
                grpc::StatusCode::UNIMPLEMENTED);
        if (pmt::is_null(v))
            result.mutable_nil_value();
        else if (pmt::is_blob(v))
            result.set_bytes_value(pmt::blob_data(v), pmt::blob_length(v));
        else if (pmt::is_complex(v)) {
            auto c = pmt::to_complex(v);
            result.mutable_complex_value()->set_real(c.real());
            result.mutable_complex_value()->set_imaginary(c.imag());
        } else if (pmt::is_dict(v)) {
            auto entries = pmt::dict_items(v);
            while (pmt::is_pair(entries)) {
                auto entry = pmt::car(entries);
                auto* out = result.mutable_dictionary_value()->add_entries();
                *out->mutable_key() = encode_value(pmt::car(entry), o, b, depth + 1);
                *out->mutable_value() = encode_value(pmt::cdr(entry), o, b, depth + 1);
                entries = pmt::cdr(entries);
            }
        } else if (pmt::is_tuple(v)) {
            for (size_t i = 0; i < pmt::length(v); ++i)
                *result.mutable_tuple_value()->add_values() =
                    encode_value(pmt::tuple_ref(v, i), o, b, depth + 1);
            result.mutable_tuple_value();
        } else if (pmt::is_pair(v)) {
            auto tail = v;
            size_t n = 0;
            while (pmt::is_pair(tail) && n <= MAX_NODES) {
                tail = pmt::cdr(tail);
                ++n;
            }
            require(n <= MAX_NODES,
                    "cyclic/oversized PMT list",
                    grpc::StatusCode::RESOURCE_EXHAUSTED);
            if (pmt::is_null(tail)) {
                tail = v;
                for (size_t i = 0; i < n; ++i) {
                    *result.mutable_list_value()->add_values() =
                        encode_value(pmt::car(tail), o, b, depth + 1);
                    tail = pmt::cdr(tail);
                }
            } else {
                *result.mutable_pair_value()->mutable_first() =
                    encode_value(pmt::car(v), o, b, depth + 1);
                *result.mutable_pair_value()->mutable_second() =
                    encode_value(pmt::cdr(v), o, b, depth + 1);
            }
        } else
            throw protocol_error(grpc::StatusCode::UNIMPLEMENTED,
                                 "unsupported PMT value (no implicit serialization)");
    }
    return result;
}
std::string key_identity(const wire::TagValue& v)
{
    std::string content;
    switch (v.kind_case()) {
    case wire::TagValue::kBoolValue:
        content = v.bool_value() ? "1" : "0";
        break;
    case wire::TagValue::kInt64Value:
        content = std::to_string(v.int64_value());
        break;
    case wire::TagValue::kUint64Value:
        content = std::to_string(v.uint64_value());
        break;
    case wire::TagValue::kStringValue:
        content = v.string_value();
        break;
    case wire::TagValue::kSymbolValue:
        content = v.symbol_value();
        break;
    case wire::TagValue::kBytesValue:
        content = v.bytes_value();
        break;
    case wire::TagValue::kNilValue:
        break;
    default:
        throw protocol_error(grpc::StatusCode::INVALID_ARGUMENT,
                             "invalid dictionary key kind");
    }
    return std::to_string(v.kind_case()) + ":" + content;
}
void validate_value(const wire::TagValue& v,
                    const std::set<int>& declared,
                    value_budget& b,
                    size_t depth)
{
    b.enter(depth);
    require(v.kind_case() != wire::TagValue::KIND_NOT_SET, "unset tag value");
    require(declared.count(v.kind_case()), "undeclared tag value kind");
    switch (v.kind_case()) {
    case wire::TagValue::kStringValue:
        require(valid_utf8(v.string_value()), "invalid string UTF-8");
        break;
    case wire::TagValue::kSymbolValue:
        require(valid_utf8(v.symbol_value()), "invalid symbol UTF-8");
        break;
    case wire::TagValue::kListValue:
    case wire::TagValue::kTupleValue: {
        const auto& values = v.kind_case() == wire::TagValue::kListValue
                                 ? v.list_value().values()
                                 : v.tuple_value().values();
        for (const auto& child : values)
            validate_value(child, declared, b, depth + 1);
        break;
    }
    case wire::TagValue::kPairValue:
        require(v.pair_value().has_first() && v.pair_value().has_second(),
                "missing pair member");
        validate_value(v.pair_value().first(), declared, b, depth + 1);
        validate_value(v.pair_value().second(), declared, b, depth + 1);
        break;
    case wire::TagValue::kDictionaryValue: {
        std::set<std::string> keys;
        for (const auto& e : v.dictionary_value().entries()) {
            require(e.has_key() && e.has_value(), "missing dictionary key/value");
            require(keys.insert(key_identity(e.key())).second,
                    "duplicate dictionary key");
            validate_value(e.key(), declared, b, depth + 1);
            validate_value(e.value(), declared, b, depth + 1);
        }
        break;
    }
    case wire::TagValue::kJsonValue:
        require(valid_utf8(v.json_value()), "invalid JSON UTF-8");
        break;
    case wire::TagValue::kOpaqueValue:
        require(!v.opaque_value().type_url().empty() &&
                    valid_utf8(v.opaque_value().type_url()),
                "invalid opaque codec identifier");
        break;
    default:
        break;
    }
}
pmt::pmt_t decode_value(const wire::TagValue& v, const stream_options& o)
{
    switch (v.kind_case()) {
    case wire::TagValue::kFloat32Value:
        return pmt::from_double(v.float32_value());
    case wire::TagValue::kFloat64Value:
        return pmt::from_double(v.float64_value());
    case wire::TagValue::kBoolValue:
        return pmt::from_bool(v.bool_value());
    case wire::TagValue::kInt64Value:
        require(v.int64_value() >= LONG_MIN && v.int64_value() <= LONG_MAX,
                "PMT signed integer range",
                grpc::StatusCode::UNIMPLEMENTED);
        return pmt::from_long(static_cast<long>(v.int64_value()));
    case wire::TagValue::kUint64Value:
        return pmt::from_uint64(v.uint64_value());
    case wire::TagValue::kStringValue:
        return pmt::intern(v.string_value());
    case wire::TagValue::kSymbolValue:
        return pmt::intern(v.symbol_value());
    case wire::TagValue::kBytesValue:
        return pmt::make_blob(v.bytes_value().data(), v.bytes_value().size());
    case wire::TagValue::kComplexValue:
        return pmt::from_complex(v.complex_value().real(), v.complex_value().imaginary());
    case wire::TagValue::kNilValue:
        return pmt::PMT_NIL;
    case wire::TagValue::kPairValue:
        return pmt::cons(decode_value(v.pair_value().first(), o),
                         decode_value(v.pair_value().second(), o));
    case wire::TagValue::kListValue: {
        auto list = pmt::PMT_NIL;
        const auto& values = v.list_value().values();
        for (int i = values.size() - 1; i >= 0; --i)
            list = pmt::cons(decode_value(values[i], o), list);
        return list;
    }
    case wire::TagValue::kTupleValue: {
        const auto& values = v.tuple_value().values();
        auto vec = pmt::make_vector(values.size(), pmt::PMT_NIL);
        for (int i = 0; i < values.size(); ++i)
            pmt::vector_set(vec, i, decode_value(values[i], o));
        return pmt::to_tuple(vec);
    }
    case wire::TagValue::kDictionaryValue: {
        auto dict = pmt::make_dict();
        const auto& entries = v.dictionary_value().entries();
        for (int i = entries.size() - 1; i >= 0; --i) {
            require(entries[i].key().kind_case() != wire::TagValue::kBytesValue,
                    "PMT dictionaries cannot preserve BYTES key equality",
                    grpc::StatusCode::UNIMPLEMENTED);
            auto key = decode_value(entries[i].key(), o);
            require(!pmt::dict_has_key(dict, key),
                    "PMT dictionary key conversion collision",
                    grpc::StatusCode::UNIMPLEMENTED);
            dict = pmt::dict_add(dict, key, decode_value(entries[i].value(), o));
        }
        return dict;
    }
    default:
        throw protocol_error(grpc::StatusCode::UNIMPLEMENTED,
                             "unsupported wire value kind");
    }
}
} // namespace
wire::TagValue to_wire_value(const pmt::pmt_t& v, const stream_options& o)
{
    value_budget b;
    auto result = encode_value(v, o, b, 1);
    const auto c = capabilities(o);
    std::set<int> kinds(c.begin(), c.end());
    value_budget check;
    validate_value(result, kinds, check, 1);
    // Verify that the graph mapping can represent every exported dictionary key.
    (void)decode_value(result, o);
    return result;
}
pmt::pmt_t from_wire_value(const wire::TagValue& v, const stream_options& o)
{
    const auto c = download_capabilities();
    std::set<int> kinds(c.begin(), c.end());
    value_budget b;
    validate_value(v, kinds, b, 1);
    return decode_value(v, o);
}
wire::StreamDescription description(sample_layout layout, const stream_options& o)
{
    validate_options(o, true);
    wire::StreamDescription d;
    *d.mutable_encoding() = encoding(layout);
    d.set_sample_rate_hz(o.sample_rate_hz);
    for (auto k : capabilities(o))
        d.add_tag_kinds(k);
    d.set_uses_tag_source_ids(o.profile != metadata_profile::RUSTRADIO);
    d.set_source_id(o.source_id);
    if (o.source_sample_offset)
        d.set_source_sample_offset(*o.source_sample_offset);
    if (o.sample_zero_seconds) {
        d.mutable_sample_zero_time()->set_seconds(*o.sample_zero_seconds);
        d.mutable_sample_zero_time()->set_nanos(o.sample_zero_nanos);
    }
    for (const auto& p : o.properties) {
        auto* property = d.add_properties();
        property->set_key(p.first);
        *property->mutable_value() = to_wire_value(p.second, o);
    }
    validate_description(d, layout, o);
    return d;
}
void validate_description(const wire::StreamDescription& d,
                          sample_layout layout,
                          const stream_options& o)
{
    require(d.has_encoding(), "missing encoding");
    const auto expected = encoding(layout);
    require(d.encoding().layout() != wire::SAMPLE_LAYOUT_UNSPECIFIED,
            "missing sample layout");
    require(d.encoding().component_type() == expected.component_type() &&
                d.encoding().byte_order() == expected.byte_order() &&
                d.encoding().layout() == expected.layout(),
            "only exact FLOAT32/LE graph layout is supported",
            grpc::StatusCode::UNIMPLEMENTED);
    require(std::isfinite(d.sample_rate_hz()) && d.sample_rate_hz() > 0,
            "invalid sample rate");
    require(!d.uses_terminal_tags(),
            "terminal tags unsupported",
            grpc::StatusCode::UNIMPLEMENTED);
    require(valid_utf8(d.source_id()), "invalid source ID UTF-8");
    if (d.has_sample_zero_time()) {
        auto t = d.sample_zero_time();
        require(t.seconds() >= -62135596800LL && t.seconds() <= 253402300799LL &&
                    t.nanos() >= 0 && t.nanos() < 1000000000,
                "invalid sample-zero timestamp");
    }
    (void)o;
    std::set<int> declared;
    for (auto k : d.tag_kinds()) {
        require(wire::TagKind_IsValid(k) && k != 0 && declared.insert(k).second,
                "invalid/duplicate tag kind");
    }
    std::set<std::string> codecs;
    for (const auto& codec : d.opaque_codecs())
        require(declared.count(wire::TAG_KIND_OPAQUE) && !codec.empty() &&
                    valid_utf8(codec) && codec.find('*') == std::string::npos &&
                    codecs.insert(codec).second,
                "invalid/duplicate opaque codec declaration");
    value_budget b;
    std::set<std::string> keys;
    for (const auto& p : d.properties()) {
        require(!p.key().empty() && valid_utf8(p.key()) && keys.insert(p.key()).second &&
                    p.has_value(),
                "invalid property");
        validate_value(p.value(), declared, b, 1);
    }
}
namespace {
bool equal_message(const google::protobuf::Message& a, const google::protobuf::Message& b)
{
    auto* d = a.GetDescriptor();
    auto* ar = a.GetReflection();
    auto* br = b.GetReflection();
    for (int i = 0; i < d->field_count(); ++i) {
        auto* f = d->field(i);
        if (f->has_presence()) {
            if (ar->HasField(a, f) != br->HasField(b, f))
                return false;
            if (!ar->HasField(a, f))
                continue;
        }
        int n = f->is_repeated() ? ar->FieldSize(a, f) : 1;
        if (f->is_repeated() && n != br->FieldSize(b, f))
            return false;
        for (int j = 0; j < n; ++j) {
#define COMPARE(TYPE, GETTER)                                    \
    case google::protobuf::FieldDescriptor::TYPE:                \
        if ((f->is_repeated() ? ar->GetRepeated##GETTER(a, f, j) \
                              : ar->Get##GETTER(a, f)) !=        \
            (f->is_repeated() ? br->GetRepeated##GETTER(b, f, j) \
                              : br->Get##GETTER(b, f))) {        \
            return false;                                        \
        }                                                        \
        break
            switch (f->cpp_type()) {
                COMPARE(CPPTYPE_INT32, Int32);
                COMPARE(CPPTYPE_INT64, Int64);
                COMPARE(CPPTYPE_UINT32, UInt32);
                COMPARE(CPPTYPE_UINT64, UInt64);
                COMPARE(CPPTYPE_BOOL, Bool);
                COMPARE(CPPTYPE_ENUM, EnumValue);
                COMPARE(CPPTYPE_STRING, String);
            case google::protobuf::FieldDescriptor::CPPTYPE_DOUBLE:
            case google::protobuf::FieldDescriptor::CPPTYPE_FLOAT: {
                double x, y;
                if (f->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_DOUBLE) {
                    x = f->is_repeated() ? ar->GetRepeatedDouble(a, f, j)
                                         : ar->GetDouble(a, f);
                    y = f->is_repeated() ? br->GetRepeatedDouble(b, f, j)
                                         : br->GetDouble(b, f);
                } else {
                    x = f->is_repeated() ? ar->GetRepeatedFloat(a, f, j)
                                         : ar->GetFloat(a, f);
                    y = f->is_repeated() ? br->GetRepeatedFloat(b, f, j)
                                         : br->GetFloat(b, f);
                }
                if (!(x == y || (std::isnan(x) && std::isnan(y))))
                    return false;
                break;
            }
            case google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE:
                if (!equal_message(f->is_repeated() ? ar->GetRepeatedMessage(a, f, j)
                                                    : ar->GetMessage(a, f),
                                   f->is_repeated() ? br->GetRepeatedMessage(b, f, j)
                                                    : br->GetMessage(b, f)))
                    return false;
                break;
            }
#undef COMPARE
        }
    }
    return true;
}
} // namespace
bool equal_description(const wire::StreamDescription& a, const wire::StreamDescription& b)
{
    auto x = a, y = b;
    std::sort(x.mutable_tag_kinds()->begin(), x.mutable_tag_kinds()->end());
    std::sort(y.mutable_tag_kinds()->begin(), y.mutable_tag_kinds()->end());
    std::sort(x.mutable_opaque_codecs()->begin(), x.mutable_opaque_codecs()->end());
    std::sort(y.mutable_opaque_codecs()->begin(), y.mutable_opaque_codecs()->end());
    auto by_key = [](const wire::Property& p, const wire::Property& q) {
        return p.key() < q.key();
    };
    std::sort(x.mutable_properties()->begin(), x.mutable_properties()->end(), by_key);
    std::sort(y.mutable_properties()->begin(), y.mutable_properties()->end(), by_key);
    return equal_message(x, y);
}
wire::Tag to_wire_tag(const gr::tag_t& t,
                      uint64_t local_origin,
                      uint64_t wire_origin,
                      const stream_options& o)
{
    require(pmt::is_symbol(t.key), "tag key must be a PMT symbol");
    wire::Tag out;
    out.set_key(pmt::symbol_to_string(t.key));
    require(!out.key().empty() && valid_utf8(out.key()) && !reserved_key(out.key()),
            "empty/invalid/reserved tag key");
    require(t.offset >= local_origin, "tag offset precedes window");
    out.set_sample_index(advance(wire_origin, t.offset - local_origin));
    *out.mutable_value() = to_wire_value(t.value, o);
    if (t.srcid != pmt::PMT_F && t.srcid != pmt::PMT_NIL) {
        require(o.profile != metadata_profile::RUSTRADIO && pmt::is_symbol(t.srcid),
                "unsupported tag provenance",
                grpc::StatusCode::UNIMPLEMENTED);
        out.set_source_id(pmt::symbol_to_string(t.srcid));
        require(valid_utf8(out.source_id()), "invalid tag source ID UTF-8");
    }
    return out;
}
gr::tag_t
from_wire_tag(const wire::Tag& t, uint64_t local_offset, const stream_options& o)
{
    gr::tag_t out;
    out.offset = local_offset;
    out.value = from_wire_value(t.value(), o);
    out.key = pmt::intern(t.key());
    out.srcid = t.has_source_id() ? pmt::intern(t.source_id()) : pmt::PMT_F;
    return out;
}
void validate_tags(const google::protobuf::RepeatedPtrField<wire::Tag>& tags,
                   const wire::StreamDescription& d,
                   uint64_t first,
                   uint64_t count,
                   bool terminal)
{
    require(tags.size() <= static_cast<int>(MAX_TAGS),
            "tag count limit",
            grpc::StatusCode::RESOURCE_EXHAUSTED);
    auto end = advance(first, count);
    uint64_t previous = first;
    std::set<int> declared(d.tag_kinds().begin(), d.tag_kinds().end());
    value_budget b;
    for (const auto& t : tags) {
        require(t.sample_index() >= previous &&
                    (terminal ? t.sample_index() == first : t.sample_index() < end),
                "tag position/order");
        require(!t.key().empty() && valid_utf8(t.key()) && !reserved_key(t.key()) &&
                    t.has_value(),
                "invalid tag key/value");
        require(!t.has_source_id() || d.uses_tag_source_ids(), "undeclared provenance");
        require(!t.has_source_id() || valid_utf8(t.source_id()),
                "invalid provenance UTF-8");
        validate_value(t.value(), declared, b, 1);
        previous = t.sample_index();
    }
}
void validate_frame(const wire::Frame& f,
                    const wire::StreamDescription& d,
                    uint64_t sequence,
                    uint64_t cursor,
                    uint64_t grant,
                    wire::LossPolicy policy)
{
    require(f.sequence() == sequence && f.sequence() < grant,
            "invalid frame sequence/credit");
    advance(sequence, 1);
    if (f.has_chunk()) {
        auto& c = f.chunk();
        const size_t sample_bytes =
            d.encoding().layout() == wire::SAMPLE_LAYOUT_REAL ? 4 : 8;
        require(c.first_sample() == cursor && c.sample_count(),
                "invalid chunk cursor/count");
        require(c.sample_count() <= SIZE_MAX / sample_bytes &&
                    c.samples().size() == c.sample_count() * sample_bytes,
                "truncated/incorrect sample payload");
        advance(cursor, c.sample_count());
        validate_tags(c.tags(), d, cursor, c.sample_count());
    } else if (f.has_gap()) {
        require(policy == wire::LOSS_POLICY_ALLOW_GAPS, "gap in lossless session");
        require(f.gap().first_sample() == cursor && f.gap().sample_count(),
                "invalid gap cursor/count");
        require(valid_utf8(f.gap().reason()), "invalid gap reason UTF-8");
        advance(cursor, f.gap().sample_count());
    } else
        require(false, "missing/unknown frame body");
}
void swap_components(std::string& bytes)
{
    const uint32_t one = 1;
    if (*reinterpret_cast<const uint8_t*>(&one) == 1)
        return;
    for (size_t i = 0; i < bytes.size(); i += 4)
        std::reverse(bytes.begin() + i, bytes.begin() + i + 4);
}
namespace {
uint64_t varint(const std::string& bytes, size_t& pos, size_t end)
{
    uint64_t v = 0;
    for (unsigned shift = 0; shift <= 63; shift += 7) {
        require(pos < end, "truncated protobuf varint");
        auto b = static_cast<uint8_t>(bytes[pos++]);
        require(shift != 63 || b <= 1, "protobuf varint overflow");
        v |= static_cast<uint64_t>(b & 127) << shift;
        if (!(b & 128))
            return v;
    }
    throw protocol_error(grpc::StatusCode::INVALID_ARGUMENT, "invalid varint");
}
struct wire_budget {
    size_t nodes = 0;
    size_t frame_bytes = 0;
    bool envelope_frame = false;
};
void inspect(const std::string& bytes,
             size_t pos,
             size_t end,
             const google::protobuf::Descriptor* desc,
             size_t depth,
             size_t value_depth,
             wire_budget& budget,
             uint32_t group = 0)
{
    require(
        depth <= 256, "protobuf recursion limit", grpc::StatusCode::RESOURCE_EXHAUSTED);
    if (desc == wire::TagValue::descriptor()) {
        require(++value_depth <= MAX_DEPTH && ++budget.nodes <= MAX_NODES,
                "tag decoder budget",
                grpc::StatusCode::RESOURCE_EXHAUSTED);
    }
    if (desc == wire::Frame::descriptor())
        budget.frame_bytes += end - pos;
    size_t tags = 0;
    while (pos < end) {
        auto key = varint(bytes, pos, end);
        require((key >> 3) > 0 && (key >> 3) <= 0x1fffffff,
                "invalid protobuf field number");
        auto number = static_cast<uint32_t>(key >> 3);
        auto type = key & 7;
        auto* field = desc ? desc->FindFieldByNumber(number) : nullptr;
        if (type == 0) {
            varint(bytes, pos, end);
            continue;
        }
        if (type == 4) {
            require(group == number, "unexpected protobuf end group");
            return;
        }
        if (type == 3) {
            // Skip unknown groups with a bounded coded stream, without allocating.
            google::protobuf::io::CodedInputStream input(
                reinterpret_cast<const uint8_t*>(bytes.data() + pos), end - pos);
            input.SetRecursionLimit(256 - static_cast<int>(depth));
            require(google::protobuf::internal::WireFormatLite::SkipField(
                        &input, static_cast<uint32_t>(key)),
                    "invalid protobuf group");
            pos += input.CurrentPosition();
            continue;
        }
        size_t size;
        if (type == 1)
            size = 8;
        else if (type == 5)
            size = 4;
        else if (type == 2) {
            auto n = varint(bytes, pos, end);
            require(n <= end - pos, "truncated protobuf message");
            size = static_cast<size_t>(n);
        } else
            throw protocol_error(grpc::StatusCode::INVALID_ARGUMENT,
                                 "invalid protobuf wire type");
        require(size <= end - pos, "truncated protobuf field");
        if (depth == 0 && type == 2 && field && field->containing_oneof())
            budget.envelope_frame = field->message_type() == wire::Frame::descriptor();
        if (type == 2 && field &&
            field->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE) {
            if (field->message_type() == wire::Tag::descriptor())
                require(++tags <= MAX_TAGS,
                        "tag decoder count limit",
                        grpc::StatusCode::RESOURCE_EXHAUSTED);
            inspect(bytes,
                    pos,
                    pos + size,
                    field->message_type(),
                    depth + 1,
                    value_depth,
                    budget);
        }
        pos += size;
    }
    require(!group, "unterminated protobuf group");
}
} // namespace
size_t inspect_wire(const std::string& bytes, const google::protobuf::Descriptor* desc)
{
    require(bytes.size() <= MAX_ENVELOPE_BYTES,
            "envelope byte limit",
            grpc::StatusCode::RESOURCE_EXHAUSTED);
    wire_budget b;
    inspect(bytes, 0, bytes.size(), desc, 0, 0, b);
    require(b.frame_bytes <= 1024 * 1024,
            "frame byte limit",
            grpc::StatusCode::RESOURCE_EXHAUSTED);
    require(b.envelope_frame || bytes.size() <= MAX_CONTROL_BYTES,
            "control byte limit",
            grpc::StatusCode::RESOURCE_EXHAUSTED);
    return b.frame_bytes;
}
} // namespace iqstream
} // namespace gr
