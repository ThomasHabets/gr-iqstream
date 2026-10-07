/* SPDX-License-Identifier: GPL-3.0-or-later */
#include "session.h"
#include <gnuradio/thread/thread.h>
#include <algorithm>
#include <cstring>
#include <set>

namespace gr {
namespace iqstream {
namespace {
wire::LossPolicy policy(loss_policy p)
{
    return p == loss_policy::LOSSLESS ? wire::LOSS_POLICY_LOSSLESS
                                      : wire::LOSS_POLICY_ALLOW_GAPS;
}
template <class Message>
std::string control_bytes(const Message& m)
{
    auto bytes = m.SerializeAsString();
    require(bytes.size() <= MAX_CONTROL_BYTES,
            "outgoing control byte limit",
            grpc::StatusCode::RESOURCE_EXHAUSTED);
    inspect_wire(bytes, Message::descriptor());
    return bytes;
}
void validate_open(const wire::Open& o)
{
    require(o.protocol_version() == 1,
            "unsupported protocol version",
            grpc::StatusCode::UNIMPLEMENTED);
    require(o.has_limits(), "missing limits");
    validate_limits(o.limits());
    require(o.loss_policy() == wire::LOSS_POLICY_LOSSLESS ||
                o.loss_policy() == wire::LOSS_POLICY_ALLOW_GAPS,
            "invalid loss policy");
    require(o.completion_mode() != wire::COMPLETION_MODE_UNSPECIFIED,
            "missing completion mode");
    require(o.completion_mode() == wire::COMPLETION_MODE_ACCEPTED,
            "only ACCEPTED completion supported",
            grpc::StatusCode::UNIMPLEMENTED);
    require(o.has_upload() || o.has_download(), "missing operation");
    const auto& id = o.has_upload() ? o.upload().destination() : o.download().source();
    require(!id.empty() && valid_utf8(id), "empty/invalid resource identifier");
}
void validate_download(const wire::Download& d)
{
    require(!d.accepted_encodings().empty(), "empty encoding capability set");
    std::set<std::tuple<int, int, int>> encodings;
    for (const auto& e : d.accepted_encodings()) {
        require(wire::ComponentType_IsValid(e.component_type()) &&
                    e.component_type() != 0 &&
                    (e.layout() == wire::SAMPLE_LAYOUT_REAL ||
                     e.layout() == wire::SAMPLE_LAYOUT_COMPLEX),
                "invalid offered encoding");
        bool eight = e.component_type() == wire::COMPONENT_TYPE_INT8 ||
                     e.component_type() == wire::COMPONENT_TYPE_UINT8;
        require(eight ? e.byte_order() == wire::BYTE_ORDER_NOT_APPLICABLE
                      : (e.byte_order() == wire::BYTE_ORDER_LITTLE_ENDIAN ||
                         e.byte_order() == wire::BYTE_ORDER_BIG_ENDIAN),
                "invalid byte order");
        require(encodings.emplace(e.component_type(), e.byte_order(), e.layout()).second,
                "duplicate offered encoding");
    }
    std::set<int> kinds;
    for (auto k : d.accepted_tag_kinds())
        require(k && wire::TagKind_IsValid(k) && kinds.insert(k).second,
                "invalid/duplicate offered tag kind");
    std::set<std::string> codecs;
    require(d.accepted_opaque_codecs().empty() || kinds.count(wire::TAG_KIND_OPAQUE),
            "opaque codecs require OPAQUE capability");
    for (const auto& c : d.accepted_opaque_codecs())
        require(!c.empty() && valid_utf8(c) && c.find('*') == std::string::npos &&
                    codecs.insert(c).second,
                "invalid offered opaque codec");
}
} // namespace
session::session(bool client,
                 bool sending,
                 sample_layout layout,
                 stream_options options,
                 std::string resource)
    : d_client(client),
      d_sending(sending),
      d_layout(layout),
      d_options(std::move(options)),
      d_resource(std::move(resource)),
      d_policy(policy(d_options.loss)),
      d_open_time(std::chrono::steady_clock::now()),
      d_credit_time(d_open_time)
{
    validate_options(d_options, sending);
    d_limits.set_max_frame_bytes(d_options.max_frame_bytes);
    d_limits.set_max_in_flight_frames(d_options.max_in_flight_frames);
    if (sending)
        d_description = description(layout, d_options);
    d_status.state = session_state::OPENING;
}
void session::bind(std::function<void(const std::string&)> write,
                   std::function<void()> half_close,
                   std::function<void(grpc::Status)> finish,
                   std::function<void()> abort,
                   resolver resolve)
{
    std::lock_guard<std::mutex> guard(d_mutex);
    d_write = std::move(write);
    d_half_close = std::move(half_close);
    d_finish = std::move(finish);
    d_abort = std::move(abort);
    d_resolve = std::move(resolve);
}
std::string session::client_control(const wire::ClientMessage& m) const
{
    return control_bytes(m);
}
std::string session::server_control(const wire::ServerMessage& m) const
{
    return control_bytes(m);
}
void session::begin_client()
{
    std::lock_guard<std::mutex> guard(d_mutex);
    wire::ClientMessage m;
    auto* o = m.mutable_open();
    o->set_protocol_version(1);
    *o->mutable_limits() = d_limits;
    o->set_loss_policy(d_policy);
    o->set_completion_mode(wire::COMPLETION_MODE_ACCEPTED);
    require(!d_resource.empty() && valid_utf8(d_resource), "invalid resource identifier");
    if (d_sending) {
        o->mutable_upload()->set_destination(d_resource);
        *o->mutable_upload()->mutable_description() = d_description;
    } else {
        auto* download = o->mutable_download();
        download->set_source(d_resource);
        *download->add_accepted_encodings() = encoding(d_layout);
        for (auto kind : capabilities(d_options))
            download->add_accepted_tag_kinds(kind);
        download->set_accept_tag_source_ids(d_options.profile !=
                                            metadata_profile::RUSTRADIO);
    }
    d_controls.emplace_back(client_control(m), action::OPEN);
    drive_locked();
}
void session::update_description_locked()
{
    d_status.description_bytes = d_description.SerializeAsString();
    d_status.sample_rate_hz = d_description.sample_rate_hz();
    if (d_description.has_source_sample_offset())
        d_status.source_sample_offset = d_description.source_sample_offset();
}
void session::open_locked(const wire::Open& o)
{
    require(!d_started, "second Open");
    validate_open(o);
    if (o.has_download())
        validate_download(o.download());
    auto binding = d_resolve(o, shared_from_this());
    d_sending = binding.sending;
    d_layout = binding.layout;
    d_options = binding.options;
    d_policy = policy(d_options.loss);
    require(o.loss_policy() == d_policy,
            "resource loss policy mismatch",
            grpc::StatusCode::FAILED_PRECONDITION);
    d_limits.set_max_frame_bytes(
        std::min(o.limits().max_frame_bytes(), d_options.max_frame_bytes));
    d_limits.set_max_in_flight_frames(
        std::min(o.limits().max_in_flight_frames(), d_options.max_in_flight_frames));
    if (o.has_upload()) {
        require(!d_sending && o.upload().has_description(),
                "incompatible upload resource",
                grpc::StatusCode::FAILED_PRECONDITION);
        validate_description(o.upload().description(), d_layout, d_options);
        require(o.upload().description().sample_rate_hz() == d_options.sample_rate_hz,
                "upload sample rate mismatch",
                grpc::StatusCode::FAILED_PRECONDITION);
        d_description = o.upload().description();
    } else {
        require(d_sending,
                "incompatible download resource",
                grpc::StatusCode::FAILED_PRECONDITION);
        d_description = description(d_layout, d_options);
        d_description.set_source_sample_offset(
            advance(d_options.source_sample_offset.value_or(0), binding.graph_origin));
        bool compatible = false;
        for (const auto& e : o.download().accepted_encodings()) {
            const auto& own = d_description.encoding();
            compatible |= e.component_type() == own.component_type() &&
                          e.byte_order() == own.byte_order() &&
                          e.layout() == own.layout();
        }
        require(compatible, "no compatible encoding", grpc::StatusCode::UNIMPLEMENTED);
        std::set<int> kinds(o.download().accepted_tag_kinds().begin(),
                            o.download().accepted_tag_kinds().end());
        for (auto kind : d_description.tag_kinds())
            require(kinds.count(kind),
                    "incompatible declared tag kinds",
                    grpc::StatusCode::UNIMPLEMENTED);
        require(!d_description.uses_tag_source_ids() ||
                    o.download().accept_tag_source_ids(),
                "peer cannot preserve provenance",
                grpc::StatusCode::UNIMPLEMENTED);
    }
    update_description_locked();
    wire::ServerMessage reply;
    auto* s = reply.mutable_started();
    *s->mutable_description() = d_description;
    *s->mutable_limits() = d_limits;
    s->set_loss_policy(d_policy);
    s->set_completion_mode(wire::COMPLETION_MODE_ACCEPTED);
    if (!d_sending)
        s->mutable_initial_credit()->set_send_limit(d_limits.max_in_flight_frames());
    d_controls.emplace_back(server_control(reply), action::STARTED);
    d_started = true;
    d_status.state = session_state::STREAMING;
    d_cv.notify_all();
}
void session::started_locked(const wire::Started& s)
{
    require(!d_started && s.has_description() && s.has_limits(),
            "second/missing Started fields");
    validate_limits(s.limits());
    require(s.limits().max_frame_bytes() <= d_limits.max_frame_bytes() &&
                s.limits().max_in_flight_frames() <= d_limits.max_in_flight_frames(),
            "server raised limits");
    require(s.loss_policy() == d_policy &&
                s.completion_mode() == wire::COMPLETION_MODE_ACCEPTED,
            "policy/completion downgrade");
    validate_description(s.description(), d_layout, d_options);
    if (d_sending) {
        require(equal_description(d_description, s.description()),
                "upload description changed");
        require(s.has_initial_credit() &&
                    s.initial_credit().send_limit() == s.limits().max_in_flight_frames(),
                "invalid initial upload credit");
        d_send_limit = s.initial_credit().send_limit();
    } else {
        require(!s.has_initial_credit(), "unexpected initial download credit");
        d_description = s.description();
        d_credit = s.limits().max_in_flight_frames();
    }
    d_limits = s.limits();
    d_started = true;
    d_status.state = session_state::STREAMING;
    update_description_locked();
    d_cv.notify_all();
}
void session::credit_locked(const wire::FlowControl& c)
{
    require(d_started && d_sending && !d_peer_complete,
            "credit in wrong phase/direction");
    auto limit = c.send_limit();
    auto window = d_limits.max_in_flight_frames();
    require(limit >= d_send_limit && limit >= window && limit - window <= d_sequence,
            "invalid fixed-window credit");
    if (!d_send_limit)
        require(limit == window, "initial credit must equal window");
    d_send_limit = limit;
    d_cv.notify_all();
}
void session::frame_locked(wire::Frame f, size_t bytes)
{
    require(d_started && !d_sending && !d_end_received && d_grant,
            "frame in wrong phase/direction");
    require(bytes <= d_limits.max_frame_bytes(),
            "negotiated frame byte limit",
            grpc::StatusCode::RESOURCE_EXHAUSTED);
    validate_frame(f, d_description, d_sequence, d_cursor, d_grant, d_policy);
    if (f.has_gap()) {
        d_receive_gap = advance(d_receive_gap, f.gap().sample_count());
        d_status.lost_samples = advance(d_status.lost_samples, f.gap().sample_count());
        d_status.trailing_gap = d_receive_gap;
        d_cursor = advance(d_cursor, f.gap().sample_count());
        d_sequence = advance(d_sequence, 1);
        // Gap storage is bounded separately, but retirement must remain ordered.
        incoming item;
        item.frame = std::move(f);
        d_incoming.push_back(std::move(item));
        while (!d_incoming.empty() && d_incoming.front().frame.has_gap()) {
            d_incoming.pop_front();
            retire_locked();
        }
    } else {
        incoming item;
        item.gap_before = d_receive_gap;
        d_receive_gap = 0;
        d_status.trailing_gap = 0;
        for (const auto& t : f.chunk().tags())
            item.tags.push_back(from_wire_tag(t, t.sample_index(), d_options));
        swap_components(*f.mutable_chunk()->mutable_samples());
        d_cursor = advance(d_cursor, f.chunk().sample_count());
        d_sequence = advance(d_sequence, 1);
        item.frame = std::move(f);
        d_incoming.push_back(std::move(item));
    }
    d_status.next_sequence = d_sequence;
    d_status.next_sample = d_cursor;
    d_cv.notify_all();
}
void session::end_locked(const wire::End& end)
{
    require(d_started && !d_sending && d_grant && !d_end_received,
            "End in wrong phase/direction");
    require(end.next_sequence() == d_sequence && end.next_sample() == d_cursor,
            "End cursors mismatch");
    require(end.tags().empty(),
            "terminal metadata unsupported",
            grpc::StatusCode::UNIMPLEMENTED);
    d_end_received = true;
    d_status.state = session_state::FINISHING;
    d_finish_time = std::chrono::steady_clock::now();
    receiver_completion_locked();
    d_cv.notify_all();
}
wire::Complete session::completion_locked() const
{
    wire::Complete c;
    c.set_next_sequence(d_sequence);
    c.set_next_sample(d_cursor);
    c.set_completion_mode(wire::COMPLETION_MODE_ACCEPTED);
    return c;
}
void session::complete_locked(const wire::Complete& c)
{
    require(d_started && !d_peer_complete &&
                (d_sending ? d_end_sent : d_client && d_complete_sent),
            "Complete in wrong phase/direction");
    require(c.next_sequence() == d_sequence && c.next_sample() == d_cursor &&
                c.completion_mode() == wire::COMPLETION_MODE_ACCEPTED,
            "Complete mismatch");
    d_peer_complete = true;
    if (!d_client) {
        wire::ServerMessage echo;
        *echo.mutable_complete() = c;
        d_controls.emplace_back(server_control(echo), action::COMPLETE);
    }
}
void session::retire_locked()
{
    d_retired = advance(d_retired, 1);
    if (!d_end_received) {
        const auto window = d_limits.max_in_flight_frames();
        // At the uint64 ceiling retain the final grant and retire remaining
        // permitted frames without wrapping or granting additional capacity.
        if (d_retired <= UINT64_MAX - window)
            d_credit = d_retired + window;
    }
}
void session::receiver_completion_locked()
{
    if (!d_end_received || !d_incoming.empty() || d_complete_queued || d_failed)
        return;
    // Queued credits precede Complete; none are issued after End.
    if (d_client) {
        wire::ClientMessage m;
        *m.mutable_complete() = completion_locked();
        d_controls.emplace_back(client_control(m), action::COMPLETE);
    } else {
        wire::ServerMessage m;
        *m.mutable_complete() = completion_locked();
        d_controls.emplace_back(server_control(m), action::COMPLETE);
    }
    // Prevent enqueueing a second completion while this one waits for a write.
    d_complete_queued = true;
}
void session::queue_end_locked()
{
    if (!d_finish_requested || !d_started || !d_send_limit || !d_outgoing.empty() ||
        d_pending_gap || d_end_sent)
        return;
    if (d_client) {
        wire::ClientMessage m;
        m.mutable_end()->set_next_sequence(d_sequence);
        m.mutable_end()->set_next_sample(d_cursor);
        d_controls.emplace_back(client_control(m), action::END);
    } else {
        wire::ServerMessage m;
        m.mutable_end()->set_next_sequence(d_sequence);
        m.mutable_end()->set_next_sample(d_cursor);
        d_controls.emplace_back(server_control(m), action::END);
    }
}
void session::send_locked(const std::string& bytes, action kind)
{
    d_busy = true;
    d_write_action = kind;
    if (kind == action::END) {
        d_end_sent = true;
        d_status.state = session_state::FINISHING;
    }
    if (kind == action::COMPLETE)
        d_complete_sent = true;
    if (kind == action::STARTED && !d_sending)
        d_grant = d_limits.max_in_flight_frames();
    d_write(bytes);
}
void session::drive_locked()
{
    if (d_busy || d_done || d_read_closed || d_finishing_transport)
        return;
    // A credit is capacity, so record it when submitted, not when queued.
    if (d_credit && !d_failed) {
        const auto now = std::chrono::steady_clock::now();
        const auto delta = *d_credit - d_grant;
        if (d_grant == 0 ||
            delta >= std::max<uint64_t>(1, d_limits.max_in_flight_frames() / 2) ||
            now - d_credit_time >= std::chrono::milliseconds(10) || d_complete_queued) {
            auto limit = *d_credit;
            d_credit.reset();
            d_grant = limit;
            d_credit_time = now;
            if (d_client) {
                wire::ClientMessage m;
                m.mutable_flow_control()->set_send_limit(limit);
                send_locked(client_control(m), action::CREDIT);
            } else {
                wire::ServerMessage m;
                m.mutable_flow_control()->set_send_limit(limit);
                send_locked(server_control(m), action::CREDIT);
            }
            return;
        }
    }
    if (!d_controls.empty()) {
        auto entry = std::move(d_controls.front());
        d_controls.pop_front();
        send_locked(entry.first, entry.second);
        return;
    }
    if (d_failed || !d_started || !d_sending || !d_send_limit || d_end_sent)
        return;
    if (d_outgoing.empty() && d_pending_gap) {
        wire::Frame f;
        f.mutable_gap()->set_first_sample(d_pending_gap_first);
        f.mutable_gap()->set_sample_count(d_pending_gap);
        f.mutable_gap()->set_reason(
            "bounded acquisition queue overflow; persistent tag state unknown");
        d_outgoing.push_back(std::move(f));
        d_pending_gap = 0;
    }
    if (!d_outgoing.empty() && d_sequence < d_send_limit) {
        auto f = std::move(d_outgoing.front());
        d_outgoing.pop_front();
        f.set_sequence(d_sequence);
        validate_frame(f, d_description, d_sequence, d_cursor, d_send_limit, d_policy);
        require(f.ByteSizeLong() <= d_limits.max_frame_bytes(),
                "outgoing frame byte limit",
                grpc::StatusCode::RESOURCE_EXHAUSTED);
        d_sequence = advance(d_sequence, 1);
        d_cursor = advance(
            d_cursor, f.has_chunk() ? f.chunk().sample_count() : f.gap().sample_count());
        d_status.next_sequence = d_sequence;
        d_status.next_sample = d_cursor;
        if (d_client) {
            wire::ClientMessage m;
            *m.mutable_frame() = std::move(f);
            send_locked(m.SerializeAsString(), action::FRAME);
        } else {
            wire::ServerMessage m;
            *m.mutable_frame() = std::move(f);
            send_locked(m.SerializeAsString(), action::FRAME);
        }
        d_cv.notify_all();
        return;
    }
    queue_end_locked();
    if (!d_controls.empty()) {
        auto entry = std::move(d_controls.front());
        d_controls.pop_front();
        send_locked(entry.first, entry.second);
    }
}
void session::fail_locked(grpc::StatusCode code, const std::string& text, bool notify)
{
    if (d_done || d_failed)
        return;
    d_failed = true;
    d_status.state = code == grpc::StatusCode::CANCELLED ? session_state::CANCELLED
                                                         : session_state::FAILED;
    d_status.grpc_status_code = code;
    d_status.message = text.substr(0, 4096);
    d_status.completion_uncertain = d_end_sent || d_complete_sent;
    d_finish_time = std::chrono::steady_clock::now();
    d_outgoing.clear();
    d_incoming.clear();
    d_controls.clear();
    d_credit.reset();
    d_pending_gap = 0;
    if (notify && !d_read_closed && !d_peer_complete && !d_complete_sent &&
        !d_half_closed) {
        if (d_client) {
            wire::ClientMessage m;
            if (d_end_sent)
                m.mutable_cancel()->set_reason(d_status.message);
            else {
                m.mutable_failure()->set_grpc_status_code(code);
                m.mutable_failure()->set_message(d_status.message);
            }
            d_controls.emplace_back(client_control(m), action::FAILURE);
        } else {
            wire::ServerMessage m;
            m.mutable_failure()->set_grpc_status_code(code);
            m.mutable_failure()->set_message(d_status.message);
            d_controls.emplace_back(server_control(m), action::FAILURE);
        }
        drive_locked();
    } else if (d_abort)
        d_abort();
    d_cv.notify_all();
}
void session::remote_failure_locked(const wire::Failure& failure)
{
    d_failed = true;
    d_remote_failure = true;
    d_status.state = session_state::FAILED;
    d_status.grpc_status_code = failure.grpc_status_code();
    d_status.message = failure.message();
    d_status.completion_uncertain = d_end_sent || d_complete_sent;
    d_finish_time = std::chrono::steady_clock::now();
    d_controls.clear();
    d_outgoing.clear();
    d_incoming.clear();
    d_credit.reset();
    if (!d_busy && !d_half_closed) {
        d_half_closed = true;
        d_half_close();
    }
    d_cv.notify_all();
}
void session::received(const std::string& bytes)
{
    std::lock_guard<std::mutex> guard(d_mutex);
    try {
        require(!d_done && !d_peer_complete, "message after terminal Complete");
        if (d_failed)
            return;
        if (d_client) {
            wire::ServerMessage m;
            auto size = decode(bytes, m);
            if (!m.has_frame())
                require(bytes.size() <= MAX_CONTROL_BYTES,
                        "control byte limit",
                        grpc::StatusCode::RESOURCE_EXHAUSTED);
            switch (m.body_case()) {
            case wire::ServerMessage::kStarted:
                started_locked(m.started());
                break;
            case wire::ServerMessage::kFrame:
                frame_locked(std::move(*m.mutable_frame()), size);
                break;
            case wire::ServerMessage::kFlowControl:
                credit_locked(m.flow_control());
                break;
            case wire::ServerMessage::kEnd:
                end_locked(m.end());
                break;
            case wire::ServerMessage::kComplete:
                complete_locked(m.complete());
                break;
            case wire::ServerMessage::kFailure:
                require(m.failure().grpc_status_code() >= 1 &&
                            m.failure().grpc_status_code() <= 16,
                        "invalid Failure status");
                remote_failure_locked(m.failure());
                break;
            default:
                require(false, "missing/unknown server envelope body");
            }
        } else {
            wire::ClientMessage m;
            auto size = decode(bytes, m);
            if (!m.has_frame())
                require(bytes.size() <= MAX_CONTROL_BYTES,
                        "control byte limit",
                        grpc::StatusCode::RESOURCE_EXHAUSTED);
            if (!d_started)
                require(m.has_open(), "first message must be Open");
            switch (m.body_case()) {
            case wire::ClientMessage::kOpen:
                open_locked(m.open());
                break;
            case wire::ClientMessage::kFrame:
                frame_locked(std::move(*m.mutable_frame()), size);
                break;
            case wire::ClientMessage::kFlowControl:
                credit_locked(m.flow_control());
                break;
            case wire::ClientMessage::kEnd:
                end_locked(m.end());
                break;
            case wire::ClientMessage::kComplete:
                complete_locked(m.complete());
                break;
            case wire::ClientMessage::kCancel:
                fail_locked(grpc::StatusCode::CANCELLED, m.cancel().reason());
                break;
            case wire::ClientMessage::kFailure:
                require(!d_end_received && !d_complete_sent,
                        "client Failure after End/Complete");
                require(m.failure().grpc_status_code() >= 1 &&
                            m.failure().grpc_status_code() <= 16,
                        "invalid Failure status");
                fail_locked(static_cast<grpc::StatusCode>(m.failure().grpc_status_code()),
                            m.failure().message());
                break;
            default:
                require(false, "missing/unknown client envelope body");
            }
        }
        drive_locked();
    } catch (const protocol_error& e) {
        fail_locked(e.code(), e.what());
    } catch (const std::exception& e) {
        fail_locked(grpc::StatusCode::INTERNAL, e.what());
    }
}
void session::write_done(bool ok)
{
    std::lock_guard<std::mutex> guard(d_mutex);
    d_busy = false;
    if (!ok) {
        fail_locked(grpc::StatusCode::UNAVAILABLE, "transport write failed", false);
        return;
    }
    if (d_remote_failure) {
        if (!d_half_closed && !d_read_closed) {
            d_half_closed = true;
            d_half_close();
        }
        return;
    }
    if (d_write_action == action::FAILURE) {
        if (d_client) {
            if (!d_half_closed) {
                d_half_closed = true;
                d_half_close();
            }
        } else {
            d_finishing_transport = true;
            d_finish(
                grpc::Status(static_cast<grpc::StatusCode>(d_status.grpc_status_code),
                             d_status.message));
        }
        return;
    }
    if (d_client &&
        (d_write_action == action::END || d_write_action == action::COMPLETE)) {
        d_half_closed = true;
        d_half_close();
    }
    if (!d_client && d_write_action == action::COMPLETE) {
        d_finishing_transport = true;
        d_finish(grpc::Status::OK);
        return;
    }
    try {
        drive_locked();
    } catch (const protocol_error& e) {
        fail_locked(e.code(), e.what());
    }
    d_cv.notify_all();
}
void session::read_closed()
{
    std::lock_guard<std::mutex> guard(d_mutex);
    if (d_client) {
        d_read_closed = true;
        d_controls.clear();
        d_credit.reset();
        d_cv.notify_all();
    } else if (!d_failed && !d_finishing_transport &&
               !(d_sending ? d_peer_complete : d_end_received)) {
        fail_locked(grpc::StatusCode::DATA_LOSS, "half-close before End/Complete");
    }
}
void session::transport_done(const grpc::Status& result)
{
    std::lock_guard<std::mutex> guard(d_mutex);
    if (d_done)
        return;
    bool complete = d_client ? d_peer_complete : d_complete_sent;
    if (!d_failed && complete && result.ok()) {
        d_status.state = session_state::COMPLETE;
        d_status.grpc_status_code = 0;
    } else {
        if (!d_failed) {
            d_status.grpc_status_code =
                result.ok() ? grpc::StatusCode::DATA_LOSS : result.error_code();
            d_status.message = result.ok()
                                   ? "transport ended without final acknowledgement"
                                   : result.error_message();
            d_status.state = result.error_code() == grpc::StatusCode::CANCELLED
                                 ? session_state::CANCELLED
                                 : session_state::FAILED;
        } else if (d_remote_failure && !result.ok()) {
            if (static_cast<int>(result.error_code()) != d_status.grpc_status_code)
                d_status.message =
                    "Failure/trailer status mismatch: " + result.error_message();
            d_status.grpc_status_code = result.error_code();
        } else if (result.ok()) {
            d_status.grpc_status_code = grpc::StatusCode::INVALID_ARGUMENT;
            d_status.message = "Failure followed by gRPC OK";
            d_status.state = session_state::FAILED;
        }
        d_status.completion_uncertain = d_end_sent || d_complete_sent;
        d_failed = true;
    }
    d_done = true;
    d_controls.clear();
    d_outgoing.clear();
    d_credit.reset();
    d_cv.notify_all();
}
void session::tick()
{
    std::lock_guard<std::mutex> guard(d_mutex);
    if (d_done)
        return;
    auto now = std::chrono::steady_clock::now();
    if (!d_started &&
        now - d_open_time >= std::chrono::milliseconds(d_options.handshake_timeout_ms)) {
        fail_locked(
            grpc::StatusCode::DEADLINE_EXCEEDED, "handshake deadline expired", false);
    } else if ((d_finish_requested || d_end_received || d_failed) &&
               now - d_finish_time >=
                   std::chrono::milliseconds(d_options.shutdown_timeout_ms)) {
        if (!d_failed)
            fail_locked(grpc::StatusCode::DEADLINE_EXCEEDED,
                        "completion deadline expired",
                        false);
        else
            d_abort();
    } else if (d_options.transfer_timeout_ms &&
               now - d_open_time >=
                   std::chrono::milliseconds(d_options.transfer_timeout_ms)) {
        fail_locked(
            grpc::StatusCode::DEADLINE_EXCEEDED, "transfer deadline expired", false);
    }
    try {
        drive_locked();
    } catch (const protocol_error& e) {
        fail_locked(e.code(), e.what());
    }
}
size_t session::push(const void* data,
                     size_t count,
                     const std::vector<gr::tag_t>& tags,
                     uint64_t local_offset)
{
    std::unique_lock<std::mutex> lock(d_mutex);
    while (
        !d_failed && !d_done &&
        (!d_started || !d_send_limit ||
         (d_options.blocking && d_outgoing.size() >= d_limits.max_in_flight_frames()))) {
        d_cv.wait_for(lock, std::chrono::milliseconds(10));
        boost::this_thread::interruption_point();
    }
    if (d_failed || d_done || d_read_closed)
        return 0;
    require(!d_finish_requested && d_sending, "input after finish");
    count = std::min<size_t>(count, UINT32_MAX);
    if (!count)
        return 0;
    if (!d_options.blocking &&
        (d_pending_gap || d_outgoing.size() >= d_limits.max_in_flight_frames())) {
        if (!d_outgoing.empty() && d_outgoing.back().has_gap() && !d_pending_gap) {
            auto* gap = d_outgoing.back().mutable_gap();
            gap->set_sample_count(advance(gap->sample_count(), count));
        } else {
            if (!d_pending_gap)
                d_pending_gap_first = d_acquisition_cursor;
            d_pending_gap = advance(d_pending_gap, count);
        }
        d_acquisition_cursor = advance(d_acquisition_cursor, count);
        d_status.lost_samples = advance(d_status.lost_samples, count);
        d_status.trailing_gap = advance(d_status.trailing_gap, count);
        drive_locked();
        return count;
    }
    const size_t bytes_per_sample = d_layout == sample_layout::REAL ? 4 : 8;
    count = std::min<size_t>(count, d_limits.max_frame_bytes() / bytes_per_sample);
    std::vector<wire::Tag> converted;
    for (const auto& t : tags) {
        if (t.offset >= local_offset && t.offset - local_offset < count)
            converted.push_back(
                to_wire_tag(t, local_offset, d_acquisition_cursor, d_options));
    }
    std::stable_sort(
        converted.begin(), converted.end(), [](const auto& a, const auto& b) {
            return a.sample_index() < b.sample_index();
        });
    auto build = [&](size_t n) {
        wire::Frame f;
        // Reserve worst-case frame-sequence encoding before its final assignment.
        f.set_sequence(UINT64_MAX);
        auto* c = f.mutable_chunk();
        c->set_first_sample(d_acquisition_cursor);
        c->set_sample_count(static_cast<uint32_t>(n));
        c->set_samples(data, n * bytes_per_sample);
        for (const auto& t : converted)
            if (t.sample_index() - d_acquisition_cursor < n)
                *c->add_tags() = t;
        return f;
    };
    auto f = build(count);
    size_t fit = count;
    if (f.ByteSizeLong() > d_limits.max_frame_bytes() ||
        f.chunk().tags_size() > static_cast<int>(MAX_TAGS)) {
        size_t lo = 1, hi = count;
        fit = 0;
        while (lo <= hi) {
            auto mid = lo + (hi - lo) / 2;
            auto candidate = build(mid);
            if (candidate.ByteSizeLong() <= d_limits.max_frame_bytes() &&
                candidate.chunk().tags_size() <= static_cast<int>(MAX_TAGS)) {
                fit = mid;
                lo = mid + 1;
            } else
                hi = mid - 1;
        }
        require(fit,
                "one sample and its tags cannot fit in a frame",
                grpc::StatusCode::RESOURCE_EXHAUSTED);
        f = build(fit);
    }
    validate_tags(f.chunk().tags(), d_description, d_acquisition_cursor, fit);
    // Include aggregate pre-allocation budgets, not just each individual value.
    wire::ClientMessage budget_message;
    *budget_message.mutable_frame() = f;
    inspect_wire(budget_message.SerializeAsString(), wire::ClientMessage::descriptor());
    swap_components(*f.mutable_chunk()->mutable_samples());
    d_outgoing.push_back(std::move(f));
    d_acquisition_cursor = advance(d_acquisition_cursor, fit);
    d_status.trailing_gap = 0;
    drive_locked();
    return fit;
}
int session::pull(void* output,
                  int capacity,
                  uint64_t local_offset,
                  std::vector<gr::tag_t>& tags)
{
    std::unique_lock<std::mutex> lock(d_mutex);
    while (d_incoming.empty() && !d_failed && !d_done) {
        receiver_completion_locked();
        drive_locked();
        d_cv.wait_for(lock, std::chrono::milliseconds(10));
        boost::this_thread::interruption_point();
    }
    if (d_failed || (d_done && d_incoming.empty()))
        return -1;
    while (!d_incoming.empty() && d_incoming.front().frame.has_gap()) {
        d_incoming.pop_front();
        retire_locked();
    }
    if (d_incoming.empty()) {
        receiver_completion_locked();
        drive_locked();
        return 0;
    }
    auto& item = d_incoming.front();
    const auto& c = item.frame.chunk();
    auto n = std::min<size_t>(capacity, c.sample_count() - item.position);
    const size_t stride = d_layout == sample_layout::REAL ? 4 : 8;
    std::memcpy(output, c.samples().data() + item.position * stride, n * stride);
    const auto first = advance(c.first_sample(), item.position);
    for (auto t : item.tags) {
        if (t.offset >= first && t.offset - first < n) {
            t.offset = advance(local_offset, t.offset - first);
            tags.push_back(t);
        }
    }
    if (item.position == 0 && item.gap_before) {
        gr::tag_t gap;
        gap.offset = local_offset;
        gap.key = pmt::intern(GAP_SAMPLES);
        gap.value = pmt::from_uint64(item.gap_before);
        tags.insert(tags.begin(), gap);
        gap.key = pmt::intern(SAMPLE_INDEX);
        gap.value = pmt::from_uint64(c.first_sample());
        tags.insert(tags.begin() + 1, gap);
    }
    item.position += n;
    if (item.position == c.sample_count()) {
        d_incoming.pop_front();
        retire_locked();
        while (!d_incoming.empty() && d_incoming.front().frame.has_gap()) {
            d_incoming.pop_front();
            retire_locked();
        }
        receiver_completion_locked();
        drive_locked();
    }
    return static_cast<int>(n);
}
void session::finish_sending()
{
    std::lock_guard<std::mutex> guard(d_mutex);
    if (d_done || d_failed || d_finish_requested)
        return;
    d_finish_requested = true;
    d_finish_time = std::chrono::steady_clock::now();
    try {
        drive_locked();
    } catch (const protocol_error& e) {
        fail_locked(e.code(), e.what());
    }
    d_cv.notify_all();
}
void session::cancel()
{
    std::lock_guard<std::mutex> guard(d_mutex);
    if (d_done)
        return;
    fail_locked(grpc::StatusCode::CANCELLED, "explicit cancellation", false);
    d_abort();
}
void session::local_failure(grpc::StatusCode code, const std::string& text)
{
    std::lock_guard<std::mutex> guard(d_mutex);
    fail_locked(code, text);
}
bool session::wait_done(std::chrono::milliseconds timeout)
{
    std::unique_lock<std::mutex> lock(d_mutex);
    return d_cv.wait_for(lock, timeout, [&] { return d_done; });
}
session_status session::status() const
{
    std::lock_guard<std::mutex> guard(d_mutex);
    return d_status;
}
bool session::sending() const
{
    std::lock_guard<std::mutex> guard(d_mutex);
    return d_sending;
}
bool session::done() const
{
    std::lock_guard<std::mutex> guard(d_mutex);
    return d_done;
}
} // namespace iqstream
} // namespace gr
