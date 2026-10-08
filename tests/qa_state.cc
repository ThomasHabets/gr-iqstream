/* SPDX-License-Identifier: MIT */
#include "session.h"
#include "test.h"
#include <algorithm>
#include <deque>
#include <thread>

using namespace gr::iqstream;
struct peer {
    std::shared_ptr<session> state;
    std::deque<wire::ClientMessage> sent;
    bool aborted = false;
    bool half_closed = false;
    peer(bool upload, stream_options options = {})
    {
        state =
            std::make_shared<session>(true, upload, sample_layout::REAL, options, "iq");
        state->bind(
            [this](const std::string& data) {
                wire::ClientMessage m;
                CHECK(m.ParseFromString(data));
                sent.push_back(m);
            },
            [this] { half_closed = true; },
            [](grpc::Status) {},
            [this] { aborted = true; });
        state->begin_client();
    }
    void start(bool upload, stream_options options = {})
    {
        wire::ServerMessage reply;
        auto* s = reply.mutable_started();
        *s->mutable_description() = description(sample_layout::REAL, options);
        s->mutable_limits()->set_max_frame_bytes(options.max_frame_bytes);
        s->mutable_limits()->set_max_in_flight_frames(options.max_in_flight_frames);
        s->set_loss_policy(options.loss == loss_policy::LOSSLESS
                               ? wire::LOSS_POLICY_LOSSLESS
                               : wire::LOSS_POLICY_ALLOW_GAPS);
        s->set_completion_mode(wire::COMPLETION_MODE_ACCEPTED);
        if (upload)
            s->mutable_initial_credit()->set_send_limit(options.max_in_flight_frames);
        state->received(reply.SerializeAsString());
    }
    void write_done() { state->write_done(true); }
};
wire::ServerMessage chunk(uint64_t sequence, uint64_t first, uint32_t n)
{
    wire::ServerMessage m;
    m.mutable_frame()->set_sequence(sequence);
    auto* c = m.mutable_frame()->mutable_chunk();
    c->set_first_sample(first);
    c->set_sample_count(n);
    c->set_samples(std::string(n * 4, '\0'));
    return m;
}
void racing_and_completion()
{
    stream_options o;
    o.max_in_flight_frames = 1;
    peer p(true, o);
    // Started may race completion of the transport's Open write.
    p.start(true, o);
    p.write_done();
    float sample = 1;
    CHECK(p.state->push(&sample, 1, {}, 0) == 1);
    CHECK(p.sent.back().frame().sequence() == 0);
    wire::ServerMessage credit;
    credit.mutable_flow_control()->set_send_limit(2);
    // Submitted frame accounting precedes its write completion.
    p.state->received(credit.SerializeAsString());
    CHECK(p.state->status().state == session_state::STREAMING);
    p.state->finish_sending();
    p.write_done();
    CHECK(p.sent.back().has_end());
    wire::ServerMessage complete;
    complete.mutable_complete()->set_next_sequence(1);
    complete.mutable_complete()->set_next_sample(1);
    complete.mutable_complete()->set_completion_mode(wire::COMPLETION_MODE_ACCEPTED);
    p.state->received(complete.SerializeAsString());
    p.write_done();
    CHECK(p.half_closed);
    p.state->transport_done(grpc::Status::OK);
    CHECK(p.state->status().state == session_state::COMPLETE);
    peer full(true, o);
    full.start(true, o);
    full.write_done();
    CHECK(full.state->push(&sample, 1, {}, 0) == 1);
    full.state->finish_sending();
    full.write_done();
    CHECK(full.sent.back().has_end()); // End consumes no frame credit.
    full.state->transport_done(
        grpc::Status(grpc::StatusCode::UNAVAILABLE, "lost acknowledgement"));
    CHECK(full.state->status().completion_uncertain);
}
void receiver_and_gaps()
{
    stream_options o;
    o.max_in_flight_frames = 1;
    o.loss = loss_policy::ALLOW_GAPS;
    peer p(false, o);
    p.start(false, o);
    p.write_done();
    CHECK(p.sent.back().flow_control().send_limit() == 1);
    wire::ServerMessage gap;
    gap.mutable_frame()->set_sequence(0);
    gap.mutable_frame()->mutable_gap()->set_sample_count(10);
    p.state->received(gap.SerializeAsString());
    p.write_done();
    CHECK(p.sent.back().flow_control().send_limit() == 2);
    p.write_done();
    p.state->received(chunk(1, 10, 2).SerializeAsString());
    wire::ServerMessage end;
    end.mutable_end()->set_next_sequence(2);
    end.mutable_end()->set_next_sample(12);
    p.state->received(end.SerializeAsString());
    float output[2];
    std::vector<gr::tag_t> tags;
    CHECK(p.state->pull(output, 1, 20, tags) == 1);
    CHECK(tags.size() == 2);
    CHECK(tags[0].offset == 20 && pmt::to_uint64(tags[0].value) == 10);
    CHECK(pmt::symbol_to_string(tags[0].key) == GAP_SAMPLES);
    CHECK(pmt::symbol_to_string(tags[1].key) == SAMPLE_INDEX);
    tags.clear();
    CHECK(p.state->pull(output, 1, 21, tags) == 1);
    CHECK(tags.empty());
    CHECK(p.sent.back().has_complete());
    CHECK(p.state->status().lost_samples == 10 && p.state->status().trailing_gap == 0);
    wire::ServerMessage complete;
    *complete.mutable_complete() = p.sent.back().complete();
    p.state->received(complete.SerializeAsString());
    p.write_done();
    p.state->transport_done(grpc::Status::OK);
    CHECK(p.state->status().state == session_state::COMPLETE);
    peer trailing(false, o);
    trailing.start(false, o);
    trailing.write_done();
    gap.mutable_frame()->mutable_gap()->set_sample_count(UINT64_MAX);
    trailing.state->received(gap.SerializeAsString());
    trailing.write_done();
    trailing.write_done();
    end.mutable_end()->set_next_sequence(1);
    end.mutable_end()->set_next_sample(UINT64_MAX);
    trailing.state->received(end.SerializeAsString());
    CHECK(trailing.sent.back().has_complete());
    CHECK(trailing.state->status().trailing_gap == UINT64_MAX);
}
void rustradio_negotiation()
{
    stream_options remote;
    remote.profile = metadata_profile::RUSTRADIO;
    remote.loss = loss_policy::ALLOW_GAPS;
    peer defaults(false);
    defaults.start(false, remote);
    CHECK(defaults.state->status().state == session_state::FAILED);

    stream_options wrong_profile;
    wrong_profile.loss = loss_policy::ALLOW_GAPS;
    peer native(false, wrong_profile);
    native.start(false, remote);
    CHECK(native.state->status().state == session_state::STREAMING);

    peer compatible(false, remote);
    const auto& offered = compatible.sent.front().open();
    CHECK(offered.loss_policy() == wire::LOSS_POLICY_ALLOW_GAPS);
    CHECK(std::find(offered.download().accepted_tag_kinds().begin(),
                    offered.download().accepted_tag_kinds().end(),
                    wire::TAG_KIND_STRING) !=
          offered.download().accepted_tag_kinds().end());
    compatible.start(false, remote);
    compatible.write_done();
    CHECK(compatible.state->status().state == session_state::STREAMING);
    compatible.state->received(chunk(0, 0, 1).SerializeAsString());
    float output;
    std::vector<gr::tag_t> tags;
    CHECK(compatible.state->pull(&output, 1, 0, tags) == 1);
}
void drop_unsupported_metadata()
{
    peer p(false);
    const auto& offered = p.sent.front().open().download();
    CHECK(offered.accepted_tag_kinds_size() == wire::TagKind_MAX);
    CHECK(offered.accept_tag_source_ids());
    wire::ServerMessage started;
    auto* s = started.mutable_started();
    *s->mutable_description() = description(sample_layout::REAL, {});
    s->mutable_description()->clear_tag_kinds();
    for (auto kind : download_capabilities())
        s->mutable_description()->add_tag_kinds(kind);
    auto* property = s->mutable_description()->add_properties();
    property->set_key("unsupported");
    property->mutable_value()->set_json_value("{}");
    s->mutable_limits()->set_max_frame_bytes(4096);
    s->mutable_limits()->set_max_in_flight_frames(8);
    s->set_loss_policy(wire::LOSS_POLICY_LOSSLESS);
    s->set_completion_mode(wire::COMPLETION_MODE_ACCEPTED);
    p.state->received(started.SerializeAsString());
    p.write_done();
    auto frame = chunk(0, 0, 3);
    auto* tags = frame.mutable_frame()->mutable_chunk()->mutable_tags();
    auto* t = tags->Add();
    t->set_key("json");
    t->mutable_value()->set_json_value("{}");
    t = tags->Add();
    t->set_key("nested");
    t->mutable_value()->mutable_list_value()->add_values()->set_json_value("null");
    t = tags->Add();
    t->set_key("opaque");
    t->mutable_value()->mutable_opaque_value()->set_type_url("example/v1");
    t = tags->Add();
    t->set_key("string");
    t->mutable_value()->set_string_value("received");
    t = tags->Add();
    t->set_sample_index(2);
    t->set_key("bool");
    t->mutable_value()->set_bool_value(false);
    p.state->received(frame.SerializeAsString());
    float output[3];
    std::vector<gr::tag_t> received;
    CHECK(p.state->pull(output, 3, 10, received) == 3);
    CHECK(received.size() == 2);
    CHECK(received[0].offset == 10);
    CHECK(pmt::symbol_to_string(received[0].value) == "received");
    CHECK(received[1].offset == 12 && received[1].value == pmt::PMT_F);
    CHECK(p.state->status().state == session_state::STREAMING);
    // Unsupported metadata does not bypass structural or resource validation.
    auto bad = chunk(1, 3, 1);
    t = bad.mutable_frame()->mutable_chunk()->add_tags();
    t->set_sample_index(4);
    t->set_key("bad");
    t->mutable_value()->set_json_value("{}");
    p.state->received(bad.SerializeAsString());
    CHECK(p.state->status().state == session_state::FAILED);
}
void malformed_peers()
{
    stream_options o;
    o.max_in_flight_frames = 1;
    peer premature(false, o);
    premature.state->received(chunk(0, 0, 1).SerializeAsString());
    CHECK(premature.state->status().state == session_state::FAILED);
    peer sender(true, o);
    sender.start(true, o);
    sender.write_done();
    wire::ServerMessage credit;
    credit.mutable_flow_control()->set_send_limit(2);
    sender.state->received(credit.SerializeAsString());
    CHECK(sender.state->status().state == session_state::FAILED);
    peer receiver(false, o);
    receiver.start(false, o);
    receiver.write_done();
    receiver.write_done();
    receiver.state->received(chunk(0, 0, 1).SerializeAsString());
    receiver.state->received(chunk(1, 1, 1).SerializeAsString());
    CHECK(receiver.state->status().state == session_state::FAILED); // Exceeded grant.
    peer repeated(false, o);
    repeated.start(false, o);
    repeated.write_done();
    repeated.write_done();
    repeated.start(false, o);
    CHECK(repeated.state->status().state == session_state::FAILED);
    peer failure(false, o);
    wire::ServerMessage f;
    f.mutable_failure()->set_grpc_status_code(grpc::StatusCode::NOT_FOUND);
    failure.state->received(f.SerializeAsString());
    failure.write_done();
    CHECK(!failure.aborted);
    failure.state->transport_done(grpc::Status::OK);
    CHECK(failure.state->status().grpc_status_code == grpc::StatusCode::INVALID_ARGUMENT);
    peer oversized(false, o);
    oversized.start(false, o);
    oversized.write_done();
    oversized.write_done();
    wire::ServerMessage m = chunk(0, 0, 1);
    m.mutable_frame()->mutable_chunk()->set_samples(
        std::string(o.max_frame_bytes + 1, 'a'));
    oversized.state->received(m.SerializeAsString());
    CHECK(oversized.state->status().grpc_status_code ==
          grpc::StatusCode::RESOURCE_EXHAUSTED);
    peer ending(false, o);
    ending.start(false, o);
    ending.write_done();
    ending.write_done();
    wire::ServerMessage end;
    end.mutable_end()->set_next_sample(1);
    ending.state->received(end.SerializeAsString());
    CHECK(ending.state->status().state == session_state::FAILED);
}
int main()
{
    try {
        racing_and_completion();
        receiver_and_gaps();
        rustradio_negotiation();
        drop_unsupported_metadata();
        malformed_peers();
        std::cout << "state machine checks passed\n";
    } catch (const std::exception& e) {
        std::cerr << e.what() << '\n';
        return 1;
    }
}
