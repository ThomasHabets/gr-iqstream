/* SPDX-License-Identifier: GPL-3.0-or-later */
#include "session.h"
#include "test.h"
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
        malformed_peers();
        std::cout << "state machine checks passed\n";
    } catch (const std::exception& e) {
        std::cerr << e.what() << '\n';
        return 1;
    }
}
