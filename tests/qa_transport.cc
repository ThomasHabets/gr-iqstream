/* SPDX-License-Identifier: MIT */
#include "client_connection.h"
#include "server_impl.h"
#include "test.h"
#include <cstring>
#include <future>

using namespace gr::iqstream;
std::shared_ptr<session> accepted(const std::shared_ptr<resource>& resource)
{
    std::unique_lock<std::mutex> lock(resource->mutex);
    CHECK(resource->cv.wait_for(lock, std::chrono::seconds(5), [&] {
        return static_cast<bool>(resource->connection);
    }));
    return resource->connection;
}
void roundtrip(bool upload, sample_layout layout, size_t samples)
{
    stream_options options;
    options.sample_rate_hz = 48000;
    options.max_frame_bytes = 512;
    options.max_in_flight_frames = 1;
    auto listener = server::make("127.0.0.1:0");
    auto resource =
        server_impl::get(listener)->add_resource("iq", !upload, layout, options);
    auto client = std::make_shared<session>(true, upload, layout, options, "iq");
    client_connection transport(listener->address(), client, options);
    auto remote = accepted(resource);
    auto sender = upload ? client : remote;
    auto receiver = upload ? remote : client;
    const size_t stride = layout == sample_layout::REAL ? 4 : 8;
    std::string payload(samples * stride, '\0');
    for (size_t i = 0; i < payload.size(); ++i)
        payload[i] = static_cast<char>(i * 37);
    gr::tag_t tag;
    tag.offset = 3;
    tag.key = pmt::intern("u64");
    tag.value = pmt::from_uint64(UINT64_MAX);
    tag.srcid = pmt::intern("");
    auto producer = std::async(std::launch::async, [&] {
        size_t offset = 0;
        while (offset < samples) {
            auto n = sender->push(
                payload.data() + offset * stride, samples - offset, { tag, tag }, offset);
            CHECK(n);
            offset += n;
        }
        sender->finish_sending();
    });
    std::string result;
    size_t tag_count = 0;
    for (;;) {
        char out[24];
        std::vector<gr::tag_t> tags;
        int n = receiver->pull(out, 3, result.size() / stride, tags);
        if (n < 0)
            break;
        result.append(out, n * stride);
        for (const auto& t : tags) {
            CHECK(t.offset == 3);
            CHECK(pmt::to_uint64(t.value) == UINT64_MAX);
            CHECK(pmt::is_symbol(t.srcid) && pmt::symbol_to_string(t.srcid).empty());
            ++tag_count;
        }
    }
    producer.get();
    CHECK(sender->wait_done(std::chrono::seconds(5)));
    CHECK(receiver->wait_done(std::chrono::seconds(5)));
    CHECK(sender->status().state == session_state::COMPLETE);
    CHECK(receiver->status().state == session_state::COMPLETE);
    CHECK(result == payload);
    CHECK(tag_count == (samples > 3 ? 2 : 0));
    listener->shutdown();
}
void gaps(bool upload, bool retain_after_gap)
{
    stream_options options;
    options.loss = loss_policy::ALLOW_GAPS;
    options.blocking = false;
    options.max_frame_bytes = 512;
    options.max_in_flight_frames = 1;
    auto listener = server::make("127.0.0.1:0");
    auto entry = server_impl::get(listener)->add_resource(
        "iq", !upload, sample_layout::REAL, options);
    auto client =
        std::make_shared<session>(true, upload, sample_layout::REAL, options, "iq");
    client_connection transport(listener->address(), client, options);
    auto remote = accepted(entry);
    auto sender = upload ? client : remote;
    auto receiver = upload ? remote : client;
    std::vector<float> samples(1000, 0.25f);
    size_t consumed = 0;
    while (consumed < samples.size()) {
        auto n = sender->push(
            samples.data() + consumed, samples.size() - consumed, {}, consumed);
        CHECK(n);
        consumed += n;
    }
    auto lost = sender->status().lost_samples;
    CHECK(lost > 0 && lost < samples.size());
    auto ending = std::async(std::launch::async, [&] {
        if (retain_after_gap) {
            const auto deadline =
                std::chrono::steady_clock::now() + std::chrono::seconds(5);
            while (sender->status().next_sample != samples.size()) {
                CHECK(std::chrono::steady_clock::now() < deadline);
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
            float last = 0.5f;
            CHECK(sender->push(&last, 1, {}, samples.size()) == 1);
        }
        sender->finish_sending();
    });
    size_t retained = 0, markers = 0;
    for (;;) {
        float data[3];
        std::vector<gr::tag_t> tags;
        auto n = receiver->pull(data, 3, retained, tags);
        if (n < 0)
            break;
        for (const auto& t : tags) {
            CHECK(retain_after_gap);
            CHECK(t.offset == samples.size() - lost);
            if (pmt::symbol_to_string(t.key) == GAP_SAMPLES)
                CHECK(pmt::to_uint64(t.value) == lost);
            else {
                CHECK(pmt::symbol_to_string(t.key) == SAMPLE_INDEX);
                CHECK(pmt::to_uint64(t.value) == samples.size());
            }
            ++markers;
        }
        retained += n;
    }
    ending.get();
    CHECK(sender->wait_done(std::chrono::seconds(5)));
    CHECK(receiver->status().state == session_state::COMPLETE);
    CHECK(sender->status().state == session_state::COMPLETE);
    CHECK(retained == samples.size() - lost + (retain_after_gap ? 1 : 0));
    CHECK(receiver->status().lost_samples == lost);
    CHECK(receiver->status().trailing_gap == (retain_after_gap ? 0 : lost));
    CHECK(markers == (retain_after_gap ? 2 : 0));
    CHECK(receiver->status().next_sample == samples.size() + (retain_after_gap ? 1 : 0));
    listener->shutdown();
}
void completion_timeout()
{
    stream_options options;
    options.max_in_flight_frames = 1;
    options.shutdown_timeout_ms = 100;
    auto receiver_options = options;
    // Only the sender should expire in this test. With equal deadlines, the
    // receiver watchdog can abort first and the sender legitimately sees CANCELLED.
    receiver_options.shutdown_timeout_ms = 5000;
    auto listener = server::make("127.0.0.1:0");
    auto entry = server_impl::get(listener)->add_resource(
        "iq", false, sample_layout::REAL, receiver_options);
    auto sender =
        std::make_shared<session>(true, true, sample_layout::REAL, options, "iq");
    client_connection transport(listener->address(), sender, options);
    auto receiver = accepted(entry);
    float sample = 1;
    CHECK(sender->push(&sample, 1, {}, 0) == 1);
    sender->finish_sending();
    // Receiver never accepts the frame into graph buffers, so it cannot Complete.
    CHECK(sender->wait_done(std::chrono::seconds(3)));
    CHECK(sender->status().state == session_state::FAILED);
    CHECK(sender->status().grpc_status_code == grpc::StatusCode::DEADLINE_EXCEEDED);
    CHECK(sender->status().completion_uncertain);
    listener->shutdown();
}
void failures()
{
    stream_options options;
    options.handshake_timeout_ms = 100;
    options.shutdown_timeout_ms = 100;
    auto listener = server::make("127.0.0.1:0");
    auto client =
        std::make_shared<session>(true, false, sample_layout::REAL, options, "missing");
    client_connection transport(listener->address(), client, options);
    CHECK(client->wait_done(std::chrono::seconds(3)));
    CHECK(client->status().state == session_state::FAILED);
    CHECK(client->status().grpc_status_code == grpc::StatusCode::NOT_FOUND);
    listener->shutdown();
    auto stalled =
        std::make_shared<session>(true, false, sample_layout::REAL, options, "iq");
    client_connection unavailable("127.0.0.1:1", stalled, options);
    CHECK(stalled->wait_done(std::chrono::seconds(3)));
    CHECK(stalled->status().state != session_state::COMPLETE);
}
int main()
{
    try {
        for (bool upload : { false, true })
            for (auto layout : { sample_layout::REAL, sample_layout::COMPLEX }) {
                roundtrip(upload, layout, 0);
                roundtrip(upload, layout, 1000);
            }
        for (bool upload : { false, true })
            for (bool retained : { false, true })
                gaps(upload, retained);
        completion_timeout();
        failures();
        std::cout << "gRPC transport acceptance passed\n";
    } catch (const std::exception& e) {
        std::cerr << e.what() << '\n';
        return 1;
    }
}
